import { Duration, Effect, Exit, Scope } from "effect"
import { TestClock } from "effect/testing"
import { describe, expect, it } from "vitest"
import plugin, { retryDelay } from "../src/index.js"
import { captureLogs, KEY, loaded, run, settle } from "./helpers.js"
import { eventually, type FakeHost, makeHost } from "./host.js"

describe("key watcher failures", () => {
  it("retries a failed lookup 1 second later and keeps following key changes", () => {
    const { logs, layer } = captureLogs()
    return run(
      Effect.gen(function* () {
        const host = yield* loaded(undefined)
        host.failActive = 1
        yield* host.connect(KEY)
        yield* settle
        expect(logs.map((log) => log.message)).toEqual(["Nimble could not apply an API key change"])
        expect(host.providers()).toEqual([])
        expect(host.reloads()).toBe(0)
        yield* TestClock.adjust("999 millis")
        yield* settle
        expect(host.reloads()).toBe(0)
        yield* TestClock.adjust("1 millis")
        yield* settle
        expect(host.providers().map((provider) => provider.id)).toEqual(["nimble"])
        expect(host.reloads()).toBe(1)

        yield* host.connect(undefined)
        yield* settle
        expect(host.providers()).toEqual([])
        expect(host.reloads()).toBe(2)
        expect(logs).toHaveLength(1)
      }).pipe(Effect.provide(layer), Effect.provide(TestClock.layer())),
    )
  })

  it("logs a failed lookup with its cause and without the key", () => {
    const { logs, layer } = captureLogs()
    return run(
      Effect.gen(function* () {
        const host = yield* loaded(KEY)
        host.failActive = 1
        yield* host.connect(undefined)
        yield* eventually(() => logs.length > 0)
        const [log] = logs
        expect(log!.level).toBe("Warn")
        expect(log!.message).toBe("Nimble could not apply an API key change")
        expect(log!.cause).toContain("connection lookup failed")
        expect(JSON.stringify(logs)).not.toContain(KEY)
      }).pipe(Effect.provide(layer)),
    )
  })

  it("resubscribes after the event stream fails and catches up on missed changes", () => {
    const { logs, layer } = captureLogs()
    return run(
      Effect.gen(function* () {
        const host = yield* loaded(undefined)
        expect(host.subscriptions()).toBe(1)
        yield* host.failSubscriptions()
        yield* settle
        const log = logs.find((item) => item.message.includes("lost its subscription"))
        expect(log!.cause).toContain("event stream lost")
        // No subscription during the backoff, so only the catch-up read after resubscribing can notice this key.
        yield* host.connect(KEY)
        yield* settle
        expect(host.subscriptions()).toBe(1)
        expect(host.providers()).toEqual([])
        yield* TestClock.adjust("1 second")
        yield* settle
        expect(host.subscriptions()).toBe(2)
        expect(host.providers().map((provider) => provider.id)).toEqual(["nimble"])
        // The new subscription handles later changes.
        yield* host.connect(undefined)
        yield* settle
        expect(host.providers()).toEqual([])
      }).pipe(Effect.provide(layer), Effect.provide(TestClock.layer())),
    )
  })

  it("waits 1 second, then 2 seconds, before resubscribing after repeated failures", () =>
    run(
      Effect.gen(function* () {
        const host = yield* loaded(undefined)
        yield* host.failSubscriptions()
        yield* settle
        yield* TestClock.adjust("999 millis")
        expect(host.subscriptions()).toBe(1)
        yield* TestClock.adjust("1 millis")
        expect(host.subscriptions()).toBe(2)
        yield* host.failSubscriptions()
        yield* settle
        yield* TestClock.adjust("1999 millis")
        expect(host.subscriptions()).toBe(2)
        yield* TestClock.adjust("1 millis")
        expect(host.subscriptions()).toBe(3)
      }).pipe(Effect.provide(TestClock.layer())),
    ))

  it("resubscribes after the host interrupts the subscription", () => {
    const { logs, layer } = captureLogs()
    return run(
      Effect.gen(function* () {
        const host = yield* loaded(undefined)
        yield* host.interruptSubscriptions()
        yield* settle
        expect(logs.map((log) => log.message)).toEqual([
          "Nimble lost its subscription to API key changes and will resubscribe",
        ])
        yield* TestClock.adjust("1 second")
        yield* settle
        expect(host.subscriptions()).toBe(2)
        yield* host.connect(KEY)
        yield* settle
        expect(host.providers().map((provider) => provider.id)).toEqual(["nimble"])
      }).pipe(Effect.provide(layer), Effect.provide(TestClock.layer())),
    )
  })

  it("logs a lookup the host interrupts and retries it", () => {
    const { logs, layer } = captureLogs()
    return run(
      Effect.gen(function* () {
        const host = yield* loaded(undefined)
        host.interruptActive = 1
        yield* host.connect(KEY)
        yield* settle
        expect(logs.map((log) => log.message)).toEqual(["Nimble could not apply an API key change"])
        expect(host.providers()).toEqual([])
        yield* TestClock.adjust("1 second")
        yield* settle
        expect(host.providers().map((provider) => provider.id)).toEqual(["nimble"])
        expect(host.reloads()).toBe(1)
      }).pipe(Effect.provide(layer), Effect.provide(TestClock.layer())),
    )
  })

  it("logs a failed reload and retries it 1 second later", () => {
    const { logs, layer } = captureLogs()
    return run(
      Effect.gen(function* () {
        const host = yield* loaded(undefined)
        host.failReload = 1
        yield* host.connect(KEY)
        yield* settle
        expect(logs).toHaveLength(1)
        expect(logs[0]!.message).toBe("Nimble could not apply an API key change")
        expect(logs[0]!.cause).toContain("web search reload failed")
        expect(host.reloads()).toBe(0)
        yield* TestClock.adjust("1 second")
        yield* settle
        expect(host.reloads()).toBe(1)
        expect(logs).toHaveLength(1)
        expect(JSON.stringify(logs)).not.toContain(KEY)
      }).pipe(Effect.provide(layer), Effect.provide(TestClock.layer())),
    )
  })

  // Every reload fails until the retries run out, then the next event finds the same answer as the failed one.
  const exhaustRetries = (initial: string | undefined, change: (host: FakeHost) => Effect.Effect<void>) =>
    Effect.gen(function* () {
      const host = yield* loaded(initial)
      host.failReload = 4
      yield* change(host)
      yield* settle
      for (const wait of ["1 second", "2 seconds", "4 seconds"] as const) {
        yield* TestClock.adjust(wait)
        yield* settle
      }
      expect(host.failReload).toBe(0)
      expect(host.reloads()).toBe(0)
      yield* TestClock.adjust("1 minute")
      yield* settle
      expect(host.activeCalls()).toBe(5)
      return host
    })

  it("retries a failed reload 1, 2 and 4 seconds later, then stops until the next event", () => {
    const { logs, layer } = captureLogs()
    return run(
      Effect.gen(function* () {
        const host = yield* loaded(undefined)
        host.failReload = 4
        yield* host.connect(KEY)
        yield* settle
        expect(host.failReload).toBe(3)
        for (const [wait, left] of [[1000, 3], [2000, 2], [4000, 1]] as const) {
          yield* TestClock.adjust(`${wait - 1} millis`)
          yield* settle
          expect(host.failReload).toBe(left)
          yield* TestClock.adjust("1 millis")
          yield* settle
          expect(host.failReload).toBe(left - 1)
        }
        yield* TestClock.adjust("1 minute")
        yield* settle
        expect(host.activeCalls()).toBe(5)
        expect(host.reloads()).toBe(0)
        expect(logs.map((log) => log.message)).toEqual(Array(4).fill("Nimble could not apply an API key change"))
      }).pipe(Effect.provide(layer), Effect.provide(TestClock.layer())),
    )
  })

  // OpenCode's reload can rebuild web search and then fail while notifying, and another plugin's reload rebuilds it
  // too, so after a failed reload web search may already show the new answer. When the key then flips back before
  // the retry, the retry must still reload.
  it("reloads on the retry when the key is removed after its add failed to reload", () =>
    run(
      Effect.gen(function* () {
        const host = yield* loaded(undefined)
        host.failReload = 1
        yield* host.connect(KEY)
        yield* settle
        yield* host.connect(undefined)
        yield* settle
        expect(host.reloads()).toBe(0)
        yield* TestClock.adjust("1 second")
        yield* settle
        expect(host.reloads()).toBe(1)
        expect(host.providers()).toEqual([])
      }).pipe(Effect.provide(TestClock.layer())),
    ))

  it("reloads on the retry when the key comes back after its removal failed to reload", () =>
    run(
      Effect.gen(function* () {
        const host = yield* loaded(KEY)
        host.failReload = 1
        yield* host.connect(undefined)
        yield* settle
        yield* host.connect("second-key")
        yield* settle
        expect(host.reloads()).toBe(0)
        yield* TestClock.adjust("1 second")
        yield* settle
        expect(host.reloads()).toBe(1)
        expect(host.providers().map((provider) => provider.id)).toEqual(["nimble"])
      }).pipe(Effect.provide(TestClock.layer())),
    ))

  it("reloads on the next event after a failed reload for an added key, though the key is still there", () =>
    run(
      Effect.gen(function* () {
        const host = yield* exhaustRetries(undefined, (host) => host.connect(KEY))
        yield* host.connect("second-key")
        yield* settle
        expect(host.reloads()).toBe(1)
        // Web search is up to date now, so another event with a key doesn't reload it.
        yield* host.publish({ type: "integration.updated", data: {} })
        yield* settle
        expect(host.activeCalls()).toBe(7)
        expect(host.reloads()).toBe(1)
      }).pipe(Effect.provide(TestClock.layer())),
    ))

  it("reloads on the next event after a failed reload for a removed key, though the key is still gone", () =>
    run(
      Effect.gen(function* () {
        const host = yield* exhaustRetries(KEY, (host) => host.connect(undefined))
        yield* host.publish({ type: "integration.updated", data: {} })
        yield* settle
        expect(host.reloads()).toBe(1)
        expect(host.providers()).toEqual([])
      }).pipe(Effect.provide(TestClock.layer())),
    ))

  describe("unloading stops the watcher at once, without a log", () => {
    const unload = (setup: (host: FakeHost) => Effect.Effect<void>, hung = false) => {
      const { logs, layer } = captureLogs()
      return Effect.runPromise(
        Effect.gen(function* () {
          const host = yield* makeHost()
          const scope = yield* Scope.make()
          yield* plugin.effect(host.ctx).pipe(Scope.provide(scope))
          yield* setup(host)
          yield* settle
          const before = { logs: logs.length, subscriptions: host.subscriptions(), activeCalls: host.activeCalls() }
          yield* Scope.close(scope, Exit.void)
          // A watcher that survived the unload would resubscribe once the backoff ends, or handle this change.
          yield* TestClock.adjust("1 minute")
          yield* host.connect(KEY)
          yield* settle
          expect(host.subscriptions()).toBe(before.subscriptions)
          expect(host.activeCalls()).toBe(before.activeCalls)
          expect(host.reloads()).toBe(0)
          expect(host.hungInterrupted()).toBe(hung ? 1 : 0)
          // Nothing logged by the unload itself (a subscription failure set up earlier is logged before it).
          expect(logs.slice(before.logs)).toEqual([])
        }).pipe(Effect.provide(layer), Effect.provide(TestClock.layer())),
      )
    }
    it("while subscribed", () => unload(() => Effect.void))
    it("during the backoff sleep", () => unload((host) => host.failSubscriptions()))
    it("while waiting to retry a failed reload", () =>
      unload((host) =>
        Effect.gen(function* () {
          host.failReload = 1
          yield* host.connect(KEY)
        }),
      ))
    it("with a lookup that hangs", () =>
      unload(
        (host) =>
          Effect.gen(function* () {
            host.hangActive = true
            yield* host.publish({ type: "integration.updated", data: {} })
          }),
        true,
      ))
  })

  it("stops watching when OpenCode ends the event stream normally", () => {
    const { logs, layer } = captureLogs()
    return run(
      Effect.gen(function* () {
        const host = yield* loaded(undefined)
        yield* host.endSubscriptions()
        yield* settle
        yield* TestClock.adjust("1 minute")
        yield* settle
        expect(host.subscriptions()).toBe(1)
        expect(logs).toEqual([])
      }).pipe(Effect.provide(layer), Effect.provide(TestClock.layer())),
    )
  })

  it("waits 1 second before a retry or resubscription, doubling up to 30 seconds", () => {
    expect([1, 2, 3, 4, 5, 6, 7, 20].map((attempt) => Duration.toMillis(retryDelay(attempt)))).toEqual(
      [1, 2, 4, 8, 16, 30, 30, 30].map((seconds) => seconds * 1000),
    )
  })
})
