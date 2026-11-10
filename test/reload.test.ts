import { Effect } from "effect"
import { TestClock } from "effect/testing"
import { describe, expect, it } from "vitest"
import { captureLogs, KEY, loaded, run, settle } from "./helpers.js"
import type { FakeHost } from "./host.js"

describe("key watcher failures", () => {
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
})
