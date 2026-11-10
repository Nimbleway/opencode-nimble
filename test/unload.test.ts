import { Effect, Exit, Scope } from "effect"
import { TestClock } from "effect/testing"
import { describe, expect, it } from "vitest"
import plugin from "../src/index.js"
import { captureLogs, KEY, loaded, run, settle } from "./helpers.js"
import { type FakeHost, makeHost } from "./host.js"

describe("key watcher failures", () => {
  describe("unloading stops the watcher at once, without a log", () => {
    const unload = (
      setup: (host: FakeHost) => Effect.Effect<void>,
      options: { hung?: boolean; key?: string; prepare?: (host: FakeHost) => void } = {},
    ) => {
      const { logs, layer } = captureLogs()
      return Effect.runPromise(
        Effect.gen(function* () {
          const host = yield* makeHost({ key: options.key })
          options.prepare?.(host)
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
          expect(host.hungInterrupted()).toBe(options.hung ? 1 : 0)
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
        { hung: true },
      ))
    it("while waiting to retry a failed lookup at startup", () =>
      unload(() => Effect.void, { key: KEY, prepare: (host) => void (host.failActive = 1) }))
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
})
