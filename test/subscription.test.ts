import { Effect } from "effect"
import { TestClock } from "effect/testing"
import { describe, expect, it } from "vitest"
import { captureLogs, KEY, loaded, run, settle } from "./helpers.js"

describe("key watcher failures", () => {
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

  it("subscribes before the catch-up read, so a change made during that read is not missed", () =>
    run(
      Effect.gen(function* () {
        const host = yield* loaded(undefined)
        yield* host.failSubscriptions()
        yield* settle
        host.onActive = () => host.connect(KEY)
        yield* TestClock.adjust("1 second")
        yield* settle
        expect(host.subscriptions()).toBe(2)
        expect(host.providers().map((provider) => provider.id)).toEqual(["nimble"])
      }).pipe(Effect.provide(TestClock.layer())),
    ))

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
})
