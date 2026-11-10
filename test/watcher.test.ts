import { Duration, Effect } from "effect"
import { TestClock } from "effect/testing"
import { describe, expect, it } from "vitest"
import { retryDelay } from "../src/index.js"
import { captureLogs, KEY, loaded, run, settle } from "./helpers.js"
import { eventually } from "./host.js"

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

  it("loads when the startup lookup fails, logs it and retries 1 second later", () => {
    const { logs, layer } = captureLogs()
    return run(
      Effect.gen(function* () {
        const host = yield* loaded(KEY, (host) => void (host.failActive = 1))
        expect(host.integrations().get("nimble")?.name).toBe("Nimble")
        yield* settle
        expect(logs.map((log) => log.message)).toEqual(["Nimble could not read its API key at startup and will retry"])
        expect(logs[0]!.cause).toContain("connection lookup failed")
        expect(host.providers()).toEqual([])
        yield* TestClock.adjust("999 millis")
        yield* settle
        expect(host.reloads()).toBe(0)
        yield* TestClock.adjust("1 millis")
        yield* settle
        expect(host.providers().map((provider) => provider.id)).toEqual(["nimble"])
        expect(host.reloads()).toBe(1)
        expect(logs).toHaveLength(1)
        expect(JSON.stringify(logs)).not.toContain(KEY)
      }).pipe(Effect.provide(layer), Effect.provide(TestClock.layer())),
    )
  })

  it("applies the key on the next event after every startup retry fails", () => {
    const { logs, layer } = captureLogs()
    return run(
      Effect.gen(function* () {
        const host = yield* loaded(KEY, (host) => void (host.failActive = 4))
        yield* settle
        for (const wait of ["1 second", "2 seconds", "4 seconds", "1 minute"] as const) {
          yield* TestClock.adjust(wait)
          yield* settle
        }
        expect(host.activeCalls()).toBe(4)
        expect(logs.map((log) => log.message)).toEqual([
          "Nimble could not read its API key at startup and will retry",
          ...Array(3).fill("Nimble could not apply an API key change"),
        ])
        expect(host.providers()).toEqual([])
        yield* host.publish({ type: "integration.updated", data: {} })
        yield* settle
        expect(host.providers().map((provider) => provider.id)).toEqual(["nimble"])
        expect(host.reloads()).toBe(1)
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

  it("waits 1 second before a retry or resubscription, doubling up to 30 seconds", () => {
    expect([1, 2, 3, 4, 5, 6, 7, 20].map((attempt) => Duration.toMillis(retryDelay(attempt)))).toEqual(
      [1, 2, 4, 8, 16, 30, 30, 30].map((seconds) => seconds * 1000),
    )
  })
})
