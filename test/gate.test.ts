import { Effect, Exit, Scope } from "effect"
import { describe, expect, it } from "vitest"
import plugin from "../src/index.js"
import { KEY, loaded, run } from "./helpers.js"
import { eventually, makeHost } from "./host.js"

describe("integration", () => {
  it("adds Nimble to /connect with a key method and NIMBLE_API_KEY", () =>
    run(
      Effect.gen(function* () {
        const host = yield* loaded(undefined)
        const integrations = host.integrations()
        expect([...integrations.keys()]).toEqual(["nimble"])
        expect(integrations.get("nimble")).toEqual({
          name: "Nimble",
          methods: [{ type: "key" }, { type: "env", names: ["NIMBLE_API_KEY"] }],
        })
      }),
    ))
})

describe("key gate", () => {
  it("registers no provider without a key", () =>
    run(
      Effect.gen(function* () {
        const host = yield* loaded(undefined)
        expect(host.providers()).toEqual([])
      }),
    ))

  it("registers the provider when a key exists at startup", () =>
    run(
      Effect.gen(function* () {
        const host = yield* loaded(KEY)
        expect(host.providers().map((provider) => [provider.id, provider.name])).toEqual([["nimble", "Nimble"]])
      }),
    ))

  it("adds the provider when a key is connected and removes it when the key is removed", () =>
    run(
      Effect.gen(function* () {
        const host = yield* loaded(undefined)
        yield* host.connect(KEY)
        yield* eventually(() => host.reloads() === 1)
        expect(host.providers().map((provider) => provider.id)).toEqual(["nimble"])

        yield* host.connect(undefined)
        yield* eventually(() => host.reloads() === 2)
        expect(host.providers()).toEqual([])
      }),
    ))

  it("picks up an env key announced by integration.updated", () =>
    run(
      Effect.gen(function* () {
        const host = yield* loaded(undefined)
        host.env.set("NIMBLE_API_KEY", KEY)
        yield* host.publish({ type: "integration.updated", data: {} })
        yield* eventually(() => host.reloads() === 1)
        expect(host.providers().map((provider) => provider.id)).toEqual(["nimble"])
      }),
    ))

  it("ignores credential events for other integrations", () =>
    run(
      Effect.gen(function* () {
        const host = yield* loaded(undefined)
        const before = host.activeCalls()
        yield* host.connect("exa-key", "exa")
        // Events are handled in order, so once this Nimble event is handled the Exa one has been too.
        yield* host.publish({ type: "credential.switched", data: { integrationID: "nimble", credentialID: null } })
        yield* eventually(() => host.activeCalls() >= before + 1)
        yield* Effect.sleep("50 millis")
        expect(host.activeCalls()).toBe(before + 1)
        expect(host.reloads()).toBe(0)
      }),
    ))

  it("does not reload web search when the key's presence doesn't change", () =>
    run(
      Effect.gen(function* () {
        const host = yield* loaded(KEY)
        const before = host.activeCalls()
        yield* host.connect("another-key")
        yield* host.publish({ type: "integration.updated", data: {} })
        yield* eventually(() => host.activeCalls() >= before + 2)
        yield* Effect.sleep("50 millis")
        expect(host.reloads()).toBe(0)
        expect(host.providers().map((provider) => provider.id)).toEqual(["nimble"])
      }),
    ))

  it("catches a key connected between subscribing and the first read", () =>
    run(
      Effect.gen(function* () {
        const host = yield* makeHost()
        // The startup read reports no key, then the key arrives. The watcher must see the event, and its newer read
        // must not be overwritten by the stale startup one (the lock around lookups and reloads).
        host.onActive = () => host.connect(KEY)
        yield* plugin.effect(host.ctx)
        yield* eventually(() => host.providers().length === 1)
      }),
    ))

  it("stops watching for key changes when the plugin unloads", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const host = yield* makeHost()
        const scope = yield* Scope.make()
        yield* plugin.effect(host.ctx).pipe(Scope.provide(scope))
        yield* Scope.close(scope, Exit.void)
        yield* host.connect(KEY)
        yield* Effect.sleep("100 millis")
        expect(host.reloads()).toBe(0)
      }),
    ))
})
