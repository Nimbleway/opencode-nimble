import { Effect, Exit, Fiber, Scope } from "effect"
import { FetchHttpClient, HttpClientError } from "effect/http"
import { TestClock } from "effect/testing"
import { describe, expect, it } from "vitest"
import plugin, { endpoint, TimeoutError } from "../src/index.js"
import { eventually, makeHost } from "./host.js"

const KEY = "test-key-not-real"

type Captured = { url: string; method: string; headers: Record<string, string>; body: unknown; signal?: AbortSignal }

/** A fetch that records each request and answers with `respond`. */
const fakeFetch = (respond: (request: Captured) => Promise<Response> | Response) => {
  const requests: Captured[] = []
  const fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = new Request(input, init)
    const captured: Captured = {
      url: request.url,
      method: request.method,
      headers: Object.fromEntries(request.headers.entries()),
      body: request.body ? JSON.parse(await request.text()) : undefined,
      signal: init?.signal ?? undefined,
    }
    requests.push(captured)
    return respond(captured)
  }) as typeof globalThis.fetch
  return { fetch, requests }
}

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } })

const nimbleResults = {
  request_id: "req_1",
  results: [
    { title: "Effect", url: "https://effect.website", description: "Effect documentation", content: "" },
    { title: null, url: "https://example.com/untitled", description: "No title here" },
    { title: "Empty", url: "https://example.com/empty", description: null },
    { url: "https://example.com/bare" },
  ],
}

/** Loads the plugin against a fake host and returns its Nimble provider, which needs a key to exist. */
const loaded = (key: string | undefined) =>
  Effect.gen(function* () {
    const host = yield* makeHost({ key })
    yield* plugin.effect(host.ctx)
    return host
  })

const run = <A, E>(effect: Effect.Effect<A, E, Scope.Scope>) => Effect.runPromise(Effect.scoped(effect))

const searchWith = (fetch: typeof globalThis.fetch, query = "what is effect") =>
  Effect.gen(function* () {
    const host = yield* loaded(KEY)
    const [provider] = host.providers()
    expect(provider).toBeDefined()
    return yield* provider!.execute({ query }).pipe(Effect.provideService(FetchHttpClient.Fetch, fetch))
  })

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

describe("request", () => {
  it("posts a standard-depth search with the key and X-Client-Source: opencode", () =>
    run(
      Effect.gen(function* () {
        const { fetch, requests } = fakeFetch(() => json(nimbleResults))
        yield* searchWith(fetch, "what is effect")
        expect(requests).toHaveLength(1)
        const [request] = requests
        expect(request!.url).toBe(endpoint)
        expect(endpoint).toBe("https://sdk.nimbleway.com/v2/search")
        expect(request!.method).toBe("POST")
        expect(request!.body).toEqual({ query: "what is effect", search_depth: "standard", max_results: 8 })
        expect(request!.headers).toMatchObject({
          authorization: `Bearer ${KEY}`,
          "x-client-source": "opencode",
          "user-agent": "opencode/latest/2.2.6/cli",
          accept: "application/json",
          "content-type": "application/json",
        })
      }),
    ))

  it("reads the key at search time, so a replaced key takes effect without a reload", () =>
    run(
      Effect.gen(function* () {
        const { fetch, requests } = fakeFetch(() => json(nimbleResults))
        const host = yield* loaded(KEY)
        host.key = "replacement-key"
        yield* host.providers()[0]!.execute({ query: "q" }).pipe(Effect.provideService(FetchHttpClient.Fetch, fetch))
        expect(requests[0]!.headers.authorization).toBe("Bearer replacement-key")
      }),
    ))

  it("sends a NIMBLE_API_KEY key when nothing is saved with /connect", () =>
    run(
      Effect.gen(function* () {
        const { fetch, requests } = fakeFetch(() => json(nimbleResults))
        const host = yield* loaded(undefined)
        host.env.set("NIMBLE_API_KEY", "env-key")
        yield* host.publish({ type: "integration.updated", data: {} })
        yield* eventually(() => host.providers().length === 1)
        yield* host.providers()[0]!.execute({ query: "q" }).pipe(Effect.provideService(FetchHttpClient.Fetch, fetch))
        expect(requests[0]!.headers.authorization).toBe("Bearer env-key")
      }),
    ))
})

describe("results", () => {
  it("maps Nimble's description to content and keeps titles only when present", () =>
    run(
      Effect.gen(function* () {
        const { fetch } = fakeFetch(() => json(nimbleResults))
        const results = yield* searchWith(fetch)
        expect(results).toEqual([
          { url: "https://effect.website", title: "Effect", content: "Effect documentation", time: {} },
          { url: "https://example.com/untitled", content: "No title here", time: {} },
          { url: "https://example.com/empty", title: "Empty", time: {} },
          { url: "https://example.com/bare", time: {} },
        ])
      }),
    ))

  it("fails on a response without results", () =>
    run(
      Effect.gen(function* () {
        const { fetch } = fakeFetch(() => json({ request_id: "req_2" }))
        const error = yield* searchWith(fetch).pipe(Effect.flip)
        expect(error).toBeDefined()
      }),
    ))
})

describe("errors", () => {
  it("fails a 401 with an HttpClientError OpenCode reports as an authentication failure, without the key", () =>
    run(
      Effect.gen(function* () {
        const { fetch } = fakeFetch(() => json({ message: "Unauthorized" }, 401))
        const error = yield* searchWith(fetch).pipe(Effect.flip)
        expect(HttpClientError.isHttpClientError(error)).toBe(true)
        expect((error as HttpClientError.HttpClientError).response?.status).toBe(401)
        expect(JSON.stringify(error)).not.toContain(KEY)
        expect(String(error)).not.toContain(KEY)
      }),
    ))

  it("fails a 429 with the status OpenCode needs to move random search to another provider", () =>
    run(
      Effect.gen(function* () {
        // A body that would parse as results, so only the status can fail the search.
        const { fetch } = fakeFetch(
          () =>
            new Response(JSON.stringify({ results: [] }), {
              status: 429,
              headers: { "content-type": "application/json", "retry-after": "30" },
            }),
        )
        const error = yield* searchWith(fetch).pipe(Effect.flip)
        expect(HttpClientError.isHttpClientError(error)).toBe(true)
        expect((error as HttpClientError.HttpClientError).response?.status).toBe(429)
        expect((error as HttpClientError.HttpClientError).response?.headers["retry-after"]).toBe("30")
      }),
    ))

  it("times out after 25 seconds and aborts the request", () =>
    run(
      Effect.gen(function* () {
        // Never answers, like a hung connection.
        const { fetch, requests } = fakeFetch(() => new Promise<Response>(() => {}))
        const fiber = yield* searchWith(fetch).pipe(Effect.flip, Effect.forkChild({ startImmediately: true }))
        yield* Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 20)))
        expect(requests).toHaveLength(1)
        yield* TestClock.adjust("24 seconds")
        expect(fiber.pollUnsafe()).toBeUndefined()
        yield* TestClock.adjust("1 second")
        const error = yield* Fiber.join(fiber)
        expect(error).toBeInstanceOf(TimeoutError)
        expect(requests[0]!.signal?.aborted).toBe(true)
      }).pipe(Effect.provide(TestClock.layer())),
    ))
})
