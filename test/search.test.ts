import { Effect, Fiber } from "effect"
import { FetchHttpClient, HttpClientError } from "effect/http"
import { TestClock } from "effect/testing"
import { describe, expect, it } from "vitest"
import { endpoint, MissingKeyError, TimeoutError } from "../src/index.js"
import { fakeFetch, json, KEY, loaded, run, searchWith } from "./helpers.js"
import { eventually, type FakeHost } from "./host.js"

const nimbleResults = {
  request_id: "req_1",
  results: [
    { title: "Effect", url: "https://effect.website", description: "Effect documentation", content: "" },
    { title: null, url: "https://example.com/untitled", description: "No title here" },
    { title: "Empty", url: "https://example.com/empty", description: null },
    { url: "https://example.com/bare" },
  ],
}

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

  // The provider list can lag behind a removed key, and a search can start just before the key goes.
  describe("fails without sending a request when the key is gone", () => {
    const searchAfter = (remove: (host: FakeHost) => void) =>
      Effect.gen(function* () {
        const { fetch, requests } = fakeFetch(() => json(nimbleResults))
        const host = yield* loaded(KEY)
        const [provider] = host.providers()
        remove(host)
        const error = yield* provider!.execute({ query: "q" }).pipe(
          Effect.provideService(FetchHttpClient.Fetch, fetch),
          Effect.flip,
        )
        expect(error).toBeInstanceOf(MissingKeyError)
        expect((error as Error).message).toBe("No Nimble API key is connected")
        expect(requests).toHaveLength(0)
      })
    it("before the search reads the connection", () => run(searchAfter((host) => void (host.key = undefined))))
    it("between reading the connection and resolving it", () =>
      run(searchAfter((host) => void (host.onActive = () => Effect.sync(() => void (host.key = undefined))))))
  })

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
