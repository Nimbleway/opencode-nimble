import { Cause, Effect, Logger, Scope } from "effect"
import { FetchHttpClient } from "effect/http"
import type { WebSearch } from "@opencode/plugin"
import { expect } from "vitest"
import plugin from "../src/index.js"
import { makeHost } from "./host.js"

export const KEY = "test-key-not-real"

type Captured = { url: string; method: string; headers: Record<string, string>; body: unknown; signal?: AbortSignal }

/** A fetch that records each request and answers with `respond`. */
export const fakeFetch = (respond: (request: Captured) => Promise<Response> | Response) => {
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

export const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } })

/** Loads the plugin against a fake host and returns its Nimble provider, which needs a key to exist. */
export const loaded = (key: string | undefined) =>
  Effect.gen(function* () {
    const host = yield* makeHost({ key })
    yield* plugin.effect(host.ctx)
    return host
  })

export const run = <A, E>(effect: Effect.Effect<A, E, Scope.Scope>) => Effect.runPromise(Effect.scoped(effect))

export const searchWith = (
  fetch: typeof globalThis.fetch,
  query = "what is effect",
): Effect.Effect<readonly WebSearch.Result[], unknown, Scope.Scope> =>
  Effect.gen(function* () {
    const host = yield* loaded(KEY)
    const [provider] = host.providers()
    expect(provider).toBeDefined()
    return yield* provider!.execute({ query }).pipe(Effect.provideService(FetchHttpClient.Fetch, fetch))
  })

/** Lets the plugin's fibers run, in real time, without advancing a TestClock. */
export const settle = Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 20)))

/** Collects the plugin's log lines, with each line's cause rendered as text. */
export const captureLogs = () => {
  const logs: Array<{ level: string; message: string; cause: string }> = []
  const logger = Logger.make((options) => {
    logs.push({
      level: String(options.logLevel),
      message: Array.isArray(options.message) ? options.message.map(String).join(" ") : String(options.message),
      cause: Cause.pretty(options.cause),
    })
  })
  return { logs, layer: Logger.layer([logger]) }
}
