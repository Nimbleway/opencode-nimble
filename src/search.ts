import { Duration, Effect, Schema } from "effect"
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http"
import type { WebSearch } from "@opencode/plugin"

export const endpoint = "https://sdk.nimbleway.com/v2/search"

/** OpenCode's built-in web search providers give up after 25 seconds, so Nimble does too. */
export const timeout = Duration.seconds(25)

const SearchRequest = Schema.Struct({
  query: Schema.String,
  search_depth: Schema.Literal("standard"),
  max_results: Schema.Number,
})

const SearchResponse = Schema.Struct({
  results: Schema.Array(
    Schema.Struct({
      title: Schema.NullOr(Schema.String).pipe(Schema.optional),
      url: Schema.String,
      description: Schema.NullOr(Schema.String).pipe(Schema.optional),
    }),
  ),
})

export class TimeoutError extends Error {
  constructor() {
    super("Nimble web search request timed out")
    this.name = "NimbleTimeoutError"
  }
}

/**
 * Runs one Nimble search at standard depth. Non-2xx responses fail with effect's HttpClientError, which OpenCode
 * reads to report 401s and to move `random` web search to another provider after a 429.
 */
export const search = (input: { readonly query: string; readonly key: string | undefined; readonly userAgent: string }) =>
  Effect.gen(function* () {
    const http = yield* HttpClient.HttpClient
    const request = yield* HttpClientRequest.post(endpoint).pipe(
      HttpClientRequest.acceptJson,
      HttpClientRequest.setHeaders({
        "User-Agent": input.userAgent,
        "X-Client-Source": "opencode",
        ...(input.key ? { Authorization: `Bearer ${input.key}` } : {}),
      }),
      HttpClientRequest.schemaBodyJson(SearchRequest)({
        query: input.query,
        search_depth: "standard",
        max_results: 8,
      }),
    )
    const response = yield* HttpClient.withScope(HttpClient.filterStatusOk(http))
      .execute(request)
      .pipe(
        Effect.flatMap(HttpClientResponse.schemaBodyJson(SearchResponse)),
        Effect.scoped,
        Effect.timeoutOrElse({
          duration: timeout,
          orElse: () => Effect.fail(new TimeoutError()),
        }),
      )
    // At standard depth Nimble leaves `content` empty and puts the page text in `description`.
    return response.results.map(
      (item): WebSearch.Result => ({
        url: item.url,
        ...(item.title ? { title: item.title } : {}),
        ...(item.description ? { content: item.description } : {}),
        time: {},
      }),
    )
  })
