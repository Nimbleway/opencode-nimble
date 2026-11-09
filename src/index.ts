import { Plugin } from "@opencode/plugin/effect"
import { Effect, Semaphore, Stream } from "effect"
import { FetchHttpClient } from "effect/http"
import { search } from "./search.js"

export { endpoint, timeout, TimeoutError } from "./search.js"

export const id = "nimble"
export const env = "NIMBLE_API_KEY"

export default Plugin.define({
  id: "nimble",
  effect: (ctx) =>
    Effect.gen(function* () {
      // A Nimble integration in /connect, with the key from /connect or NIMBLE_API_KEY.
      yield* ctx.integration.transform((editor) => {
        editor.update(id, (integration) => (integration.name = "Nimble"))
        editor.method.update({ integrationID: id, method: { type: "key" } })
        editor.method.update({ integrationID: id, method: { type: "env", names: [env] } })
      })

      // `random` web search only moves on after an HTTP 429, so a provider that cannot search without a key
      // would fail every search in a session that picked it. Register only while a key is connected.
      const connected = { current: false }
      // One refresh at a time, so the startup read can never overwrite a newer answer from the watcher.
      const lock = yield* Semaphore.make(1)
      // Reports whether the answer changed, so unrelated events don't reload web search.
      const refresh = ctx.integration.connection.active(id).pipe(
        Effect.map((connection) => {
          const previous = connected.current
          connected.current = connection !== undefined
          return previous !== connected.current
        }),
        lock.withPermits(1),
      )
      // OpenCode publishes credential.switched whenever the active key changes, including the first add and the
      // last removal. An env key counts once the env method is registered, which integration.updated announces.
      // Subscribe before the first read so a change in between still triggers a reload.
      yield* ctx.event.subscribe({ types: ["credential.switched", "integration.updated"] }).pipe(
        Stream.filter((event) => event.type !== "credential.switched" || event.data.integrationID === id),
        Stream.runForEach(() =>
          refresh.pipe(Effect.flatMap((changed) => (changed ? ctx.websearch.reload() : Effect.void))),
        ),
        Effect.catchCause(() => Effect.logWarning("Nimble stopped watching for API key changes")),
        Effect.forkScoped({ startImmediately: true }),
      )
      yield* refresh

      yield* ctx.websearch.transform((editor) => {
        if (!connected.current) return
        editor.add({
          id,
          name: "Nimble",
          execute: (input) =>
            Effect.gen(function* () {
              const connection = yield* ctx.integration.connection.active(id)
              const credential = connection ? yield* ctx.integration.connection.resolve(connection) : undefined
              return yield* search({
                query: input.query,
                key: credential?.type === "key" ? credential.key : undefined,
                userAgent: `opencode/${ctx.app.channel}/${ctx.app.version}/${ctx.app.name}`,
              })
            }).pipe(Effect.provide(FetchHttpClient.layer)),
        })
      })
    }),
})
