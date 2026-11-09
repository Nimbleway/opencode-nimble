import { Plugin } from "@opencode/plugin/effect"
import { Effect } from "effect"
import { FetchHttpClient } from "effect/http"
import { search } from "./search.js"
import { watchKey } from "./watcher.js"

export { endpoint, timeout, TimeoutError } from "./search.js"
export { retries, retryDelay } from "./watcher.js"

export const id = "nimble"
export const env = "NIMBLE_API_KEY"

export default Plugin.define({
  id,
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
      // Web search runs this transform once the plugin has loaded and on every reload, with the latest answer.
      yield* watchKey(ctx, id, (connected) =>
        ctx.websearch.transform((editor) => {
          if (!connected()) return
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
        }),
      )
    }),
})
