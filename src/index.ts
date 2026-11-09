import { Plugin } from "@opencode/plugin/effect"
import { Cause, Duration, Effect, Exit, Fiber, Semaphore, Stream } from "effect"
import { FetchHttpClient } from "effect/http"
import { search } from "./search.js"

export { endpoint, timeout, TimeoutError } from "./search.js"

export const id = "nimble"
export const env = "NIMBLE_API_KEY"

/** Waits before resubscribing to key changes: 1 second, doubling up to 30 seconds. */
export const resubscribeDelay = (attempt: number) =>
  Duration.millis(Math.min(1000 * 2 ** Math.max(0, attempt - 1), 30_000))

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
      // Handles one key change. A failure is logged and dropped, so later changes are still handled.
      const update = refresh.pipe(
        Effect.flatMap((changed) => (changed ? ctx.websearch.reload() : Effect.void)),
        Effect.catchCauseIf(
          (cause) => !Cause.hasInterruptsOnly(cause),
          (cause) => Effect.logWarning("Nimble could not check its API key after a change", cause),
        ),
      )
      // OpenCode publishes credential.switched whenever the active key changes, including the first add and the
      // last removal. An env key counts once the env method is registered, which integration.updated announces.
      // Subscribing happens as the stream starts, so fork it before reading: a change in between still triggers an
      // update.
      const subscribe = (catchUp: boolean) =>
        Effect.gen(function* () {
          const watcher = yield* ctx.event.subscribe({ types: ["credential.switched", "integration.updated"] }).pipe(
            Stream.filter((event) => event.type !== "credential.switched" || event.data.integrationID === id),
            Stream.runForEach(() => update),
            Effect.forkChild({ startImmediately: true }),
          )
          // After a resubscription, catch up on changes made while there was no subscription.
          if (catchUp) yield* update
          return yield* Fiber.join(watcher)
        })
      // If the subscription fails, resubscribe with a capped backoff. It ends only when OpenCode ends the stream
      // or the plugin unloads.
      const watch = Effect.gen(function* () {
        for (let attempt = 0; ; attempt++) {
          if (attempt > 0) yield* Effect.sleep(resubscribeDelay(attempt))
          const exit = yield* Effect.exit(subscribe(attempt > 0))
          if (Exit.isSuccess(exit) || Cause.hasInterruptsOnly(exit.cause)) return
          yield* Effect.logWarning("Nimble lost its subscription to API key changes and will resubscribe", exit.cause)
        }
      })
      yield* watch.pipe(Effect.forkScoped({ startImmediately: true }))
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
