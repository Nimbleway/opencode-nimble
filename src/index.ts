import { Plugin } from "@opencode/plugin/effect"
import { Duration, Effect, Exit, Fiber, Semaphore, Stream } from "effect"
import { FetchHttpClient } from "effect/http"
import { search } from "./search.js"

export { endpoint, timeout, TimeoutError } from "./search.js"

export const id = "nimble"
export const env = "NIMBLE_API_KEY"

/** Waits before retrying a key change or resubscribing to key changes: 1 second, doubling up to 30 seconds. */
export const retryDelay = (attempt: number) =>
  Duration.millis(Math.min(1000 * 2 ** Math.max(0, attempt - 1), 30_000))

/** Retries of a key change that could not be applied, 1, 2 and 4 seconds apart. Later events try again too. */
export const retries = 3

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
      const connected = { current: false }
      // The answer this plugin last applied: the startup answer, then each successful reload's. It is unknown after a
      // failed reload, since OpenCode may have rebuilt web search before the reload failed, so the next try reloads
      // whatever the lookup finds.
      const applied: { current: boolean | undefined } = { current: false }
      // One lookup and reload at a time, so an older answer never lands after a newer one.
      const lock = yield* Semaphore.make(1)
      const lookup = ctx.integration.connection.active(id).pipe(
        Effect.map((connection) => void (connected.current = connection !== undefined)),
      )
      // Reloads web search only when its answer is out of date, so unrelated events don't reload it.
      const apply = Effect.gen(function* () {
        yield* lookup
        const answer = connected.current
        if (answer === applied.current) return
        applied.current = undefined
        yield* ctx.websearch.reload()
        applied.current = answer
      }).pipe(lock.withPermits(1))
      // Handles one key change. A failed lookup or reload is logged and retried with a backoff, then dropped, so later
      // changes are still handled. Unloading the plugin interrupts this fiber itself, which Effect.exit doesn't
      // capture, so only failures and interruptions coming from the host calls are logged.
      const update = Effect.gen(function* () {
        for (let attempt = 0; attempt <= retries; attempt++) {
          if (attempt > 0) yield* Effect.sleep(retryDelay(attempt))
          const exit = yield* Effect.exit(apply)
          if (Exit.isSuccess(exit)) return
          yield* Effect.logWarning("Nimble could not apply an API key change", exit.cause)
        }
      })
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
      // If OpenCode fails or interrupts the subscription, resubscribe with a capped backoff. The loop ends when
      // OpenCode ends the stream normally; unloading the plugin interrupts it at once, even mid-sleep or mid-lookup.
      const watch = Effect.gen(function* () {
        for (let attempt = 0; ; attempt++) {
          if (attempt > 0) yield* Effect.sleep(retryDelay(attempt))
          const exit = yield* Effect.exit(subscribe(attempt > 0))
          if (Exit.isSuccess(exit)) return
          yield* Effect.logWarning("Nimble lost its subscription to API key changes and will resubscribe", exit.cause)
        }
      })
      yield* watch.pipe(Effect.forkScoped({ startImmediately: true }))

      // Web search runs this transform once the plugin has loaded and on every reload, with the latest answer.
      yield* Effect.gen(function* () {
        yield* lookup
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
        applied.current = connected.current
      }).pipe(lock.withPermits(1))
    }),
})
