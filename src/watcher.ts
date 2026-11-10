import type { Plugin } from "@opencode/plugin/effect"
import { Duration, Effect, Exit, Fiber, Semaphore, Stream } from "effect"

/** Waits before retrying a key change or resubscribing to key changes: 1 second, doubling up to 30 seconds. */
export const retryDelay = (attempt: number) =>
  Duration.millis(Math.min(1000 * 2 ** Math.max(0, attempt - 1), 30_000))

/** Retries of a key change that could not be applied, 1, 2 and 4 seconds apart. Later events try again too. */
export const retries = 3

/** The key's presence, as last looked up and as last applied to web search, and the steps that change them. */
const keyState = (ctx: Plugin.Context, id: string) =>
  Effect.gen(function* () {
    const connected = { current: false }
    // The answer this plugin last applied: the startup answer, then each successful reload's. It is unknown after a
    // failed startup lookup, and after a failed reload, since OpenCode may have rebuilt web search before the reload
    // failed, so the next try reloads whatever the lookup finds.
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
    // capture, so only failures and interruptions coming from the host calls are logged. `retry` skips the first
    // try, for a lookup that already failed.
    const attempts = (first: number) =>
      Effect.gen(function* () {
        for (let attempt = first; attempt <= retries; attempt++) {
          if (attempt > 0) yield* Effect.sleep(retryDelay(attempt))
          const exit = yield* Effect.exit(apply)
          if (Exit.isSuccess(exit)) return
          yield* Effect.logWarning("Nimble could not apply an API key change", exit.cause)
        }
      })
    return { connected, applied, lock, lookup, update: attempts(0), retry: attempts(1) }
  })

/** Runs `update` on every key change until OpenCode ends the event stream. */
const watch = (ctx: Plugin.Context, id: string, update: Effect.Effect<void>) => {
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
  return Effect.gen(function* () {
    for (let attempt = 0; ; attempt++) {
      if (attempt > 0) yield* Effect.sleep(retryDelay(attempt))
      const exit = yield* Effect.exit(subscribe(attempt > 0))
      if (Exit.isSuccess(exit)) return
      yield* Effect.logWarning("Nimble lost its subscription to API key changes and will resubscribe", exit.cause)
    }
  })
}

/**
 * Follows whether a key is connected for integration `id` and reloads web search when that changes, until the plugin's
 * scope closes or OpenCode ends the event stream. `register` runs once with a live view of the latest answer.
 */
export const watchKey = <E, R>(
  ctx: Plugin.Context,
  id: string,
  register: (connected: () => boolean) => Effect.Effect<unknown, E, R>,
) =>
  Effect.gen(function* () {
    const key = yield* keyState(ctx, id)
    yield* watch(ctx, id, key.update).pipe(Effect.forkScoped({ startImmediately: true }))
    // The startup lookup and registration hold the same lock as later lookups and reloads, so an event that arrives
    // meanwhile is applied after them. A failed lookup doesn't stop the plugin loading: it registers without a key,
    // leaves the applied answer unknown and retries, 1, 2 and 4 seconds apart, unless the plugin unloads first.
    const startup = yield* Effect.gen(function* () {
      const exit = yield* Effect.exit(key.lookup)
      yield* register(() => key.connected.current)
      key.applied.current = Exit.isSuccess(exit) ? key.connected.current : undefined
      return exit
    }).pipe(key.lock.withPermits(1))
    if (Exit.isSuccess(startup)) return
    yield* Effect.logWarning("Nimble could not read its API key at startup and will retry", startup.cause)
    yield* key.retry.pipe(Effect.forkScoped({ startImmediately: true }))
  })
