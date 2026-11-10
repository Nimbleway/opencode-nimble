// Fault injection for the fake host: failing, interrupted and hung connection lookups, failing web search reloads,
// and event subscriptions the host fails, interrupts or ends.
import { Effect, PubSub, Stream } from "effect"

/**
 * Control entries for every subscription that sees them: "fault" fails it, "interrupt" interrupts it and "end" ends
 * it normally.
 */
export type Control = "fault" | "interrupt" | "end"

export interface Faults {
  /** How many of the next active-connection reads fail, like a lookup that breaks inside OpenCode. */
  failActive: number
  /** How many of the next active-connection reads are interrupted by the host. */
  interruptActive: number
  /** Makes every later active-connection read hang. */
  hangActive: boolean
  /** How many of the next web search reloads fail. */
  failReload: number
  /** Ends every current event subscription with an error. */
  readonly failSubscriptions: () => Effect.Effect<void>
  /** Interrupts every current event subscription from the host's side. */
  readonly interruptSubscriptions: () => Effect.Effect<void>
  /** Ends every current event subscription normally, as OpenCode's bus does when it shuts down. */
  readonly endSubscriptions: () => Effect.Effect<void>
  /** How many hung active-connection reads were interrupted. */
  readonly hungInterrupted: () => number
}

export const makeFaults = <Event>(events: PubSub.PubSub<Event | Control>) => {
  let hungInterrupted = 0
  const control = (entry: Control) => PubSub.publish(events, entry).pipe(Effect.asVoid)
  const controls: Faults = {
    failActive: 0,
    interruptActive: 0,
    hangActive: false,
    failReload: 0,
    failSubscriptions: () => control("fault"),
    interruptSubscriptions: () => control("interrupt"),
    endSubscriptions: () => control("end"),
    hungInterrupted: () => hungInterrupted,
  }
  return {
    controls,
    /** Runs an active-connection read unless a fault is due. */
    active: <A>(read: Effect.Effect<A>) =>
      Effect.gen(function* () {
        if (controls.failActive > 0) {
          controls.failActive--
          return yield* Effect.die(new Error("connection lookup failed"))
        }
        if (controls.interruptActive > 0) {
          controls.interruptActive--
          return yield* Effect.interrupt
        }
        if (controls.hangActive)
          return yield* Effect.never.pipe(Effect.onInterrupt(() => Effect.sync(() => void hungInterrupted++)))
        return yield* read
      }),
    /** Runs a web search reload unless a failure is due. */
    reload: (reload: Effect.Effect<void>) =>
      Effect.suspend(() => {
        if (controls.failReload > 0) {
          controls.failReload--
          return Effect.die(new Error("web search reload failed"))
        }
        return reload
      }),
    /** Turns the control entries in a subscription's stream into a failure, an interruption or its end. */
    subscription: (stream: Stream.Stream<Event | Control>) =>
      stream.pipe(
        Stream.takeWhile((event) => event !== "end"),
        Stream.mapEffect((event) =>
          event === "fault"
            ? Effect.fail(new Error("event stream lost"))
            : event === "interrupt"
              ? Effect.interrupt
              : Effect.succeed(event as Event),
        ),
      ),
  }
}
