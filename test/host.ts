// A stand-in for the slice of OpenCode's plugin host this plugin uses: integrations, connections, events and
// web search. It replays transforms on every read and keeps state per integration ID, as OpenCode does.
import type { Plugin } from "@opencode/plugin/effect"
import { Effect, PubSub, Stream } from "effect"

type Context = Parameters<Plugin.Plugin["effect"]>[0]
type IntegrationEditor = Parameters<Parameters<Context["integration"]["transform"]>[0]>[0]
type WebSearchEditor = Parameters<Parameters<Context["websearch"]["transform"]>[0]>[0]
type WebSearchDefinition = Parameters<WebSearchEditor["add"]>[0]
type Method = Parameters<IntegrationEditor["method"]["update"]>[0]["method"]
type Connection =
  | { type: "credential"; id: string; label: string; method: "key" }
  | { type: "env"; name: string }
type HostEvent =
  | { type: "credential.switched"; data: { integrationID: string; credentialID: string | null } }
  | { type: "integration.updated"; data: Record<string, never> }

export interface IntegrationState {
  name?: string
  methods: Method[]
}

export interface FakeHost {
  readonly ctx: Context
  /** Keys saved with /connect, by integration ID. */
  readonly credentials: Map<string, string>
  /** The server's environment, which counts for an integration only once it registers an env method naming it. */
  readonly env: Map<string, string>
  /** The key saved with /connect for Nimble. */
  key: string | undefined
  /** Changes the key saved with /connect for an integration and publishes the event OpenCode would. */
  readonly connect: (key: string | undefined, integrationID?: string) => Effect.Effect<void>
  readonly publish: (event: HostEvent) => Effect.Effect<void>
  /** Every integration the transforms touched, by ID. */
  readonly integrations: () => Map<string, IntegrationState>
  readonly providers: () => WebSearchDefinition[]
  readonly reloads: () => number
  /** How many times the plugin asked for an active connection. */
  readonly activeCalls: () => number
  /** Runs once, right after the next active-connection read returns its answer. */
  onActive?: () => Effect.Effect<void>
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
  /** How many event subscriptions the plugin has started. */
  readonly subscriptions: () => number
}

export const makeHost = (initial?: { key?: string }) =>
  Effect.gen(function* () {
    // A "fault" entry ends every subscription that sees it with an error, an "interrupt" entry interrupts it.
    const events = yield* PubSub.unbounded<HostEvent | "fault" | "interrupt">()
    let subscriptions = 0
    const integrationTransforms: Array<(editor: IntegrationEditor) => void> = []
    const websearchTransforms: Array<(editor: WebSearchEditor) => void> = []
    const credentials = new Map<string, string>()
    // Like OpenCode, every saved key gets its own credential ID; replacing a key adds a credential and activates it.
    const saved = new Map<string, string>()
    const activeIDs = new Map<string, string>()
    let nextID = 1
    const save = (integrationID: string, key: string | undefined) => {
      if (key === undefined) {
        const id = activeIDs.get(integrationID)
        if (id) saved.delete(id)
        activeIDs.delete(integrationID)
        credentials.delete(integrationID)
        return null
      }
      const id = `cred_${nextID++}`
      saved.set(id, key)
      activeIDs.set(integrationID, id)
      credentials.set(integrationID, key)
      return id
    }
    if (initial?.key !== undefined) save("nimble", initial.key)
    let reloads = 0
    let activeCalls = 0

    const integrations = () => {
      const state = new Map<string, IntegrationState>()
      const entry = (id: string) => {
        const existing = state.get(id)
        if (existing) return existing
        const created: IntegrationState = { methods: [] }
        state.set(id, created)
        return created
      }
      const editor = {
        list: () => Array.from(state, ([id, item]) => ({ id, name: item.name ?? id })),
        get: (id: string) => {
          const item = state.get(id)
          return item && { id, name: item.name ?? id }
        },
        update: (id: string, update: (integration: { id: string; name: string }) => void) => {
          const item = entry(id)
          const ref = { id, name: item.name ?? id }
          update(ref)
          item.name = ref.name
        },
        remove: (id: string) => state.delete(id),
        method: {
          list: (integrationID: string) => state.get(integrationID)?.methods ?? [],
          update: (input: { integrationID: string; method: Method }) => {
            const item = entry(input.integrationID)
            item.methods = item.methods.filter((method) => method.type !== input.method.type)
            item.methods.push(input.method)
          },
          remove: () => {},
        },
      } as unknown as IntegrationEditor
      integrationTransforms.forEach((transform) => transform(editor))
      return state
    }

    // Like OpenCode: a saved credential comes first, then an env variable the integration's env method names.
    const active = (integrationID: string): Connection | undefined => {
      const credentialID = activeIDs.get(integrationID)
      if (credentialID) return { type: "credential", id: credentialID, label: integrationID, method: "key" }
      const name = integrations()
        .get(integrationID)
        ?.methods.flatMap((method) => (method.type === "env" ? method.names : []))
        .find((name) => host.env.has(name))
      return name === undefined ? undefined : { type: "env", name }
    }

    const resolve = (connection: Connection) => {
      if (connection.type === "env") {
        const key = host.env.get(connection.name)
        return key === undefined ? undefined : { type: "key" as const, key }
      }
      const key = saved.get(connection.id)
      return key === undefined ? undefined : { type: "key" as const, key }
    }

    const host: FakeHost = {
      credentials,
      env: new Map(),
      get key() {
        return credentials.get("nimble")
      },
      set key(key) {
        save("nimble", key)
      },
      connect: (key, integrationID = "nimble") =>
        Effect.suspend(() => {
          const credentialID = save(integrationID, key)
          return PubSub.publish(events, {
            type: "credential.switched",
            data: { integrationID, credentialID },
          })
        }).pipe(Effect.asVoid),
      publish: (event) => PubSub.publish(events, event).pipe(Effect.asVoid),
      integrations,
      providers: () => {
        const providers: WebSearchDefinition[] = []
        const editor: WebSearchEditor = {
          add: (definition) => providers.push(definition),
          default: { get: () => undefined, set: () => {} },
        }
        websearchTransforms.forEach((transform) => transform(editor))
        return providers
      },
      reloads: () => reloads,
      activeCalls: () => activeCalls,
      failActive: 0,
      interruptActive: 0,
      hangActive: false,
      failReload: 0,
      failSubscriptions: () => PubSub.publish(events, "fault").pipe(Effect.asVoid),
      interruptSubscriptions: () => PubSub.publish(events, "interrupt").pipe(Effect.asVoid),
      subscriptions: () => subscriptions,
      ctx: undefined as unknown as Context,
    }

    const registration = { dispose: Effect.void }
    const ctx = {
      app: { name: "cli", version: "2.2.6", channel: "latest" },
      integration: {
        transform: (callback: (editor: IntegrationEditor) => void) =>
          Effect.sync(() => {
            integrationTransforms.push(callback)
            return registration
          }),
        reload: () => Effect.void,
        connection: {
          active: (integrationID: string) =>
            Effect.gen(function* () {
              activeCalls++
              if (host.failActive > 0) {
                host.failActive--
                return yield* Effect.die(new Error("connection lookup failed"))
              }
              if (host.interruptActive > 0) {
                host.interruptActive--
                return yield* Effect.interrupt
              }
              if (host.hangActive) return yield* Effect.never
              const result = active(integrationID)
              const hook = host.onActive
              host.onActive = undefined
              if (hook) yield* hook()
              return result
            }),
          resolve: (connection: Connection) => Effect.sync(() => resolve(connection)),
        },
      },
      event: {
        // Like OpenCode's bus, each subscription starts when its stream starts and sees only later events.
        subscribe: (input?: { types?: ReadonlyArray<string> }) =>
          Stream.suspend(() => {
            subscriptions++
            return Stream.fromPubSub(events).pipe(
              Stream.mapEffect((event) =>
                event === "fault"
                  ? Effect.fail(new Error("event stream lost"))
                  : event === "interrupt"
                    ? Effect.interrupt
                    : Effect.succeed(event),
              ),
            )
          }).pipe(Stream.filter((event) => !input?.types || input.types.includes(event.type))),
      },
      websearch: {
        transform: (callback: (editor: WebSearchEditor) => void) =>
          Effect.sync(() => {
            websearchTransforms.push(callback)
            return registration
          }),
        reload: () =>
          Effect.suspend(() => {
            if (host.failReload > 0) {
              host.failReload--
              return Effect.die(new Error("web search reload failed"))
            }
            reloads++
            return Effect.void
          }),
      },
    }
    ;(host as { ctx: Context }).ctx = ctx as unknown as Context
    return host
  })

/** Waits for the plugin's event fiber to catch up, since events arrive asynchronously. */
export const eventually = (check: () => boolean, seconds = 1) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < seconds * 200; attempt++) {
      if (check()) return
      yield* Effect.sleep("5 millis")
    }
    throw new Error(`condition not met within ${seconds} seconds`)
  })
