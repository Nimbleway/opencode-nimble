// A stand-in for the slice of OpenCode's plugin host this plugin uses: integrations, connections, events and
// web search. It replays transforms on every read and keeps state per integration ID, as OpenCode does.
import type { Plugin } from "@opencode/plugin/effect"
import { Effect, PubSub, Stream } from "effect"
import { type Control, type Faults, makeFaults } from "./faults.js"

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

export interface FakeHost extends Faults {
  readonly ctx: Context
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
  /** How many event subscriptions the plugin has started. */
  readonly subscriptions: () => number
}

export const makeHost = (initial?: { key?: string }) =>
  Effect.gen(function* () {
    const events = yield* PubSub.unbounded<HostEvent | Control>()
    const faults = makeFaults(events)
    let subscriptions = 0
    let reloads = 0
    let activeCalls = 0
    const integrationTransforms: Array<(editor: IntegrationEditor) => void> = []
    const websearchTransforms: Array<(editor: WebSearchEditor) => void> = []
    // The active key saved with /connect, by integration ID. Like OpenCode, every saved key gets its own credential
    // ID; replacing a key adds a credential and activates it.
    const credentials = new Map<string, { id: string; key: string }>()
    let nextID = 1
    const save = (integrationID: string, key: string | undefined) => {
      if (key === undefined) {
        credentials.delete(integrationID)
        return null
      }
      const id = `cred_${nextID++}`
      credentials.set(integrationID, { id, key })
      return id
    }
    if (initial?.key !== undefined) save("nimble", initial.key)

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
        update: (id: string, update: (integration: { id: string; name: string }) => void) => {
          const item = entry(id)
          const ref = { id, name: item.name ?? id }
          update(ref)
          item.name = ref.name
        },
        method: {
          update: (input: { integrationID: string; method: Method }) => {
            const item = entry(input.integrationID)
            item.methods = item.methods.filter((method) => method.type !== input.method.type)
            item.methods.push(input.method)
          },
        },
      } as unknown as IntegrationEditor
      integrationTransforms.forEach((transform) => transform(editor))
      return state
    }

    // Like OpenCode: a saved credential comes first, then an env variable the integration's env method names.
    const active = (integrationID: string): Connection | undefined => {
      const credential = credentials.get(integrationID)
      if (credential) return { type: "credential", id: credential.id, label: integrationID, method: "key" }
      const name = integrations()
        .get(integrationID)
        ?.methods.flatMap((method) => (method.type === "env" ? method.names : []))
        .find((name) => host.env.has(name))
      return name === undefined ? undefined : { type: "env", name }
    }

    const resolve = (connection: Connection) => {
      const key =
        connection.type === "env"
          ? host.env.get(connection.name)
          : [...credentials.values()].find((credential) => credential.id === connection.id)?.key
      return key === undefined ? undefined : { type: "key" as const, key }
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
        connection: {
          active: (integrationID: string) =>
            Effect.suspend(() => {
              activeCalls++
              return faults.active(
                Effect.gen(function* () {
                  const result = active(integrationID)
                  const hook = host.onActive
                  host.onActive = undefined
                  if (hook) yield* hook()
                  return result
                }),
              )
            }),
          resolve: (connection: Connection) => Effect.sync(() => resolve(connection)),
        },
      },
      event: {
        // Like OpenCode's bus, each subscription starts when its stream starts and sees only later events.
        subscribe: (input?: { types?: ReadonlyArray<string> }) =>
          Stream.suspend(() => {
            subscriptions++
            return faults.subscription(Stream.fromPubSub(events))
          }).pipe(Stream.filter((event) => !input?.types || input.types.includes(event.type))),
      },
      websearch: {
        transform: (callback: (editor: WebSearchEditor) => void) =>
          Effect.sync(() => {
            websearchTransforms.push(callback)
            return registration
          }),
        reload: () => faults.reload(Effect.sync(() => void reloads++)),
      },
    }

    // The fault controls are the host's own fields, so tests set them on the host.
    const host: FakeHost = Object.defineProperties(
      faults.controls,
      Object.getOwnPropertyDescriptors({
        ctx: ctx as unknown as Context,
        env: new Map<string, string>(),
        get key() {
          return credentials.get("nimble")?.key
        },
        set key(key) {
          save("nimble", key)
        },
        connect: (key: string | undefined, integrationID = "nimble") =>
          Effect.suspend(() => {
            const credentialID = save(integrationID, key)
            return PubSub.publish(events, {
              type: "credential.switched",
              data: { integrationID, credentialID },
            })
          }).pipe(Effect.asVoid),
        publish: (event: HostEvent) => PubSub.publish(events, event).pipe(Effect.asVoid),
        integrations,
        providers: () => {
          const providers: WebSearchDefinition[] = []
          const editor = {
            add: (definition: WebSearchDefinition) => providers.push(definition),
          } as unknown as WebSearchEditor
          websearchTransforms.forEach((transform) => transform(editor))
          return providers
        },
        reloads: () => reloads,
        activeCalls: () => activeCalls,
        subscriptions: () => subscriptions,
      } satisfies Omit<FakeHost, keyof Faults | "onActive">),
    ) as FakeHost
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
