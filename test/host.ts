// A stand-in for the slice of OpenCode's plugin host this plugin uses: integrations, connections, events and
// web search. It replays transforms on every read and keeps state per integration ID, as OpenCode does.
import type { Plugin } from "@opencode/plugin/effect"
import { Effect, PubSub, Stream } from "effect"
import { type Connection, credentialStore } from "./credentials.js"
import { type Control, type Faults, makeFaults } from "./faults.js"

type Context = Parameters<Plugin.Plugin["effect"]>[0]
type IntegrationEditor = Parameters<Parameters<Context["integration"]["transform"]>[0]>[0]
type WebSearchEditor = Parameters<Parameters<Context["websearch"]["transform"]>[0]>[0]
type WebSearchDefinition = Parameters<WebSearchEditor["add"]>[0]
type Method = Parameters<IntegrationEditor["method"]["update"]>[0]["method"]
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

type Transforms = {
  integration: Array<(editor: IntegrationEditor) => void>
  websearch: Array<(editor: WebSearchEditor) => void>
}

/** Replays the integration transforms into a fresh state, as OpenCode does on every read. */
const integrationsFrom = (transforms: Transforms["integration"]) => {
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
  transforms.forEach((transform) => transform(editor))
  return state
}

/** Replays the web search transforms into the list of providers they add. */
const providersFrom = (transforms: Transforms["websearch"]) => {
  const providers: WebSearchDefinition[] = []
  const editor = { add: (definition: WebSearchDefinition) => providers.push(definition) } as unknown as WebSearchEditor
  transforms.forEach((transform) => transform(editor))
  return providers
}

/** Adds a transform to `list`, like OpenCode's transform registration. */
const register =
  <T>(list: T[]) =>
  (callback: T) =>
    Effect.sync(() => {
      list.push(callback)
      return { dispose: Effect.void }
    })

/** The plugin context the fake host hands to the plugin. */
const makeCtx = (parts: {
  events: PubSub.PubSub<HostEvent | Control>
  faults: ReturnType<typeof makeFaults<HostEvent>>
  transforms: Transforms
  credentials: ReturnType<typeof credentialStore>
  counts: { subscriptions: number; reloads: number; activeCalls: number }
  /** Takes the host's onActive hook, so it runs once. */
  takeHook: () => (() => Effect.Effect<void>) | undefined
}) => {
  const { events, faults, transforms, credentials, counts, takeHook } = parts
  return {
    app: { name: "cli", version: "2.2.6", channel: "latest" },
    integration: {
      transform: register(transforms.integration),
      connection: {
        active: (integrationID: string) =>
          Effect.suspend(() => {
            counts.activeCalls++
            return faults.active(
              Effect.gen(function* () {
                const result = credentials.active(integrationID)
                const hook = takeHook()
                if (hook) yield* hook()
                return result
              }),
            )
          }),
        resolve: (connection: Connection) => Effect.sync(() => credentials.resolve(connection)),
      },
    },
    event: {
      // Like OpenCode's bus, each subscription starts when its stream starts and sees only later events.
      subscribe: (input?: { types?: ReadonlyArray<string> }) =>
        Stream.suspend(() => {
          counts.subscriptions++
          return faults.subscription(Stream.fromPubSub(events))
        }).pipe(Stream.filter((event) => !input?.types || input.types.includes(event.type))),
    },
    websearch: {
      transform: register(transforms.websearch),
      reload: () => faults.reload(Effect.sync(() => void counts.reloads++)),
    },
  } as unknown as Context
}

export const makeHost = (initial?: { key?: string }) =>
  Effect.gen(function* () {
    const events = yield* PubSub.unbounded<HostEvent | Control>()
    const faults = makeFaults<HostEvent>(events)
    const env = new Map<string, string>()
    const transforms: Transforms = { integration: [], websearch: [] }
    const integrations = () => integrationsFrom(transforms.integration)
    const credentials = credentialStore(env, integrations)
    if (initial?.key !== undefined) credentials.save("nimble", initial.key)
    const counts = { subscriptions: 0, reloads: 0, activeCalls: 0 }
    const takeHook = () => {
      const hook = host.onActive
      host.onActive = undefined
      return hook
    }

    // The fault controls are the host's own fields, so tests set them on the host.
    const host: FakeHost = Object.defineProperties(
      faults.controls,
      Object.getOwnPropertyDescriptors({
        ctx: makeCtx({ events, faults, transforms, credentials, counts, takeHook }),
        env,
        get key() {
          return credentials.key("nimble")
        },
        set key(key) {
          credentials.save("nimble", key)
        },
        connect: (key: string | undefined, integrationID = "nimble") =>
          Effect.suspend(() => {
            const credentialID = credentials.save(integrationID, key)
            return PubSub.publish(events, { type: "credential.switched", data: { integrationID, credentialID } })
          }).pipe(Effect.asVoid),
        publish: (event: HostEvent) => PubSub.publish(events, event).pipe(Effect.asVoid),
        integrations,
        providers: () => providersFrom(transforms.websearch),
        reloads: () => counts.reloads,
        activeCalls: () => counts.activeCalls,
        subscriptions: () => counts.subscriptions,
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
