// A stand-in for the slice of OpenCode's plugin host this plugin uses: integrations, connections, events and
// web search. It replays transforms on every read, as OpenCode does.
import type { Plugin } from "@opencode/plugin/effect"
import { Effect, Queue, Stream } from "effect"

type Context = Parameters<Plugin.Plugin["effect"]>[0]
type IntegrationEditor = Parameters<Parameters<Context["integration"]["transform"]>[0]>[0]
type WebSearchEditor = Parameters<Parameters<Context["websearch"]["transform"]>[0]>[0]
type WebSearchDefinition = Parameters<WebSearchEditor["add"]>[0]
type Method = Parameters<IntegrationEditor["method"]["update"]>[0]["method"]
type HostEvent =
  | { type: "credential.switched"; data: { integrationID: string; credentialID: string | null } }
  | { type: "integration.updated"; data: Record<string, never> }

export interface FakeHost {
  readonly ctx: Context
  /** The key OpenCode would resolve for Nimble's active connection, from /connect or NIMBLE_API_KEY. */
  key: string | undefined
  /** Changes the connected key and publishes the event OpenCode would. */
  readonly connect: (key: string | undefined) => Effect.Effect<void>
  readonly publish: (event: HostEvent) => Effect.Effect<void>
  readonly integration: () => { name?: string; methods: Method[] }
  readonly providers: () => WebSearchDefinition[]
  readonly reloads: () => number
}

export const makeHost = (initial?: { key?: string }) =>
  Effect.gen(function* () {
    const events = yield* Queue.unbounded<HostEvent>()
    const integrationTransforms: Array<(editor: IntegrationEditor) => void> = []
    const websearchTransforms: Array<(editor: WebSearchEditor) => void> = []
    let reloads = 0

    const host: FakeHost = {
      key: initial?.key,
      connect: (key) =>
        Effect.suspend(() => {
          host.key = key
          return Queue.offer(events, {
            type: "credential.switched",
            data: { integrationID: "nimble", credentialID: key ? "cred_1" : null },
          })
        }).pipe(Effect.asVoid),
      publish: (event) => Queue.offer(events, event).pipe(Effect.asVoid),
      integration: () => {
        const state: { name?: string; methods: Method[] } = { methods: [] }
        const editor = {
          list: () => [],
          get: () => undefined,
          update: (_id: string, update: (integration: { id: string; name: string }) => void) => {
            const ref = { id: "nimble", name: state.name ?? "" }
            update(ref)
            state.name = ref.name
          },
          remove: () => {},
          method: {
            list: () => state.methods,
            update: (input: { method: Method }) => {
              state.methods = state.methods.filter((method) => method.type !== input.method.type)
              state.methods.push(input.method)
            },
            remove: () => {},
          },
        } as unknown as IntegrationEditor
        integrationTransforms.forEach((transform) => transform(editor))
        return state
      },
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
          active: () =>
            Effect.sync(() => (host.key === undefined ? undefined : { type: "credential", id: "cred_1", label: "Nimble", method: "key" })),
          resolve: () => Effect.sync(() => (host.key === undefined ? undefined : { type: "key", key: host.key })),
        },
      },
      event: {
        subscribe: (input?: { types?: ReadonlyArray<string> }) =>
          Stream.fromQueue(events).pipe(Stream.filter((event) => !input?.types || input.types.includes(event.type))),
      },
      websearch: {
        transform: (callback: (editor: WebSearchEditor) => void) =>
          Effect.sync(() => {
            websearchTransforms.push(callback)
            return registration
          }),
        reload: () => Effect.sync(() => void reloads++),
      },
    }
    ;(host as { ctx: Context }).ctx = ctx as unknown as Context
    return host
  })

/** Waits for the plugin's event fiber to catch up, since events arrive asynchronously. */
export const eventually = (check: () => boolean) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 200; attempt++) {
      if (check()) return
      yield* Effect.sleep("5 millis")
    }
    throw new Error("condition not met within 1 second")
  })
