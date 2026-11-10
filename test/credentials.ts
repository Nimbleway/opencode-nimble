// The fake host's saved keys: the key saved with /connect per integration, and the active connection and its key.
import type { IntegrationState } from "./host.js"

export type Connection =
  | { type: "credential"; id: string; label: string; method: "key" }
  | { type: "env"; name: string }

/** The keys saved with /connect, and the active connection and its key for an integration. */
export const credentialStore = (env: Map<string, string>, integrations: () => Map<string, IntegrationState>) => {
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
  // Like OpenCode: a saved credential comes first, then an env variable the integration's env method names.
  const active = (integrationID: string): Connection | undefined => {
    const credential = credentials.get(integrationID)
    if (credential) return { type: "credential", id: credential.id, label: integrationID, method: "key" }
    const name = integrations()
      .get(integrationID)
      ?.methods.flatMap((method) => (method.type === "env" ? method.names : []))
      .find((name) => env.has(name))
    return name === undefined ? undefined : { type: "env", name }
  }
  const resolve = (connection: Connection) => {
    const key =
      connection.type === "env"
        ? env.get(connection.name)
        : [...credentials.values()].find((credential) => credential.id === connection.id)?.key
    return key === undefined ? undefined : { type: "key" as const, key }
  }
  return { key: (integrationID: string) => credentials.get(integrationID)?.key, save, active, resolve }
}
