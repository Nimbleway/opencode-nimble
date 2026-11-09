# @nimble-way/opencode

Nimble web search for [OpenCode](https://opencode.ai), as an OpenCode plugin.

The plugin adds [Nimble](https://nimbleway.com) as a provider for OpenCode's built-in `websearch` tool. When the model
searches the web, OpenCode sends the query to Nimble's Search API and gets back ranked results with the page text the
model needs to answer.

## Requirements

- OpenCode 2.2.5 or later (OpenCode V2 plugins).
- A Nimble API key from your [Nimble](https://nimbleway.com) account.

## Install

```bash
opencode plugin add @nimble-way/opencode
```

Or add it to `plugins` in your `opencode.json(c)`:

```jsonc title="opencode.jsonc"
{
  "$schema": "https://opencode.ai/config.json",
  "plugins": ["@nimble-way/opencode"],
}
```

## Set your Nimble API key

Run `/connect` in OpenCode, choose **Nimble** and enter your key. OpenCode stores it with your other credentials.

Or set `NIMBLE_API_KEY` in the environment OpenCode's server starts in:

```bash
NIMBLE_API_KEY=your-key opencode
```

A key saved with `/connect` takes precedence over `NIMBLE_API_KEY`.

Nimble shows up as a web search provider only while a key is available. Add a key and it appears without a restart;
remove it and Nimble leaves the list. That way OpenCode never picks Nimble for a search it can't run.

## Use Nimble for web search

The first web search asks you to allow web search and pick a provider. To always use Nimble, set it in
`opencode.json(c)`:

```jsonc title="opencode.jsonc"
{
  "$schema": "https://opencode.ai/config.json",
  "websearch": {
    "provider": "nimble",
  },
}
```

With `"provider": "random"`, Nimble is one of the providers OpenCode picks from. If Nimble returns HTTP 429, OpenCode
retries the search with another provider.

## What it sends

Each search is one request to `https://sdk.nimbleway.com/v2/search` with:

- the query, `search_depth: "standard"` and `max_results: 8`
- your key in the `Authorization: Bearer` header
- an `X-Client-Source: opencode` header, so Nimble can tell the request came from OpenCode
- OpenCode's usual `User-Agent`

Nimble returns the page text for each result in its `description` field, which the plugin passes to the model as the
result's content. A search that takes longer than 25 seconds fails, like OpenCode's built-in providers.

The plugin never logs or echoes your key.

## Cost

Each web search is one Nimble Search request at standard depth, which costs $5.00 per 1,000 searches (half a cent per
search) on Nimble's [published pricing](https://docs.nimbleway.com/nimble-sdk/admin/pricing). Only successful requests
are billed. Your plan may differ, so check your Nimble account for the rate that applies to you.

## Troubleshooting

- **Nimble isn't in the provider list.** Check that a key is connected: `/connect` should show Nimble as connected, or
  `NIMBLE_API_KEY` must be set where OpenCode's server runs (a background service started earlier doesn't see a
  variable you set later).
- **"Web search authentication failed (HTTP 401)".** Nimble rejected the key. Connect a valid key with `/connect`.

## Disable

Remove `@nimble-way/opencode` from `plugins`, or add `"-nimble"` to disable it without uninstalling.

## Development

```bash
pnpm install
pnpm run typecheck
pnpm test
```

The tests run offline against a fake `fetch` and a fake OpenCode host. They need no key and don't contact Nimble.

To try a local checkout in OpenCode, build it (`pnpm run build`) and add the checkout's absolute path to `plugins`.

## License

MIT. See [LICENSE](LICENSE).
