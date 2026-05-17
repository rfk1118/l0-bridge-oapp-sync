# l0-bridge-oapp-sync

GitHub Actions cron that keeps the LayerZero V2 OApp index for [l0-bridge](https://github.com/neal-zhu/layerzero-bridge) fresh.

## What it does

Every 30 minutes, for each Tier A LayerZero V2 EVM mainnet:

1. Read the GitHub-tracked JSON file `data/oapp-index-{eid}.json`.
2. Scan `EndpointV2.PacketSent` from `meta.lastSyncedBlock + 1` up to chain head, capped to keep one chain from starving the others.
3. Multicall `token()` on every newly-seen sender to classify it as **adapter** (returns a different ERC20), **native OFT** (returns self), or **unknown** (reverts — generic LZ messaging app).
4. Merge into the JSON file, advance `lastSyncedBlock`, and commit the updated files back to GitHub.

The runtime in the main app can read these GitHub JSON files on top of its bundled snapshots when resolving a user-pasted ERC20 to an OFT adapter.

## Why a separate public repo?

The main `l0-bridge` repo is private; private-repo Actions minutes are capped (2000 / month on the Free plan) and Vercel Hobby cron is daily-only. A public repo gets unlimited Actions minutes, so a per-30-minute cron costs nothing.

## Setup

No Redis setup or GitHub secret is required. The workflow uses `GITHUB_TOKEN` with `contents: write` permission to commit generated JSON files into this repository.

### Manual run

```bash
npm install
npx tsx scripts/sync.ts
```

Filter to a single chain via `EID=30110 npx tsx scripts/sync.ts`.

## Adding a chain

Edit [scripts/lib/chains.ts](scripts/lib/chains.ts) — add an entry with the chain's `eid`, `chainId`, `chainKey`, `endpointV2` address (look up in the [LayerZero deployments metadata](https://metadata.layerzero-api.com/v1/metadata/deployments)) and a curated RPC list. Push; the next cron picks it up.

## Removing the Vercel cron

The main `l0-bridge` repo previously shipped a Vercel cron handler at `api/cron/sync-oapp-index.ts`. Once this workflow is running, that handler can be removed from `vercel.json` (it stays as a manual webhook if useful).

## Monitoring

- Workflow runs: https://github.com/neal-zhu/l0-bridge-oapp-sync/actions
- Each run logs per-chain summary (`OK fromBlock=... newSenders=... adaptersAdded=...`)
- Bad RPCs surface as `EXCEPTION` or `no RPC responded` in the log
