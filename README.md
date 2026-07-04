# @jeswr/rxdb-solid

An **[RxDB](https://rxdb.info) replication plugin that syncs an RxDB collection to/from a
[Solid](https://solidproject.org) pod.**

RxDB is a popular offline-first, reactive client database. Its generic
[replication protocol](https://rxdb.info/replication.html) lets a collection sync against any
backend that supplies a `pull` + `push` handler. `@jeswr/rxdb-solid` makes that backend the
**user's own Solid pod**: one pod resource per document, read/written through an **injectable
authenticated `fetch`** (bring your own Solid auth library; this package imports none).

```ts
import { createRxDatabase } from "rxdb";
import { getRxStorageMemory } from "rxdb/plugins/storage-memory";
import { replicateSolid } from "@jeswr/rxdb-solid";

const db = await createRxDatabase({ name: "app", storage: getRxStorageMemory() });
const { items } = await db.addCollections({
  items: {
    schema: {
      version: 0,
      primaryKey: "id",
      type: "object",
      properties: { id: { type: "string", maxLength: 200 }, title: { type: "string" } },
      required: ["id", "title"],
    },
  },
});

const replication = replicateSolid({
  collection: items,
  container: "https://alice.solidpod.example/app/items/", // a pod container
  fetch: session.fetch, // an authenticated fetch (e.g. from @solid/reactive-authentication)
});

await replication.awaitInitialReplication(); // the collection is now synced with the pod

// Edit as normal — every local change replicates to the pod automatically (live).
await items.insert({ id: "a", title: "Hello, Solid!" });
```

## Installation

This package is **GitHub-installable today** (npm publish is a deferred migration). It ships a
committed, self-contained `dist/`, so it installs with **no build step** even under
`ignore-scripts=true`:

```sh
npm install github:jeswr/rxdb-solid#main rxdb
```

`rxdb` is a **peerDependency** — it is *not* bundled, so you install it alongside (you already
depend on it). Node >= 20 or any modern browser.

## What it does

- **Push** — every local insert/update/delete RxDB hands the plugin is written to the pod as one
  resource per document (an upsert; deletions write a tombstone — see below).
- **Pull** — documents changed on the pod since the last checkpoint are pulled back into the
  collection, incrementally.
- **Live** (default) — with `live: true` the replication keeps running; call `replication.reSync()`
  to pull the latest (wire this to a notification for real-time cross-client sync — the seam below).
- **Conflicts** — detected by the plugin and resolved by **RxDB's collection `conflictHandler`**
  (you supply the strategy; the plugin never picks a winner).

## Public API

```ts
function replicateSolid<RxDocType>(
  options: SolidReplicationOptions<RxDocType>,
): RxReplicationState<RxDocType, SolidCheckpoint>;

interface SolidReplicationOptions<RxDocType> {
  collection: RxCollection<RxDocType>; // the RxDB collection to replicate
  container: string;                   // absolute pod container URL (normalised internally)
  fetch: typeof globalThis.fetch;      // an already-authenticated fetch (the auth seam)
  replicationIdentifier?: string;      // default: derived from the container URL
  live?: boolean;                      // default true
  retryTime?: number;                  // default 5000 (ms)
  batchSize?: number;                  // pull + push batch size, default 50
  toRdf?: (doc: WithDeleted<RxDocType>) => { body: string; contentType: string };
  fromRdf?: (body: string, contentType: string) => WithDeleted<RxDocType>;
}

interface SolidCheckpoint {
  id: string;        // primary key of the last document in the previous pull batch
  updatedAt: number; // its monotonic write number
}
```

Returned: the RxDB [`RxReplicationState`](https://rxdb.info/replication.html) — `await
awaitInitialReplication()` / `awaitInSync()`, subscribe to `received$` / `sent$` / `error$`, and
call `reSync()` / `cancel()`.

Also exported:

- `SolidDocStore`, `type SolidDocStoreOptions`, `type FetchedDoc`, `DOC_CONTENT_TYPE`,
  `META_RESOURCE_NAME` (`@jeswr/rxdb-solid/store`) — the lower-level LDP client.
- `keyToResourceName`, `resourceNameToKey` — the injective, reversible key sanitiser.
- `assertWithinPodScope`, `isContainerUrl`, `normalizePodBase`, `PodScopeError` (re-exported from
  [`@jeswr/guarded-fetch`](https://github.com/jeswr/guarded-fetch)) — the fail-closed pod-scope
  guard this store's every operation runs through.
- `type RdfSerialization`.

### Conflict handling

Set a `conflictHandler` on the collection (RxDB's standard mechanism). On a conflict the plugin
returns the pod's **real master state**; RxDB then runs your handler to produce the resolved
document, which is re-pushed. With no custom handler, RxDB's default keeps the master state.

```ts
const { items } = await db.addCollections({
  items: { schema, conflictHandler: { isEqual, resolve } },
});
```

## Design decisions

- **JSON-default storage, with an optional RDF seam.** Each document's master state
  (`WithDeleted<RxDocType>` — the doc fields plus `_deleted`) is stored as **one pod resource**.
  By default that resource is `application/json` (a small versioned envelope `{ v: 1, doc }`).
  Supplying `toRdf`/`fromRdf` stores each document as **Linked Data** instead (e.g. `text/turtle`
  / JSON-LD). The seam **must** round-trip the document state — including `_deleted` and the
  primary key — losslessly; the plugin's own bookkeeping is never asked of the seam (it rides in
  the metadata resource, below). `toRdf` and `fromRdf` must be supplied **together** (or neither).

- **Resource naming / key sanitisation (the SSRF surface).** A document's primary key is arbitrary
  consumer-controlled text, so it is mapped to a safe in-container resource name by
  `keyToResourceName`: every byte outside the unreserved set `[A-Za-z0-9-]` is escaped as
  `_` + two uppercase hex digits (the `_` introducer is itself escaped), wrapped as
  `doc.<encoded>.json`. The output alphabet contains **no** `/`, **no** `..`, **no** `%`, **no**
  whitespace or control bytes, and nothing the WHATWG URL parser normalises — so
  `container + keyToResourceName(key)` is **always** a strict descendant of the container for any
  key, making traversal *structurally* impossible. The encoding is **injective** and
  **reversible** (`resourceNameToKey`), so a pulled resource round-trips back to its key. The
  scope guard (below) is the defence-in-depth backstop.

- **Checkpoint shape `{ id, updatedAt }`.** `updatedAt` is a **monotonic per-collection write
  counter** (not wall-clock), so the pull order is a *total* order immune to clock skew. A pull
  returns documents ordered by `(updatedAt, id)` that sort strictly after the checkpoint, capped
  at `batchSize`; the next checkpoint is the last returned document's `{ id, updatedAt }`. A null
  checkpoint pulls from the beginning. A pod has no server-side query, so the handler lists +
  consults the metadata index client-side (O(n) in the document count per pull cycle).

- **Per-collection metadata resource.** A single small JSON resource (`meta.json`) under the
  container holds the monotonic `counter` and a `{ resourceName -> updatedAt }` index. The counter
  stamps every write; the index lets pull compute "changed since the checkpoint" without parsing
  every document body. It lives in a namespace the key sanitiser can never reach (document names
  are always `doc.<…>.json`), and is filtered out of document listings, so it is never surfaced
  as a document.

- **Tombstone soft-deletes, not hard DELETE.** Deleting a document writes a **tombstone** resource
  (the doc state with `_deleted: true`) rather than hard-DELETEing it, so other clients can *pull*
  the deletion. Reclaiming the bytes is an explicit **garbage-collection seam**
  (`SolidDocStore.deleteDoc`), not part of normal replication.

- **Conflict resolution is RxDB's job.** The push handler does pure conflict *detection* — the
  pod's current master state vs the fork's `assumedMasterState` (deep-equal) — and returns the real
  master for any conflicting row. RxDB then invokes the collection's `conflictHandler`. This plugin
  never picks a winner, and a stale fork write never clobbers a newer pod state.

- **Conditional writes close the read-then-write race.** Detection alone has a TOCTOU window: a
  concurrent client could write *between* the handler's read and its write. So every document write
  is **conditional** — an atomic create (`If-None-Match: *`) for a new resource, or an optimistic
  update (`If-Match: <etag>`) for an existing one. A precondition failure (HTTP 412) is treated as a
  conflict: the handler re-reads the now-current master and returns it for RxDB to resolve, so a
  concurrent write is never silently lost. The shared metadata resource is likewise written with a
  conditional, **retried** `If-Match` (re-reading + re-applying this push's index entries against the
  fresh monotonic counter on a 412), so concurrent pushes can never lose each other's index entries.
  (Fallback: against a server that returns **no** ETag, an optimistic update is impossible, so the
  document write degrades to a best-effort overwrite — the suite's servers all return ETags.)

- **Orphan self-healing (partial-write durability).** A document body is written *before* its
  metadata index entry, so a crash or an exhausted meta-commit retry could leave a document on the
  pod with no index entry — invisible to pulls. Every metadata commit therefore also **re-indexes
  any document resource present in the container listing but absent from the index**, so the next
  push (or the same push's retry) recovers any orphan. The sweep is best-effort: a listing error
  never blocks committing the current push's own entries.

## Security: a fail-closed scope guard

Every URL the store reads, writes, or deletes is asserted to lie **under the configured container**
before any request, via [`@jeswr/guarded-fetch`](https://github.com/jeswr/guarded-fetch)'s
consolidated pod-scope guard (`assertWithinPodScope` — same-origin, segment-boundary
path-prefixed, and the container root itself rejected for resource ops via `{ allowRoot: false }`).
A hostile or buggy server that lists a foreign-origin or path-escaping member can never make the
plugin touch it: such members are skipped on read and rejected on write. The container is the one
security boundary, applied as defence-in-depth on *every* operation. The auth seam is strict:
`rxdb-solid` performs **no** crypto/DPoP and imports **no** concrete auth library — you inject an
already-authenticated `fetch`.

RDF discipline: the only RDF parsed is the LDP **container listing** (read-only), via
[`@jeswr/fetch-rdf`](https://github.com/jeswr/fetch-rdf) +
[`@solid/object`](https://www.npmjs.com/package/@solid/object) — never hand-built triples. Document
payloads are JSON, or the consumer's own RDF via the seam.

## Live cross-client pull — a documented seam (follow-up, not built here)

With `live: true` the replication re-runs the pull on `reSync()`. To get **real-time** remote
changes without polling, wire a **Solid Notifications `WebSocketChannel2023`** subscription on the
container and call `replication.reSync()` on a change notification (see the suite's
`solid-notifications` helper):

```ts
// on a container-change notification:
replication.reSync();
```

Wiring a notifications channel into the plugin directly (so live pull is automatic) is a tracked
follow-up — see the repo issues.

## Development

```sh
npm run gate   # lint (Biome) + typecheck (tsc) + test (vitest) + build + check:dist + check:lockfile-transport
```

The built `dist/` is **committed** (so the package is GitHub-installable with no build step);
`check:dist` fails the gate if it drifts from a fresh build — rebuild + commit `dist/` alongside
any `src/` change.

## License

[MIT](./LICENSE) © Jesse Wright
