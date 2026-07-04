// AUTHORED-BY Claude Opus 4.8 (Fable unavailable) — re-review/upgrade candidate.
/**
 * `@jeswr/rxdb-solid` — an RxDB replication plugin that syncs an RxDB collection
 * to/from a Solid pod.
 *
 * RxDB is a popular offline-first, reactive client database; its generic
 * replication protocol lets a collection sync against any backend that can supply
 * a `pull` + `push` handler. `@jeswr/rxdb-solid` makes that backend the USER'S OWN
 * Solid pod: one pod resource per document, written/read through an injectable
 * authenticated `fetch` (the auth seam — bring your own Solid auth library).
 *
 * - **JSON-default storage**, with an optional injectable `toRdf`/`fromRdf`
 *   Linked-Data seam (store each document as Turtle/JSON-LD instead).
 * - **Checkpoint-based incremental pull** over a monotonic per-collection write
 *   counter (a total order, wall-clock-independent).
 * - **Tombstone soft-deletes** (a `_deleted` write, never a hard DELETE — so
 *   deletions propagate to other clients); hard GC is an explicit seam.
 * - **Conflict resolution delegated to RxDB's collection `conflictHandler`** —
 *   this plugin only DETECTS conflicts; the consumer supplies the strategy.
 * - **Fail-closed scope guard + injective, traversal-proof key sanitisation**
 *   (the SSRF surface).
 * - **Live cross-client pull via WebSocketChannel2023 is a documented follow-up
 *   seam** (call `reSync()` on a container-change notification).
 *
 * @packageDocumentation
 */

export {
  assertWithinPodScope,
  isContainerUrl,
  normalizePodBase,
  PodScopeError,
} from "@jeswr/guarded-fetch";
export {
  type RdfSerialization,
  replicateSolid,
  type SolidCheckpoint,
  type SolidReplicationOptions,
} from "./replication.js";
export {
  DEFAULT_MAX_RESPONSE_BYTES,
  DOC_CONTENT_TYPE,
  type FetchedDoc,
  keyToResourceName,
  META_RESOURCE_NAME,
  resourceNameToKey,
  SolidDocStore,
  type SolidDocStoreOptions,
} from "./store.js";
