/**
 * `replicateSolid` — an RxDB replication plugin that syncs an RxDB collection
 * to/from a Solid pod.
 *
 * It is a thin, correct adapter over RxDB's generic
 * {@link https://rxdb.info/replication.html | `replicateRxCollection`}: the pod
 * is the REMOTE (the "master"), and this module supplies the `pull` + `push`
 * handlers that read/write pod resources through {@link ./store.js | SolidDocStore}.
 *
 * ## Storage model (the documented design decisions)
 *
 * - **One pod resource per document.** Each RxDB document's master state
 *   (`WithDeleted<RxDocType>` — the doc fields plus `_deleted`) is stored at a
 *   stable, sanitised resource name derived from the document's primary key
 *   (see {@link ./store.js | keyToResourceName}). No read-modify-write of a
 *   shared resource, so concurrent writers to *different* docs never contend.
 *
 * - **JSON by default, RDF via an injectable seam.** With no seam, a document
 *   resource is `application/json` carrying the bare `WithDeleted` doc state.
 *   Supplying `toRdf`/`fromRdf` lets a consumer store each document as Linked
 *   Data instead (e.g. `text/turtle`); the seam MUST round-trip the document
 *   state (incl. `_deleted` + the primary key) losslessly. The plugin's own
 *   replication bookkeeping (the monotonic `updatedAt`) is NEVER asked of the
 *   seam — it rides in a per-collection METADATA resource (below), so the seam
 *   only has to encode the document the consumer cares about.
 *
 * - **Per-collection metadata resource.** A single small JSON resource
 *   ({@link ./store.js | META_RESOURCE_NAME}) under the container holds a
 *   monotonic write counter and a `{ resourceName -> updatedAt }` index. The
 *   counter gives every write a strictly-increasing `updatedAt`, so the pull
 *   checkpoint is a TOTAL order independent of wall-clock skew. The index lets
 *   the pull handler compute "changed since the checkpoint" without parsing every
 *   document body. The metadata resource is filtered out of document listings,
 *   so it is never surfaced as a document.
 *
 * - **Checkpoint shape `{ id, updatedAt }`.** `id` is the last-returned
 *   document's primary key; `updatedAt` is its monotonic write number. Pull
 *   returns documents ordered by `(updatedAt, id)` that sort strictly AFTER the
 *   checkpoint, capped at `batchSize`. A null checkpoint pulls from the
 *   beginning. (A pod has no server-side query, so the handler lists + reads the
 *   metadata index client-side — O(n) in the number of docs per pull cycle; the
 *   live cross-client push path is the WebSocketChannel2023 follow-up seam,
 *   below.)
 *
 * - **Tombstone soft-deletes, not hard DELETE.** Deleting a document writes a
 *   TOMBSTONE resource (the JSON/RDF doc state with `_deleted: true`) rather than
 *   hard-DELETEing it, so other clients can PULL the deletion. Reclaiming the
 *   bytes is an explicit garbage-collection seam ({@link SolidDocStore.deleteDoc}),
 *   not part of normal replication.
 *
 * - **Conflict resolution is RxDB's job.** The push handler does pure conflict
 *   DETECTION (the pod's current master state vs the fork's `assumedMasterState`)
 *   and returns the real master state for any conflicting row. RxDB then invokes
 *   the COLLECTION's `conflictHandler` (the consumer supplies the strategy) — this
 *   plugin never picks a winner.
 *
 * - **Live cross-client pull is a documented follow-up seam.** With `live: true`
 *   the replication re-runs the pull on `reSync()`; wiring a Solid
 *   `WebSocketChannel2023` notification on the container to call `reSync()` (so
 *   remote writes flow in real time without polling) is the documented follow-up.
 *
 * **Scope guard + key sanitisation (SSRF).** Every pod request goes through
 * {@link SolidDocStore}'s `assertWithinPodScope` guard (`@jeswr/guarded-fetch`),
 * and every primary key is mapped to an in-container resource name by an
 * injective, traversal-proof encoder — see {@link ./store.js}.
 */
import type { RxCollection, WithDeleted } from "rxdb";
import { type RxReplicationState } from "rxdb/plugins/replication";
/**
 * The pull checkpoint: the last-returned document's primary key + its monotonic
 * write number. Documents that sort strictly after `(updatedAt, id)` are "new".
 */
export interface SolidCheckpoint {
    /** Primary key of the last document returned in the previous pull batch. */
    id: string;
    /** The monotonic write number (`updatedAt`) of that document. */
    updatedAt: number;
}
/**
 * The result of a consumer-supplied {@link SolidReplicationOptions.toRdf}: the
 * serialised RDF body + the media type to store it under.
 */
export interface RdfSerialization {
    body: string;
    contentType: string;
}
/** Options for {@link replicateSolid}. */
export interface SolidReplicationOptions<RxDocType> {
    /** The RxDB collection to replicate. */
    collection: RxCollection<RxDocType>;
    /** Absolute pod container URL the replication owns (normalised internally). */
    container: string;
    /** An already-authenticated `fetch` (the auth seam — bring your own Solid auth). */
    fetch: typeof globalThis.fetch;
    /**
     * Stable identifier for this replication (RxDB uses it to resume + flag
     * revisions). Defaults to a value derived from the container URL, so the same
     * container reliably resumes the same replication.
     */
    replicationIdentifier?: string;
    /** Ongoing realtime replication (re-pull on `reSync()`). Default `true`. */
    live?: boolean;
    /** Retry delay (ms) after a failed pod request. Default `5000`. */
    retryTime?: number;
    /** Pull + push batch size. Default `50`. */
    batchSize?: number;
    /**
     * OPTIONAL Linked-Data storage seam. When provided, a document is stored as
     * the returned RDF body/content-type instead of JSON. It receives the FULL
     * document state (`WithDeleted<RxDocType>`, incl. `_deleted` + the primary key)
     * and MUST encode enough to reconstruct it. Pair it with {@link fromRdf}.
     */
    toRdf?: (doc: WithDeleted<RxDocType>) => RdfSerialization;
    /**
     * OPTIONAL inverse of {@link toRdf}: parse a stored RDF body back to the
     * document state. MUST return a `WithDeleted<RxDocType>` deep-equal to what was
     * passed to `toRdf` (lossless round-trip). Required iff `toRdf` is provided.
     */
    fromRdf?: (body: string, contentType: string) => WithDeleted<RxDocType>;
}
/**
 * Replicate an RxDB collection to/from a Solid pod container. Returns the
 * RxDB {@link RxReplicationState} (await `awaitInitialReplication()` /
 * `awaitInSync()`, subscribe to `error$`, call `reSync()` / `cancel()`).
 *
 * @example
 * ```ts
 * const replication = replicateSolid({
 *   collection: db.items,
 *   container: "https://alice.pod/app/items/",
 *   fetch: session.fetch, // an authenticated fetch
 * });
 * await replication.awaitInitialReplication();
 * ```
 */
export declare function replicateSolid<RxDocType>(options: SolidReplicationOptions<RxDocType>): RxReplicationState<RxDocType, SolidCheckpoint>;
//# sourceMappingURL=replication.d.ts.map