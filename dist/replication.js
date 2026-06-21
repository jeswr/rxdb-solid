// AUTHORED-BY Claude Opus 4.8 (Fable unavailable) — re-review/upgrade candidate.
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
 * {@link SolidDocStore}'s `assertWithinBase` guard, and every primary key is
 * mapped to an in-container resource name by an injective, traversal-proof
 * encoder — see {@link ./store.js}.
 */
import { replicateRxCollection } from "rxdb/plugins/replication";
import { DOC_CONTENT_TYPE, keyToResourceName, META_RESOURCE_NAME, resourceNameToKey, SolidDocStore, } from "./store.js";
const EMPTY_META = { v: 1, counter: 0, index: {} };
/**
 * RxDB replication-protocol wire field names. They are underscore-prefixed by
 * RxDB (not a naming choice of ours), so we reference them via these constants
 * — computed member access — rather than inline identifiers.
 */
const DELETED_FIELD = "_deleted";
const ATTACHMENTS_FIELD = "_attachments";
/**
 * Deep structural equality over JSON-serialisable values (used to compare the
 * pod's master document state against the fork's `assumedMasterState` for
 * conflict detection). Order-insensitive across object keys.
 */
function deepEqual(a, b) {
    if (a === b)
        return true;
    if (typeof a !== typeof b)
        return false;
    if (a === null || b === null)
        return a === b;
    if (typeof a !== "object")
        return false;
    const aArr = Array.isArray(a);
    const bArr = Array.isArray(b);
    if (aArr !== bArr)
        return false;
    if (aArr && bArr) {
        if (a.length !== b.length)
            return false;
        for (let i = 0; i < a.length; i++) {
            if (!deepEqual(a[i], b[i]))
                return false;
        }
        return true;
    }
    const ao = a;
    const bo = b;
    const ak = Object.keys(ao);
    const bk = Object.keys(bo);
    if (ak.length !== bk.length)
        return false;
    for (const k of ak) {
        if (!Object.hasOwn(bo, k))
            return false;
        if (!deepEqual(ao[k], bo[k]))
            return false;
    }
    return true;
}
/** Compare two checkpoints' `(updatedAt, id)` total order: <0, 0, >0. */
function compareCheckpoint(a, b) {
    if (a.updatedAt !== b.updatedAt)
        return a.updatedAt - b.updatedAt;
    if (a.id < b.id)
        return -1;
    if (a.id > b.id)
        return 1;
    return 0;
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
export function replicateSolid(options) {
    const { collection, container, fetch, live = true, retryTime = 5000, batchSize = 50 } = options;
    if ((options.toRdf && !options.fromRdf) || (!options.toRdf && options.fromRdf)) {
        throw new Error("[rxdb-solid] `toRdf` and `fromRdf` must be supplied together (or neither).");
    }
    const store = new SolidDocStore({ container, fetch });
    // The primary-key field name (e.g. "id"), read from the collection schema.
    const primaryPath = collection.schema.primaryPath;
    const replicationIdentifier = options.replicationIdentifier ?? `rxdb-solid:${store.container}`;
    /** Read the document master state for `resourceName`, or null if absent. */
    async function readDoc(resourceName) {
        const fetched = await store.getDoc(resourceName);
        if (!fetched)
            return null;
        if (options.fromRdf) {
            return options.fromRdf(fetched.body, fetched.contentType);
        }
        const envelope = JSON.parse(fetched.body);
        return envelope.doc;
    }
    /** Serialise + PUT the document master state to its resource. */
    async function writeDoc(resourceName, doc) {
        if (options.toRdf) {
            const { body, contentType } = options.toRdf(doc);
            await store.putDoc(resourceName, body, contentType);
            return;
        }
        const envelope = { v: 1, doc };
        await store.putDoc(resourceName, JSON.stringify(envelope), DOC_CONTENT_TYPE);
    }
    /** Read the per-collection metadata resource (or the empty default). */
    async function readMeta() {
        const fetched = await store.getDoc(META_RESOURCE_NAME);
        if (!fetched)
            return { ...EMPTY_META, index: {} };
        try {
            const parsed = JSON.parse(fetched.body);
            if (parsed && parsed.v === 1 && typeof parsed.counter === "number" && parsed.index) {
                return parsed;
            }
        }
        catch {
            // Corrupt/foreign metadata — start fresh rather than throw (the index is
            // bookkeeping; a fresh index re-derives ordering from the next writes).
        }
        return { ...EMPTY_META, index: {} };
    }
    /** Write the per-collection metadata resource. */
    async function writeMeta(meta) {
        await store.putDoc(META_RESOURCE_NAME, JSON.stringify(meta), DOC_CONTENT_TYPE);
    }
    /**
     * PULL handler: return documents changed strictly AFTER `checkpoint`, ordered
     * by `(updatedAt, id)`, capped at `batchSize`, with the next checkpoint = the
     * last returned document's `{ id, updatedAt }`.
     */
    async function pullHandler(checkpoint, limit) {
        const meta = await readMeta();
        // Candidate document resources = the listing intersected with the metadata
        // index (the index is the authority on updatedAt; the listing confirms the
        // resource still exists). A resource in the listing but missing from the
        // index (e.g. written by another tool) is skipped — we only replicate our own.
        const urls = await store.listDocUrls();
        const candidates = [];
        for (const url of urls) {
            const name = store.urlToResourceName(url);
            const updatedAt = meta.index[name];
            if (typeof updatedAt !== "number")
                continue;
            let key;
            try {
                key = resourceNameToKey(name);
            }
            catch {
                continue;
            }
            candidates.push({ name, key, updatedAt });
        }
        // Total order by (updatedAt, id).
        candidates.sort((a, b) => compareCheckpoint({ id: a.key, updatedAt: a.updatedAt }, { id: b.key, updatedAt: b.updatedAt }));
        // Keep only those strictly after the checkpoint.
        const after = checkpoint
            ? candidates.filter((c) => compareCheckpoint({ id: c.key, updatedAt: c.updatedAt }, checkpoint) > 0)
            : candidates;
        const batch = after.slice(0, limit);
        const documents = [];
        let next = checkpoint;
        for (const c of batch) {
            const doc = await readDoc(c.name);
            if (!doc)
                continue; // Raced away between listing + read; skip.
            documents.push(doc);
            next = { id: c.key, updatedAt: c.updatedAt };
        }
        return { checkpoint: next, documents };
    }
    /**
     * PUSH handler: for each fork write row, detect a conflict against the pod's
     * current master state; on no conflict, write the document (tombstone for a
     * deletion) and bump the metadata index. Returns the real master state for any
     * conflicting row (RxDB then runs the collection's `conflictHandler`).
     */
    async function pushHandler(rows) {
        const conflicts = [];
        // Read the metadata once, mutate locally, write once at the end (fewer pod
        // round-trips; the writes within a single push are serialised here).
        const meta = await readMeta();
        for (const row of rows) {
            const key = String(row.newDocumentState[primaryPath]);
            const resourceName = keyToResourceName(key);
            const current = await readDoc(resourceName);
            // Conflict iff the pod's CURRENT master state differs from what the fork
            // assumed. (No assumedMasterState ⇒ the fork believes this is a fresh
            // insert; then any existing pod state is a conflict.)
            const assumed = row.assumedMasterState;
            const conflict = assumed
                ? current === null || !deepEqual(stripDoc(current), stripDoc(assumed))
                : current !== null;
            if (conflict) {
                if (current !== null) {
                    conflicts.push(current);
                }
                else {
                    // The fork assumed a master that the pod no longer has (deleted out
                    // from under it). Surface a tombstone of the assumed state so RxDB's
                    // conflict handler can reconcile against a real `WithDeleted` value.
                    conflicts.push(asTombstone(assumed));
                }
                continue;
            }
            // No conflict — write the new master state (tombstone when _deleted).
            await writeDoc(resourceName, row.newDocumentState);
            meta.counter += 1;
            meta.index[resourceName] = meta.counter;
        }
        await writeMeta(meta);
        return conflicts;
    }
    return replicateRxCollection({
        collection,
        replicationIdentifier,
        live,
        retryTime,
        deletedField: "_deleted",
        pull: {
            handler: pullHandler,
            batchSize,
        },
        push: {
            handler: pushHandler,
            batchSize,
        },
    });
}
/**
 * A view of a `WithDeleted` doc for conflict comparison. RxDB strips its internal
 * `_meta`/`_rev`/`_attachments` before they ever reach a replication handler
 * (the master state is `WithDeleted<RxDocType>`), but a defensively-stored pod
 * resource might carry an `_attachments` stub; drop it so it cannot spuriously
 * register as a conflict.
 */
function stripDoc(doc) {
    const rest = { ...doc };
    // The replication-protocol `_attachments` stub (if any) is not part of the doc
    // identity for conflict purposes.
    delete rest[ATTACHMENTS_FIELD];
    return rest;
}
/** Build a `_deleted: true` tombstone of `doc` (the RxDB wire deletion marker). */
function asTombstone(doc) {
    return { ...doc, [DELETED_FIELD]: true };
}
//# sourceMappingURL=replication.js.map