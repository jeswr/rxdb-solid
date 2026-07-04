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
 * {@link SolidDocStore}'s `assertWithinPodScope` guard (`@jeswr/guarded-fetch`),
 * and every primary key is mapped to an in-container resource name by an
 * injective, traversal-proof encoder — see {@link ./store.js}.
 */

import type { RxCollection, WithDeleted } from "rxdb";
import { type RxReplicationState, replicateRxCollection } from "rxdb/plugins/replication";
import {
  DOC_CONTENT_TYPE,
  keyToResourceName,
  META_RESOURCE_NAME,
  resourceNameToKey,
  SolidDocStore,
} from "./store.js";

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

/** The on-pod envelope a JSON-stored document is wrapped in. */
interface JsonEnvelope<RxDocType> {
  /** Envelope schema version (forward-compat). */
  v: 1;
  /** The document master state. */
  doc: WithDeleted<RxDocType>;
}

/** The on-pod per-collection metadata resource. */
interface Meta {
  v: 1;
  /** Strictly-increasing write counter; the source of every `updatedAt`. */
  counter: number;
  /** Map of document resource name → the `updatedAt` of its latest write. */
  index: Record<string, number>;
}

const EMPTY_META: Meta = { v: 1, counter: 0, index: {} };

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
function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a !== typeof b) return false;
  if (a === null || b === null) return a === b;
  if (typeof a !== "object") return false;
  const aArr = Array.isArray(a);
  const bArr = Array.isArray(b);
  if (aArr !== bArr) return false;
  if (aArr && bArr) {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      if (!deepEqual(a[i], (b as unknown[])[i])) return false;
    }
    return true;
  }
  const ao = a as Record<string, unknown>;
  const bo = b as Record<string, unknown>;
  const ak = Object.keys(ao);
  const bk = Object.keys(bo);
  if (ak.length !== bk.length) return false;
  for (const k of ak) {
    if (!Object.hasOwn(bo, k)) return false;
    if (!deepEqual(ao[k], bo[k])) return false;
  }
  return true;
}

/** Compare two checkpoints' `(updatedAt, id)` total order: <0, 0, >0. */
function compareCheckpoint(a: SolidCheckpoint, b: SolidCheckpoint): number {
  if (a.updatedAt !== b.updatedAt) return a.updatedAt - b.updatedAt;
  if (a.id < b.id) return -1;
  if (a.id > b.id) return 1;
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
export function replicateSolid<RxDocType>(
  options: SolidReplicationOptions<RxDocType>,
): RxReplicationState<RxDocType, SolidCheckpoint> {
  const { collection, container, fetch, live = true, retryTime = 5000, batchSize = 50 } = options;
  if ((options.toRdf && !options.fromRdf) || (!options.toRdf && options.fromRdf)) {
    throw new Error("[rxdb-solid] `toRdf` and `fromRdf` must be supplied together (or neither).");
  }
  const store = new SolidDocStore({ container, fetch });
  // The primary-key field name (e.g. "id"), read from the collection schema.
  const primaryPath = collection.schema.primaryPath as keyof WithDeleted<RxDocType> & string;
  const replicationIdentifier = options.replicationIdentifier ?? `rxdb-solid:${store.container}`;

  /**
   * Read the document master state for `resourceName` (with its ETag for an
   * optimistic write). Returns `null` if the resource is absent OR cannot be
   * deserialised (a corrupt / foreign body) — a parse error never escapes, so a
   * single bad resource can never break a whole pull/push. The push treats an
   * unreadable existing resource as a conflict (it will not clobber it).
   */
  async function readDoc(
    resourceName: string,
  ): Promise<{ doc: WithDeleted<RxDocType>; etag: string | null } | null> {
    const fetched = await store.getDoc(resourceName);
    if (!fetched) return null;
    try {
      const doc = options.fromRdf
        ? options.fromRdf(fetched.body, fetched.contentType)
        : (JSON.parse(fetched.body) as JsonEnvelope<RxDocType>).doc;
      // A JSON envelope must actually carry a `doc` object; reject a shape that
      // parsed but is not one of ours.
      if (doc === null || typeof doc !== "object") return null;
      return { doc, etag: fetched.etag };
    } catch {
      return null;
    }
  }

  /**
   * Read a document ONLY if it is SELF-CONSISTENT for `resourceName`: the body
   * deserialises (via {@link readDoc}) AND the document's OWN primary key maps
   * back to exactly this resource name. Returns `null` otherwise. This is the one
   * gate used everywhere a stored resource is turned into a replicated document
   * (both orphan re-indexing AND the pull), so a resource whose body's primary
   * key disagrees with its name — externally written or corrupted — is never
   * pulled under the wrong checkpoint key.
   */
  async function readConsistentDoc(
    resourceName: string,
  ): Promise<{ doc: WithDeleted<RxDocType>; etag: string | null } | null> {
    const read = await readDoc(resourceName);
    if (read === null) return null;
    const docKey = String((read.doc as Record<string, unknown>)[primaryPath]);
    if (keyToResourceName(docKey) !== resourceName) return null;
    return read;
  }

  /**
   * True iff `resourceName` is a USABLE managed document: its name decodes to a
   * key whose canonical resource name round-trips (so a foreign / odd
   * `doc.*.json` is excluded), AND it is self-consistent (body deserialises and
   * its primary key maps back to this name). Used to gate orphan re-indexing so
   * an unmanaged / corrupt / mismatched resource is never promoted into the index.
   */
  async function isUsableDoc(resourceName: string): Promise<boolean> {
    let key: string;
    try {
      key = resourceNameToKey(resourceName);
    } catch {
      return false;
    }
    // The canonical encoding of the decoded key must equal the resource name —
    // rejects any non-canonical / ambiguous name a foreign tool may have minted.
    if (keyToResourceName(key) !== resourceName) return false;
    // …AND the body must deserialise to a SELF-CONSISTENT document.
    return (await readConsistentDoc(resourceName)) !== null;
  }

  /** Serialise the document master state to its on-pod body + content type. */
  function serializeDoc(doc: WithDeleted<RxDocType>): { body: string; contentType: string } {
    if (options.toRdf) return options.toRdf(doc);
    const envelope: JsonEnvelope<RxDocType> = { v: 1, doc };
    return { body: JSON.stringify(envelope), contentType: DOC_CONTENT_TYPE };
  }

  /**
   * Conditionally write the document master state. `precondition` selects the
   * concurrency guard: `"create"` (only if absent), or an `if-match` ETag (only
   * if unchanged). Returns `true` on success, `false` on a precondition failure
   * (a concurrent write — the caller treats it as a conflict).
   */
  async function writeDocConditional(
    resourceName: string,
    doc: WithDeleted<RxDocType>,
    precondition: { create: true } | { ifMatch: string },
  ): Promise<boolean> {
    const { body, contentType } = serializeDoc(doc);
    const opts =
      "create" in precondition ? { ifNoneMatch: "*" } : { ifMatch: precondition.ifMatch };
    const res = await store.putDoc(resourceName, body, contentType, opts);
    return res.ok;
  }

  /**
   * Unconditional overwrite — the fallback for a server that returns no ETag (so
   * an optimistic `if-match` write is impossible). Always "succeeds" (returns
   * true) on a non-error response. See the README concurrency note.
   */
  async function writeDocBestEffort(
    resourceName: string,
    doc: WithDeleted<RxDocType>,
  ): Promise<boolean> {
    const { body, contentType } = serializeDoc(doc);
    const res = await store.putDoc(resourceName, body, contentType);
    return res.ok;
  }

  /** Read the per-collection metadata resource (with its ETag) or the empty default. */
  async function readMeta(): Promise<{ meta: Meta; etag: string | null }> {
    const fetched = await store.getDoc(META_RESOURCE_NAME);
    if (!fetched) return { meta: { ...EMPTY_META, index: {} }, etag: null };
    try {
      const parsed = JSON.parse(fetched.body) as Meta;
      if (parsed && parsed.v === 1 && typeof parsed.counter === "number" && parsed.index) {
        return { meta: parsed, etag: fetched.etag };
      }
    } catch {
      // Corrupt/foreign metadata — start fresh rather than throw (the index is
      // bookkeeping; a fresh index re-derives ordering from the next writes).
    }
    return { meta: { ...EMPTY_META, index: {} }, etag: fetched.etag };
  }

  /**
   * PULL handler: return documents changed strictly AFTER `checkpoint`, ordered
   * by `(updatedAt, id)`, capped at `batchSize`, with the next checkpoint = the
   * last returned document's `{ id, updatedAt }`.
   */
  async function pullHandler(
    checkpoint: SolidCheckpoint | undefined,
    limit: number,
  ): Promise<{ checkpoint: SolidCheckpoint | undefined; documents: WithDeleted<RxDocType>[] }> {
    const { meta } = await readMeta();
    // Candidate document resources = the listing intersected with the metadata
    // index (the index is the authority on updatedAt; the listing confirms the
    // resource still exists). A resource in the listing but missing from the
    // index (e.g. written by another tool) is skipped — we only replicate our own.
    const urls = await store.listDocUrls();
    const candidates: { name: string; key: string; updatedAt: number }[] = [];
    for (const url of urls) {
      const name = store.urlToResourceName(url);
      const updatedAt = meta.index[name];
      if (typeof updatedAt !== "number") continue;
      let key: string;
      try {
        key = resourceNameToKey(name);
      } catch {
        continue;
      }
      candidates.push({ name, key, updatedAt });
    }
    // Total order by (updatedAt, id).
    candidates.sort((a, b) =>
      compareCheckpoint(
        { id: a.key, updatedAt: a.updatedAt },
        { id: b.key, updatedAt: b.updatedAt },
      ),
    );
    // Keep only those strictly after the checkpoint.
    const after = checkpoint
      ? candidates.filter(
          (c) => compareCheckpoint({ id: c.key, updatedAt: c.updatedAt }, checkpoint) > 0,
        )
      : candidates;
    // Collect up to `limit` READABLE documents, scanning BEYOND the initial slice
    // when entries are skipped, and ALWAYS advancing the checkpoint past every
    // candidate we process (readable or not). This guarantees forward progress:
    // a batch of only corrupt/unreadable indexed entries still advances the
    // checkpoint, so later valid documents are eventually reached (no stall).
    const documents: WithDeleted<RxDocType>[] = [];
    let next: SolidCheckpoint | undefined = checkpoint;
    for (const c of after) {
      if (documents.length >= limit) break;
      const candidateCp: SolidCheckpoint = { id: c.key, updatedAt: c.updatedAt };
      // Read ONLY a self-consistent document (body deserialises AND its primary
      // key maps back to this resource name) — so an externally-written /
      // corrupted indexed resource whose body's id disagrees with its name is
      // never pulled under the wrong checkpoint key.
      const read = await readConsistentDoc(c.name);
      // Advance the checkpoint to this candidate regardless of readability — a
      // skipped (raced-away / unreadable / inconsistent) entry must not pin the
      // checkpoint (no stall).
      next = candidateCp;
      if (!read) continue;
      documents.push(read.doc);
    }
    return { checkpoint: next, documents };
  }

  /**
   * PUSH handler: for each fork write row, detect a conflict against the pod's
   * current master state; on no conflict, write the document with a CONDITIONAL
   * write (atomic create / `if-match` ETag) so a concurrent writer can never be
   * silently clobbered — a precondition failure is treated as a conflict. Then
   * record the written resource names and apply them to the metadata index under
   * a conditional, retried `if-match` write (so concurrent pushes can never lose
   * each other's index entries). Returns the real master state for every
   * conflicting row (RxDB then runs the collection's `conflictHandler`).
   */
  async function pushHandler(
    rows: {
      assumedMasterState?: WithDeleted<RxDocType>;
      newDocumentState: WithDeleted<RxDocType>;
    }[],
  ): Promise<WithDeleted<RxDocType>[]> {
    const conflicts: WithDeleted<RxDocType>[] = [];
    // Resource names whose document body we successfully (re)wrote in this push;
    // their metadata index entries are reconciled together afterwards.
    const written: string[] = [];

    for (const row of rows) {
      const key = String(row.newDocumentState[primaryPath]);
      const resourceName = keyToResourceName(key);
      const current = await readDoc(resourceName);

      // Conflict iff the pod's CURRENT master state differs from what the fork
      // assumed. (No assumedMasterState ⇒ the fork believes this is a fresh
      // insert; then any existing pod state is a conflict.)
      const assumed = row.assumedMasterState;
      const isConflict = assumed
        ? current === null || !deepEqual(stripDoc(current.doc), stripDoc(assumed))
        : current !== null;
      if (isConflict) {
        conflicts.push(
          current !== null
            ? current.doc
            : // The fork assumed a master the pod no longer has (deleted out from
              // under it). Surface a tombstone so RxDB reconciles against a real
              // `WithDeleted` value.
              asTombstone(assumed as WithDeleted<RxDocType>),
        );
        continue;
      }

      // No conflict by the assumed-state check — write CONDITIONALLY so a write
      // that raced in AFTER our read (same ETag window) cannot be clobbered.
      // create ⇒ if-none-match:* ; update ⇒ if-match:<etag>. If the server gave
      // no ETag (rare; the suite's servers all do), an optimistic update is not
      // possible, so we fall back to a best-effort overwrite — documented in the
      // README's concurrency note.
      const ok = await (current === null
        ? writeDocConditional(resourceName, row.newDocumentState, { create: true })
        : current.etag
          ? writeDocConditional(resourceName, row.newDocumentState, { ifMatch: current.etag })
          : writeDocBestEffort(resourceName, row.newDocumentState));
      if (!ok) {
        // The precondition failed — another client wrote concurrently. Re-read
        // the now-current master and surface it as a conflict for RxDB to resolve.
        const fresh = await readDoc(resourceName);
        conflicts.push(fresh !== null ? fresh.doc : asTombstone(row.newDocumentState));
        continue;
      }
      written.push(resourceName);
    }

    if (written.length > 0) {
      await commitMetaIndex(written);
    }
    return conflicts;
  }

  /**
   * Apply `written` resource names to the metadata index under a conditional,
   * retried `if-match` write so concurrent pushes never lose each other's index
   * entries (Finding 2). Each retry re-reads the fresh meta, re-applies our
   * pending names against the fresh monotonic counter, and re-writes with the
   * fresh ETag; a precondition failure just loops. Bounded so a pathological
   * server can't spin forever.
   *
   * **Orphan reconciliation (Finding B durability).** A document body is written
   * BEFORE its index entry, so a prior crash / exhausted-retry meta commit could
   * leave a document on the pod with NO index entry, invisible to pulls. To make
   * a partial write self-healing, every commit also re-indexes any document
   * resource present in the container LISTING but absent from the fresh index —
   * so the very next push (or this push's retry) recovers any orphan. The orphan
   * sweep is best-effort: a listing failure does not block committing `written`.
   */
  async function commitMetaIndex(written: readonly string[]): Promise<void> {
    const MaxAttempts = 10;
    for (let attempt = 0; attempt < MaxAttempts; attempt++) {
      const { meta, etag } = await readMeta();

      // Find orphans: document resources that exist on the pod but are missing
      // from the (fresh) index. Best-effort — never let a listing error abort the
      // commit of our own `written` entries. CRITICALLY, only re-index an orphan
      // we can actually USE: its name must decode to a key whose canonical
      // resource name round-trips (so a foreign/odd `doc.*.json` is not promoted),
      // AND its body must deserialise (so a corrupt body never enters the index
      // and breaks a later pull). An unusable resource is left out — it stayed
      // invisible before and stays invisible now.
      let orphans: string[] = [];
      try {
        const urls = await store.listDocUrls();
        const candidates = urls
          .map((u) => store.urlToResourceName(u))
          .filter((name) => typeof meta.index[name] !== "number" && !written.includes(name));
        for (const name of candidates) {
          if (await isUsableDoc(name)) orphans.push(name);
        }
      } catch {
        orphans = [];
      }

      // Stamp orphans first, then our written names, from the FRESH counter — both
      // get a strictly-increasing updatedAt so they become pull-discoverable.
      // (Deterministic order: orphans sorted, then the written order.)
      for (const name of [...orphans].sort()) {
        meta.counter += 1;
        meta.index[name] = meta.counter;
      }
      for (const name of written) {
        meta.counter += 1;
        meta.index[name] = meta.counter;
      }
      const body = JSON.stringify(meta);
      const res = await store.putDoc(
        META_RESOURCE_NAME,
        body,
        DOC_CONTENT_TYPE,
        // Create atomically if the meta did not exist; otherwise require the ETag
        // we just read — a concurrent meta write fails the precondition + retries.
        etag === null ? { ifNoneMatch: "*" } : { ifMatch: etag },
      );
      if (res.ok) return;
      // Precondition failed — a concurrent push updated meta. Loop to re-read +
      // re-apply our names against the now-fresh counter.
    }
    throw new Error(
      "[rxdb-solid] failed to commit the metadata index after repeated concurrent updates",
    );
  }

  return replicateRxCollection<RxDocType, SolidCheckpoint>({
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
function stripDoc<RxDocType>(doc: WithDeleted<RxDocType>): Record<string, unknown> {
  const rest = { ...(doc as Record<string, unknown>) };
  // The replication-protocol `_attachments` stub (if any) is not part of the doc
  // identity for conflict purposes.
  delete rest[ATTACHMENTS_FIELD];
  return rest;
}

/** Build a `_deleted: true` tombstone of `doc` (the RxDB wire deletion marker). */
function asTombstone<RxDocType>(doc: WithDeleted<RxDocType>): WithDeleted<RxDocType> {
  return { ...(doc as Record<string, unknown>), [DELETED_FIELD]: true } as WithDeleted<RxDocType>;
}
