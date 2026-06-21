/**
 * `SolidDocStore` — the LDP client for {@link ./replication.js | replicateSolid}.
 *
 * It stores ONE pod resource per RxDB document under a single configured
 * container. Each document body is JSON by default (an envelope carrying the
 * document state + the replication bookkeeping the plugin needs — see
 * {@link ./replication.js}), or a consumer-supplied RDF serialisation via the
 * `toRdf`/`fromRdf` seam. A small per-collection METADATA resource holds the
 * plugin's monotonic write counter (so checkpoint ordering is total + stable).
 *
 * **Injectable authenticated fetch.** The store does NO crypto / DPoP itself —
 * the caller injects an already-authenticated `fetch` (e.g. from
 * `@solid/reactive-authentication` or a client-credentials DPoP fetch). This
 * keeps it a pure LDP client, like `@jeswr/solid-memory` / `@jeswr/y-solid`.
 *
 * **Scope guard on every op.** Every target URL is asserted to lie under
 * `container` (see {@link ./scope.js}) before any request — defence in depth, so
 * a caller-supplied or server-listed URL can never make the store touch a
 * foreign origin or escape the container sub-tree. This is the SSRF backstop.
 *
 * **RDF discipline (house rule).** The ONLY RDF the store touches is the
 * container LISTING, parsed (read-only) via `@jeswr/fetch-rdf` `parseRdf` +
 * `@solid/object` `ContainerDataset`. Document payloads are JSON (or the
 * consumer's own RDF via the seam); we never hand-build triples.
 */
/** Default media type a document resource is stored with (JSON storage mode). */
export declare const DOC_CONTENT_TYPE = "application/json";
/**
 * The reserved resource name for the per-collection metadata resource. It lives
 * in a NAMESPACE that {@link keyToResourceName} can never reach: a document
 * resource name is always `doc.<encoded>.json`, and the metadata name is exactly
 * `meta.json`, so the two can never collide. {@link listDocUrls} filters this
 * resource out so it is never surfaced as a document.
 */
export declare const META_RESOURCE_NAME = "meta.json";
/** A document read from the pod, with the headers the replication layer needs. */
export interface FetchedDoc {
    /** The raw stored body (text for JSON / RDF; bytes only if a consumer stores binary). */
    readonly body: string;
    /** The stored content type (so `fromRdf` can dispatch). */
    readonly contentType: string;
    /** The resource's ETag, if the server returned one. */
    readonly etag: string | null;
}
/** Options for {@link SolidDocStore} construction. */
export interface SolidDocStoreOptions {
    /** Absolute container URL the store owns (normalised to one trailing slash). */
    container: string;
    /** The (authenticated) fetch the store issues every request with. */
    fetch: typeof globalThis.fetch;
}
/**
 * Encode an arbitrary consumer-controlled primary key into a SAFE in-container
 * resource name.
 *
 * **Scheme (deterministic, INJECTIVE, REVERSIBLE):** percent-encode EVERY byte
 * of the UTF-8 key that is not in the unreserved set `[A-Za-z0-9_-]`, using a
 * fixed two-hex-digit uppercase escape (`_`-introduced rather than `%` so the
 * result contains no URL-significant or percent-decodable characters at all),
 * then wrap it as `doc.<encoded>.json`.
 *
 * Concretely we escape any byte outside `[A-Za-z0-9-]` (note: `_` is the escape
 * introducer, so a literal `_` in the key is ALSO escaped — keeping the encoding
 * unambiguous and reversible). The output alphabet is therefore strictly
 * `[A-Za-z0-9-]` plus the `_` escape introducer plus the literal `doc.`/`.json`
 * affixes — containing NO `/`, NO `.` runs (`..`), NO `%`, NO whitespace, NO
 * control bytes, and nothing the WHATWG URL parser will normalise. As a result
 * `container + keyToResourceName(key)` is ALWAYS a strict descendant of the
 * container for ANY key, so {@link assertWithinBase} can never throw on it —
 * traversal is made structurally impossible, with the scope guard as the
 * defence-in-depth backstop.
 *
 * Injectivity: the encode is a byte-for-byte total function on the UTF-8 octets
 * with a single unambiguous escape, and the affixes are fixed, so distinct keys
 * always map to distinct names (no collisions, including with
 * {@link META_RESOURCE_NAME}).
 */
export declare function keyToResourceName(key: string): string;
/**
 * The inverse of {@link keyToResourceName}: decode a document resource name back
 * to its original primary key. Throws if `name` is not a well-formed document
 * resource name produced by {@link keyToResourceName} (so a foreign / malformed
 * listing entry is rejected, never silently mis-decoded).
 */
export declare function resourceNameToKey(name: string): string;
/**
 * The per-document/per-collection LDP store under one container.
 *
 * Construct with an absolute container URL + an authenticated fetch. The
 * constructor rejects a non-http(s) container and normalises it to a single
 * trailing slash.
 */
export declare class SolidDocStore {
    /** The normalised container URL (one trailing slash). */
    readonly container: string;
    private readonly fetch;
    constructor(options: SolidDocStoreOptions);
    /** The absolute URL of the resource named `resourceName` under the container. */
    resourceUrl(resourceName: string): string;
    /** The absolute URL a primary `key` maps to (its sanitised document resource). */
    docUrl(key: string): string;
    /**
     * Overwrite-capable PUT of `body` to `${container}${resourceName}` with the
     * given content type.
     *
     * **Concurrency control via an optional precondition.** Pass `ifMatch` to
     * write only if the resource's current ETag matches (an OPTIMISTIC update), or
     * `ifNoneMatch: "*"` to write only if the resource does NOT yet exist (an
     * atomic CREATE). When the server rejects the precondition (HTTP 412), this
     * returns `{ ok: false, precondition: "failed" }` rather than throwing, so the
     * caller can re-read + reconcile (the lost-update / conflict path). With no
     * precondition it is a plain overwrite.
     *
     * On success returns `{ ok: true, url, etag }` (the new ETag if reported).
     *
     * @throws if the target is outside the container, or on a non-ok response that
     *   is NOT a precondition failure.
     */
    putDoc(resourceName: string, body: string, contentType: string, opts?: {
        ifMatch?: string;
        ifNoneMatch?: string;
    }): Promise<{
        ok: true;
        url: string;
        etag: string | null;
    } | {
        ok: false;
        precondition: "failed";
    }>;
    /**
     * GET a single resource. Returns `null` for a missing resource (404/410).
     *
     * @throws if the target is outside the container, or on any other non-ok
     *   response.
     */
    getDoc(resourceName: string): Promise<FetchedDoc | null>;
    /**
     * DELETE a single resource. A missing resource (404/410) is treated as
     * already-deleted (no throw) — the default replication path uses TOMBSTONES
     * (a `_deleted` write) rather than a hard DELETE, so this is only the explicit
     * GC seam.
     *
     * @throws if the target is outside the container, or on any other non-ok
     *   response.
     */
    deleteDoc(resourceName: string): Promise<void>;
    /**
     * List the direct `ldp:contains` members of the container that are DOCUMENT
     * resources. Returns an empty array for a missing container (404/410). Each
     * member is scope-guarded against the container — a foreign-origin / escaping
     * member listed by a hostile or buggy server is skipped, never surfaced. Sub-
     * containers (trailing slash), the per-collection metadata resource, and any
     * member that is not a well-formed document resource name are skipped. The
     * result is sorted by URL (lexicographic) — deterministic order.
     *
     * @throws on any non-ok, non-404/410 response.
     */
    listDocUrls(): Promise<string[]>;
    /**
     * Map an absolute (or container-relative) document resource URL back to its
     * bare resource name (the segment after the container). Throws if the URL is
     * not a direct child of the container.
     */
    urlToResourceName(url: string): string;
}
//# sourceMappingURL=store.d.ts.map