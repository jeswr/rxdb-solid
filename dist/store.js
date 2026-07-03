// AUTHORED-BY Claude Opus 4.8 (Fable unavailable) — re-review/upgrade candidate.
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
import { parseRdf } from "@jeswr/fetch-rdf";
import { ContainerDataset } from "@solid/object";
import { DataFactory } from "n3";
import { assertWithinBase, isContainerUrl, normalizeContainer } from "./scope.js";
/** Default media type a document resource is stored with (JSON storage mode). */
export const DOC_CONTENT_TYPE = "application/json";
/**
 * The reserved resource name for the per-collection metadata resource. It lives
 * in a NAMESPACE that {@link keyToResourceName} can never reach: a document
 * resource name is always `doc.<encoded>.json`, and the metadata name is exactly
 * `meta.json`, so the two can never collide. {@link listDocUrls} filters this
 * resource out so it is never surfaced as a document.
 */
export const META_RESOURCE_NAME = "meta.json";
/**
 * Default {@link SolidDocStoreOptions.maxResponseBytes} — 64 MiB. Generous for
 * a single JSON/RDF document while still bounding memory against a hostile
 * server that streams an endless body.
 */
export const DEFAULT_MAX_RESPONSE_BYTES = 64 * 1024 * 1024;
/**
 * Fail-closed refusal of a REDIRECTED response on a credentialed request.
 *
 * Every request the store issues carries the caller's authenticated `fetch`
 * (DPoP-bound or Bearer credentials). If that request were transparently
 * followed to a `Location:` on ANOTHER origin, the credential (and the request
 * body) could be replayed to an attacker-controlled server — a
 * credential-exfiltration / SSRF vector. So every request is issued with
 * `redirect: "manual"` and this guard REFUSES any redirect rather than
 * following it: undici surfaces the raw 3xx (`status` 300–399), browsers
 * surface an `opaqueredirect` filtered response (`type === "opaqueredirect"`,
 * `status === 0`), and a `redirected` flag is checked defensively.
 */
function assertNotRedirected(res, url) {
    if (res.type === "opaqueredirect" ||
        res.redirected === true ||
        (res.status >= 300 && res.status < 400)) {
        throw new Error(`[rxdb-solid] refusing to follow a redirect from ${url} (status ${res.status}, type ${res.type}) — a credentialed request must not be redirected to another location`);
    }
}
/** Total byte length of a UTF-8 string (no allocation of a second buffer). */
function utf8ByteLength(s) {
    return new TextEncoder().encode(s).length;
}
/**
 * Read a response body to text with a HARD byte cap. Reads the body stream
 * chunk-by-chunk, aborting (and cancelling the stream) the instant the running
 * total exceeds `maxBytes`, so an oversized / endless body from a hostile
 * server can never be buffered into memory. An advertised `Content-Length`
 * over the cap is refused up front. Falls back to a bounded `res.text()` only
 * when the environment exposes no readable stream.
 */
async function readTextBounded(res, maxBytes, url) {
    const advertised = res.headers.get("content-length");
    if (advertised !== null) {
        const n = Number(advertised);
        if (Number.isFinite(n) && n > maxBytes) {
            throw new Error(`[rxdb-solid] response body from ${url} exceeds the ${maxBytes}-byte limit (content-length ${n})`);
        }
    }
    const stream = res.body;
    if (!stream) {
        const text = await res.text();
        if (utf8ByteLength(text) > maxBytes) {
            throw new Error(`[rxdb-solid] response body from ${url} exceeds the ${maxBytes}-byte limit`);
        }
        return text;
    }
    const reader = stream.getReader();
    const chunks = [];
    let total = 0;
    try {
        while (true) {
            const { done, value } = await reader.read();
            if (done)
                break;
            if (!value)
                continue;
            total += value.byteLength;
            if (total > maxBytes) {
                throw new Error(`[rxdb-solid] response body from ${url} exceeds the ${maxBytes}-byte limit`);
            }
            chunks.push(value);
        }
    }
    finally {
        // Release the lock / abort any remaining body (no-op once fully drained).
        await reader.cancel().catch(() => { });
    }
    const merged = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
        merged.set(chunk, offset);
        offset += chunk.byteLength;
    }
    return new TextDecoder("utf-8").decode(merged);
}
// ---------------------------------------------------------------------------
// Key sanitisation — SSRF-critical, injective + reversible.
// ---------------------------------------------------------------------------
/**
 * The fixed prefix every DOCUMENT resource name carries, keeping document
 * resources in a namespace disjoint from {@link META_RESOURCE_NAME}.
 */
const DOC_PREFIX = "doc.";
/** The fixed suffix every document resource name carries. */
const DOC_SUFFIX = ".json";
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
export function keyToResourceName(key) {
    const bytes = new TextEncoder().encode(key);
    let out = "";
    for (const b of bytes) {
        // Unreserved, escape-introducer-free alphabet: A-Z a-z 0-9 '-'.
        const isUpper = b >= 0x41 && b <= 0x5a;
        const isLower = b >= 0x61 && b <= 0x7a;
        const isDigit = b >= 0x30 && b <= 0x39;
        const isDash = b === 0x2d; // '-'
        if (isUpper || isLower || isDigit || isDash) {
            out += String.fromCharCode(b);
        }
        else {
            // `_` + two uppercase hex digits. `_` itself (0x5f) lands here, so it is
            // escaped too — the encoding is unambiguous and round-trips.
            out += `_${b.toString(16).toUpperCase().padStart(2, "0")}`;
        }
    }
    return `${DOC_PREFIX}${out}${DOC_SUFFIX}`;
}
/**
 * The inverse of {@link keyToResourceName}: decode a document resource name back
 * to its original primary key. Throws if `name` is not a well-formed document
 * resource name produced by {@link keyToResourceName} (so a foreign / malformed
 * listing entry is rejected, never silently mis-decoded).
 */
export function resourceNameToKey(name) {
    if (!name.startsWith(DOC_PREFIX) || !name.endsWith(DOC_SUFFIX)) {
        throw new Error(`[rxdb-solid] not a document resource name: ${name}`);
    }
    const body = name.slice(DOC_PREFIX.length, name.length - DOC_SUFFIX.length);
    const bytes = [];
    for (let i = 0; i < body.length; i++) {
        const ch = body[i];
        if (ch === "_") {
            const hex = body.slice(i + 1, i + 3);
            if (hex.length !== 2 || !/^[0-9A-F]{2}$/.test(hex)) {
                throw new Error(`[rxdb-solid] malformed escape in resource name: ${name}`);
            }
            bytes.push(Number.parseInt(hex, 16));
            i += 2;
        }
        else {
            const code = ch.charCodeAt(0);
            // Only the unreserved alphabet is valid OUTSIDE an escape.
            const ok = (code >= 0x41 && code <= 0x5a) ||
                (code >= 0x61 && code <= 0x7a) ||
                (code >= 0x30 && code <= 0x39) ||
                code === 0x2d;
            if (!ok) {
                throw new Error(`[rxdb-solid] unexpected character in resource name: ${name}`);
            }
            bytes.push(code);
        }
    }
    return new TextDecoder("utf-8", { fatal: false }).decode(new Uint8Array(bytes));
}
/** True iff `name` is the metadata resource name (never a document). */
function isMetaName(name) {
    return name === META_RESOURCE_NAME;
}
/**
 * The per-document/per-collection LDP store under one container.
 *
 * Construct with an absolute container URL + an authenticated fetch. The
 * constructor rejects a non-http(s) container and normalises it to a single
 * trailing slash.
 */
export class SolidDocStore {
    /** The normalised container URL (one trailing slash). */
    container;
    fetch;
    maxResponseBytes;
    constructor(options) {
        // normalizeContainer throws on a non-http(s) / non-absolute container.
        this.container = normalizeContainer(options.container);
        this.fetch = options.fetch;
        this.maxResponseBytes = options.maxResponseBytes ?? DEFAULT_MAX_RESPONSE_BYTES;
    }
    /** The absolute URL of the resource named `resourceName` under the container. */
    resourceUrl(resourceName) {
        const url = `${this.container}${resourceName}`;
        assertWithinBase(this.container, url);
        return url;
    }
    /** The absolute URL a primary `key` maps to (its sanitised document resource). */
    docUrl(key) {
        return this.resourceUrl(keyToResourceName(key));
    }
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
    async putDoc(resourceName, body, contentType, opts) {
        const url = this.resourceUrl(resourceName);
        const headers = { "content-type": contentType };
        if (opts?.ifMatch)
            headers["if-match"] = opts.ifMatch;
        if (opts?.ifNoneMatch)
            headers["if-none-match"] = opts.ifNoneMatch;
        const res = await this.fetch(url, { method: "PUT", headers, body, redirect: "manual" });
        // A credentialed write must never be followed to another Location (SSRF /
        // credential-exfil). Refuse any redirect BEFORE interpreting the status.
        assertNotRedirected(res, url);
        // 412 Precondition Failed (and 428 Precondition Required) are the concurrency
        // signal, not a hard error — let the caller reconcile.
        if (res.status === 412 || res.status === 428) {
            return { ok: false, precondition: "failed" };
        }
        if (!res.ok) {
            throw new Error(`[rxdb-solid] putDoc ${url} failed: ${res.status} ${res.statusText}`);
        }
        return { ok: true, url, etag: res.headers.get("etag") };
    }
    /**
     * GET a single resource. Returns `null` for a missing resource (404/410).
     *
     * @throws if the target is outside the container, or on any other non-ok
     *   response.
     */
    async getDoc(resourceName) {
        const url = this.resourceUrl(resourceName);
        const res = await this.fetch(url, {
            method: "GET",
            headers: { accept: "application/json, text/turtle, application/ld+json;q=0.9, */*;q=0.1" },
            redirect: "manual",
        });
        assertNotRedirected(res, url);
        if (res.status === 404 || res.status === 410) {
            return null;
        }
        if (!res.ok) {
            throw new Error(`[rxdb-solid] getDoc ${url} failed: ${res.status} ${res.statusText}`);
        }
        const body = await readTextBounded(res, this.maxResponseBytes, url);
        return {
            body,
            contentType: res.headers.get("content-type") ?? DOC_CONTENT_TYPE,
            etag: res.headers.get("etag"),
        };
    }
    /**
     * DELETE a single resource. A missing resource (404/410) is treated as
     * already-deleted (no throw) — the default replication path uses TOMBSTONES
     * (a `_deleted` write) rather than a hard DELETE, so this is only the explicit
     * GC seam.
     *
     * @throws if the target is outside the container, or on any other non-ok
     *   response.
     */
    async deleteDoc(resourceName) {
        const url = this.resourceUrl(resourceName);
        const res = await this.fetch(url, { method: "DELETE", redirect: "manual" });
        assertNotRedirected(res, url);
        if (res.status === 404 || res.status === 410) {
            return;
        }
        if (!res.ok) {
            throw new Error(`[rxdb-solid] deleteDoc ${url} failed: ${res.status} ${res.statusText}`);
        }
    }
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
    async listDocUrls() {
        const res = await this.fetch(this.container, {
            method: "GET",
            headers: { accept: "text/turtle, application/ld+json;q=0.9" },
            redirect: "manual",
        });
        assertNotRedirected(res, this.container);
        if (res.status === 404 || res.status === 410) {
            return [];
        }
        if (!res.ok) {
            throw new Error(`[rxdb-solid] list ${this.container} failed: ${res.status} ${res.statusText}`);
        }
        const body = await readTextBounded(res, this.maxResponseBytes, this.container);
        // parseRdf resolves relative IRIs against the container URL (baseIRI), so
        // ldp:contains object IRIs come back absolute.
        const dataset = await parseRdf(body, res.headers.get("content-type"), {
            baseIRI: this.container,
        });
        const container = new ContainerDataset(dataset, DataFactory).container;
        if (!container) {
            // A valid but empty / non-container document — no members.
            return [];
        }
        const urls = [];
        // The container lists ITSELF as a member; skip it. Compare on the normalised
        // origin + pathname (ignoring any query/fragment) so a root ALIAS a hostile
        // or buggy server might list — `…/c/?x=1`, `…/c/#frag` — is skipped too.
        const base = new URL(this.container);
        for (const resource of container.contains) {
            // resource.id may be relative; resolve against the container URL to be safe.
            const absolute = new URL(resource.id, this.container).toString();
            const member = new URL(absolute);
            if (member.origin === base.origin && member.pathname === base.pathname) {
                continue;
            }
            // A document resource is never a (sub-)container.
            if (isContainerUrl(absolute)) {
                continue;
            }
            // Defence in depth: never surface a member that escapes the container.
            try {
                assertWithinBase(this.container, absolute, { allowRoot: true });
            }
            catch {
                continue;
            }
            // The member must be a DIRECT child of the container (no nested path) and a
            // well-formed document resource name. Strip the container prefix to get the
            // bare resource name.
            const name = absolute.slice(this.container.length);
            if (name.includes("/")) {
                // A resource nested under a sub-path — not a document we manage.
                continue;
            }
            if (isMetaName(name)) {
                // The metadata resource is never a document.
                continue;
            }
            if (!name.startsWith(DOC_PREFIX) || !name.endsWith(DOC_SUFFIX)) {
                // Not one of our document resources (foreign content) — ignore it.
                continue;
            }
            urls.push(absolute);
        }
        urls.sort();
        return urls;
    }
    /**
     * Map an absolute (or container-relative) document resource URL back to its
     * bare resource name (the segment after the container). Throws if the URL is
     * not a direct child of the container.
     */
    urlToResourceName(url) {
        const absolute = new URL(url, this.container).toString();
        assertWithinBase(this.container, absolute);
        const name = absolute.slice(this.container.length);
        if (name.length === 0 || name.includes("/")) {
            throw new Error(`[rxdb-solid] ${url} is not a direct child resource of the container`);
        }
        return name;
    }
}
//# sourceMappingURL=store.js.map