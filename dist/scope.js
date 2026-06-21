// AUTHORED-BY Claude Opus 4.8 (Fable unavailable) — re-review/upgrade candidate.
/**
 * Container-scope guard for the rxdb-solid store (see `./store.ts`).
 *
 * The configured container is the store's primary SECURITY surface: every URL
 * the store issues an authenticated request to MUST lie under that container.
 * This module is the one reviewed home for normalising the container and
 * asserting that a target URL is `container` itself or a strict descendant of it
 * — a defence-in-depth check applied to every write target and every listed
 * member, so a hostile / buggy server cannot make the store touch a foreign
 * origin or escape the container sub-tree. (Adapted from `@jeswr/solid-memory`'s
 * `scope.ts`, itself from `@jeswr/unstorage-solid`'s `keys.ts`.)
 *
 * **Pure core, no platform.** Only the WHATWG `URL` global — no `node:*`, no RDF,
 * no RxDB — so it is usable in a browser Solid client.
 */
/**
 * Normalise a container URL to exactly one trailing slash. Throws if it is not an
 * absolute http(s) URL. A container must not carry a query or fragment.
 */
export function normalizeContainer(container) {
    let url;
    try {
        url = new URL(container);
    }
    catch {
        throw new Error(`[rxdb-solid] \`container\` must be an absolute URL, got: ${container}`);
    }
    if (url.protocol !== "http:" && url.protocol !== "https:") {
        throw new Error(`[rxdb-solid] \`container\` must be an http(s) URL, got protocol: ${url.protocol}`);
    }
    // Collapse the path to a single trailing slash; preserve everything else.
    if (!url.pathname.endsWith("/")) {
        url.pathname = `${url.pathname}/`;
    }
    // A container is an address, not a query/fragment target.
    url.search = "";
    url.hash = "";
    return url.toString();
}
/**
 * Fail-closed assertion that `url` is within the store's container sub-tree:
 * same origin and a path prefixed by the container path.
 *
 * **The container ROOT itself is rejected by default.** The store's document
 * resources are minted UNDER the container; the container root is never a write
 * target (PUT/GET/DELETE on the root would touch the container document itself,
 * a footgun that could clobber or read the container). So by default
 * `url === container` (after trailing-slash normalisation) is REFUSED. Pass
 * `{ allowRoot: true }` for the one legitimate case — validating a member URL
 * that may *be* the container in a listing — where the caller skips/handles the
 * root separately.
 *
 * Guards against any encoding/normalisation trick producing a URL outside the
 * pod sub-tree the store owns.
 */
export function assertWithinBase(container, url, opts) {
    const b = new URL(container);
    let u;
    try {
        u = new URL(url);
    }
    catch {
        throw new Error(`[rxdb-solid] target URL is invalid: ${url}`);
    }
    if (u.origin !== b.origin) {
        throw new Error(`[rxdb-solid] target URL ${url} escapes container origin ${b.origin} (refused)`);
    }
    if (!u.pathname.startsWith(b.pathname)) {
        throw new Error(`[rxdb-solid] target URL ${url} escapes container path ${b.pathname} (refused)`);
    }
    // Reject the container ROOT itself for resource access — it is not a managed
    // document resource, and acting on it (PUT/DELETE/GET) would target the
    // container document. Compare on the normalised path+origin (ignoring any
    // query/fragment, which a target never carries) so trailing-slash / `?`/`#`
    // variants of the root cannot slip through.
    if (opts?.allowRoot !== true && u.origin === b.origin && u.pathname === b.pathname) {
        throw new Error(`[rxdb-solid] target URL ${url} is the container root, not a managed resource (refused)`);
    }
}
/** True iff `url` is a container (LDP convention: a trailing slash on the path). */
export function isContainerUrl(url) {
    // Compare on the path so a query/fragment (which a container address never has)
    // cannot fool the check.
    try {
        return new URL(url).pathname.endsWith("/");
    }
    catch {
        return url.endsWith("/");
    }
}
//# sourceMappingURL=scope.js.map