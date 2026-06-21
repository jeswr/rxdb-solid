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
export declare function normalizeContainer(container: string): string;
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
export declare function assertWithinBase(container: string, url: string, opts?: {
    allowRoot?: boolean;
}): void;
/** True iff `url` is a container (LDP convention: a trailing slash on the path). */
export declare function isContainerUrl(url: string): boolean;
//# sourceMappingURL=scope.d.ts.map