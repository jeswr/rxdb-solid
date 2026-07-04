/**
 * COMPATIBILITY SHIM for the `@jeswr/rxdb-solid/scope` public subpath.
 *
 * The container-scope guard has been consolidated into
 * [`@jeswr/guarded-fetch`](https://github.com/jeswr/guarded-fetch)'s reviewed
 * `podScope` primitives (the suite's ONE home for the "is this URL within my
 * configured container?" capability check). This module re-exports those
 * primitives under the LEGACY names this package used to publish, so existing
 * consumers importing `@jeswr/rxdb-solid/scope` keep working unchanged.
 *
 * @deprecated Prefer importing `assertWithinPodScope` / `normalizePodBase` /
 * `isContainerUrl` / `PodScopeError` directly from `@jeswr/guarded-fetch`. These
 * legacy aliases are retained only for backwards compatibility and may be removed
 * in a future major version.
 *
 * **Pure core, no platform.** Only the WHATWG `URL` global — browser-safe.
 */
import { isContainerUrl as isContainerUrlImpl } from "@jeswr/guarded-fetch";
/**
 * @deprecated Use `isContainerUrl` from `@jeswr/guarded-fetch`.
 * True iff `url` is a container (LDP convention: a trailing slash on the path).
 */
export declare const isContainerUrl: typeof isContainerUrlImpl;
/**
 * @deprecated Use `normalizePodBase` from `@jeswr/guarded-fetch`.
 * Normalise a container URL to exactly one trailing slash; throws if it is not an
 * absolute http(s) URL.
 */
export declare function normalizeContainer(container: string): string;
/**
 * @deprecated Use `assertWithinPodScope` from `@jeswr/guarded-fetch`.
 *
 * Fail-closed assertion that `url` is within the store's container sub-tree.
 * BEHAVIOUR-PRESERVING shim: the legacy `assertWithinBase` REJECTED the container
 * root by default (the store's document resources are minted strictly UNDER the
 * container — write-target semantics), so this defaults `allowRoot` to `false`,
 * whereas `assertWithinPodScope` defaults it to `true`. Returns `void`, matching
 * the legacy signature (the canonical URL that `assertWithinPodScope` returns is
 * discarded here — call `assertWithinPodScope` directly if you need it).
 */
export declare function assertWithinBase(container: string, url: string, opts?: {
    allowRoot?: boolean;
}): void;
//# sourceMappingURL=scope.d.ts.map