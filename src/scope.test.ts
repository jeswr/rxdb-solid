// AUTHORED-BY Claude Sonnet 5
// Compat-shim contract for `@jeswr/rxdb-solid/scope`: the legacy names now
// delegate to @jeswr/guarded-fetch's podScope, but the BEHAVIOUR they published
// must be preserved (esp. `assertWithinBase` rejecting the container root by
// default — the write-target semantics rxdb-solid relies on).
import { describe, expect, it } from "vitest";
import { assertWithinBase, isContainerUrl, normalizeContainer } from "./scope.js";

const CONTAINER = "https://alice.pod/notes/my-doc/";

describe("normalizeContainer (compat)", () => {
  it("adds exactly one trailing slash", () => {
    expect(normalizeContainer("https://alice.pod/notes/my-doc")).toBe(CONTAINER);
    expect(normalizeContainer(CONTAINER)).toBe(CONTAINER);
  });

  it("rejects a non-http(s) protocol", () => {
    expect(() => normalizeContainer("file:///etc/passwd")).toThrow();
  });
});

describe("assertWithinBase (compat)", () => {
  it("accepts a strict descendant resource", () => {
    expect(() => assertWithinBase(CONTAINER, `${CONTAINER}update-1`)).not.toThrow();
  });

  it("REJECTS the container root BY DEFAULT (write-target semantics preserved)", () => {
    // The load-bearing compat behaviour: the legacy default was allowRoot:false.
    expect(() => assertWithinBase(CONTAINER, CONTAINER)).toThrow();
  });

  it("accepts the container root only with allowRoot:true", () => {
    expect(() => assertWithinBase(CONTAINER, CONTAINER, { allowRoot: true })).not.toThrow();
  });

  it("REJECTS a foreign origin", () => {
    expect(() => assertWithinBase(CONTAINER, "https://evil.example/notes/my-doc/u")).toThrow();
  });

  it("REJECTS a path-prefix sibling at the segment boundary", () => {
    expect(() => assertWithinBase(CONTAINER, "https://alice.pod/notes/my-doc-evil/u")).toThrow();
  });

  it("returns void (legacy signature)", () => {
    expect(assertWithinBase(CONTAINER, `${CONTAINER}update-1`)).toBeUndefined();
  });
});

describe("isContainerUrl (compat)", () => {
  it("is true for a trailing-slash path, false otherwise", () => {
    expect(isContainerUrl(CONTAINER)).toBe(true);
    expect(isContainerUrl(`${CONTAINER}update-1`)).toBe(false);
  });
});
