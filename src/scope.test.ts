// AUTHORED-BY Claude Opus 4.8 (Fable unavailable) — re-review/upgrade candidate.
import { describe, expect, it } from "vitest";
import { assertWithinBase, isContainerUrl, normalizeContainer } from "./scope.js";

const CONTAINER = "https://alice.pod/notes/my-doc/";

describe("normalizeContainer", () => {
  it("adds exactly one trailing slash", () => {
    expect(normalizeContainer("https://alice.pod/notes/my-doc")).toBe(CONTAINER);
    expect(normalizeContainer(CONTAINER)).toBe(CONTAINER);
  });

  it("strips query and fragment", () => {
    expect(normalizeContainer("https://alice.pod/notes/my-doc/?x=1#frag")).toBe(CONTAINER);
  });

  it("rejects a non-absolute URL", () => {
    expect(() => normalizeContainer("notes/my-doc/")).toThrow(/absolute URL/);
  });

  it("rejects a non-http(s) protocol", () => {
    expect(() => normalizeContainer("file:///etc/passwd")).toThrow(/http\(s\)/);
    expect(() => normalizeContainer("ftp://host/x/")).toThrow(/http\(s\)/);
  });
});

describe("assertWithinBase", () => {
  it("accepts a strict descendant resource", () => {
    expect(() => assertWithinBase(CONTAINER, `${CONTAINER}update-1`)).not.toThrow();
    expect(() => assertWithinBase(CONTAINER, `${CONTAINER}sub/update-2`)).not.toThrow();
  });

  it("REJECTS a foreign origin (the core SSRF guard)", () => {
    expect(() => assertWithinBase(CONTAINER, "https://evil.example/notes/my-doc/u")).toThrow(
      /escapes container origin/,
    );
    // Same host, different scheme is still a different origin.
    expect(() => assertWithinBase(CONTAINER, "http://alice.pod/notes/my-doc/u")).toThrow(
      /escapes container origin/,
    );
    // Same host, different PORT is a different origin.
    expect(() => assertWithinBase(CONTAINER, "https://alice.pod:8443/notes/my-doc/u")).toThrow(
      /escapes container origin/,
    );
  });

  it("REJECTS a sibling path that is not under the container", () => {
    expect(() => assertWithinBase(CONTAINER, "https://alice.pod/notes/other-doc/u")).toThrow(
      /escapes container path/,
    );
    // A path-prefix sibling (`/notes/my-doc-evil/`) must NOT pass — the container
    // path ends in a slash so the prefix check is exact at the boundary.
    expect(() => assertWithinBase(CONTAINER, "https://alice.pod/notes/my-doc-evil/u")).toThrow(
      /escapes container path/,
    );
  });

  it("REJECTS the container root by default (not a managed resource)", () => {
    expect(() => assertWithinBase(CONTAINER, CONTAINER)).toThrow(/container root/);
    // Root aliases with a query/fragment are also rejected.
    expect(() => assertWithinBase(CONTAINER, `${CONTAINER}?x=1`)).toThrow(/container root/);
    expect(() => assertWithinBase(CONTAINER, `${CONTAINER}#frag`)).toThrow(/container root/);
  });

  it("accepts the container root only with allowRoot (listing case)", () => {
    expect(() => assertWithinBase(CONTAINER, CONTAINER, { allowRoot: true })).not.toThrow();
  });

  it("REJECTS an invalid target URL", () => {
    expect(() => assertWithinBase(CONTAINER, "not a url")).toThrow(/invalid/);
  });
});

describe("isContainerUrl", () => {
  it("is true for a trailing-slash path, false otherwise", () => {
    expect(isContainerUrl(CONTAINER)).toBe(true);
    expect(isContainerUrl(`${CONTAINER}update-1`)).toBe(false);
    // A query/fragment must not fool the check.
    expect(isContainerUrl(`${CONTAINER}update-1?x=1`)).toBe(false);
  });
});
