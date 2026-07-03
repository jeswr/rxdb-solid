// AUTHORED-BY Claude Opus 4.8 (Fable unavailable) — re-review/upgrade candidate.
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it, vi } from "vitest";
import { assertWithinBase } from "./scope.js";
import {
  DEFAULT_MAX_RESPONSE_BYTES,
  DOC_CONTENT_TYPE,
  keyToResourceName,
  META_RESOURCE_NAME,
  resolveMaxResponseBytes,
  resourceNameToKey,
  SolidDocStore,
} from "./store.js";
import { makePod } from "./testPod.js";

const CONTAINER = "https://alice.pod/app/items/";

function makeStore() {
  const pod = makePod(CONTAINER);
  const store = new SolidDocStore({ container: CONTAINER, fetch: pod.fetchImpl });
  return { pod, store };
}

describe("browser-safety", () => {
  it("the store + scope modules import no node: module (browser-usable)", () => {
    for (const file of ["./store.ts", "./scope.ts"]) {
      const src = readFileSync(fileURLToPath(new URL(file, import.meta.url)), "utf8");
      expect(src).not.toMatch(/from\s+["']node:/);
    }
  });
});

describe("SolidDocStore construction", () => {
  it("normalises the container and rejects a bad one", () => {
    const pod = makePod(CONTAINER);
    const s = new SolidDocStore({ container: "https://alice.pod/app/items", fetch: pod.fetchImpl });
    expect(s.container).toBe(CONTAINER);
    expect(() => new SolidDocStore({ container: "file:///x/", fetch: pod.fetchImpl })).toThrow(
      /http\(s\)/,
    );
  });
});

describe("key sanitisation — keyToResourceName / resourceNameToKey", () => {
  // An EXHAUSTIVE adversarial table: every entry must (1) produce a resource name
  // whose URL passes assertWithinBase (no traversal/escape), and (2) round-trip.
  const adversarial = [
    "simple",
    "with space",
    "/",
    "..",
    "../../etc/passwd",
    "%2e%2e%2f",
    "%2e%2e",
    "%00",
    "\u0000", // a real NUL byte (escape, not a literal NUL in source)
    "日本語",
    "emoji-🔥-key",
    ".",
    ".leading-dot",
    "trailing-dot.",
    META_RESOURCE_NAME, // the reserved metadata name as a key
    "meta", // bare "meta"
    "doc.evil.json", // looks like one of our own resource names
    "a".repeat(4096), // very long
    "", // empty string
    "://", // scheme-like
    "https://evil.example/x", // a full foreign URL as a key
    "key/with/many/slashes",
    "back\\slash",
    "tab\tkey",
    "newline\nkey",
    "percent%41", // would decode to 'A' if naively %-decoded
    "_underscore_",
    "MixedCase-123",
    "?query#frag",
    "..%2f..%2f",
  ];

  for (const key of adversarial) {
    it(`is traversal-proof + round-trips for ${JSON.stringify(key.slice(0, 40))}`, () => {
      const name = keyToResourceName(key);
      // (1) The resource URL must never escape the container — for ANY key.
      const url = CONTAINER + name;
      expect(() => assertWithinBase(CONTAINER, url)).not.toThrow();
      // The encoded name has no path-significant or percent characters.
      expect(name).not.toMatch(/[/%]/);
      // The encoded BODY (between the fixed affixes) never contains a `..` path
      // segment — the only dots in a name are the affix dots (`doc.` / `.json`).
      const body = name.slice("doc.".length, name.length - ".json".length);
      expect(body).not.toContain(".");
      // It is a direct child (no slash) and a well-formed doc resource name.
      expect(name.startsWith("doc.")).toBe(true);
      expect(name.endsWith(".json")).toBe(true);
      // It can never collide with the metadata resource.
      expect(name).not.toBe(META_RESOURCE_NAME);
      // (2) Lossless round-trip.
      expect(resourceNameToKey(name)).toBe(key);
    });
  }

  it("is injective (distinct keys -> distinct names)", () => {
    const names = new Set(adversarial.map(keyToResourceName));
    expect(names.size).toBe(adversarial.length);
  });

  it("rejects a non-document resource name on decode", () => {
    expect(() => resourceNameToKey("meta.json")).toThrow(/document resource name/);
    expect(() => resourceNameToKey("foo")).toThrow(/document resource name/);
    expect(() => resourceNameToKey("doc._GG.json")).toThrow(/malformed escape/);
    expect(() => resourceNameToKey("doc.has space.json")).toThrow(/unexpected character/);
  });
});

describe("putDoc / getDoc / deleteDoc — round-trip", () => {
  it("writes and reads back a JSON body with the content type", async () => {
    const { pod, store } = makeStore();
    const name = keyToResourceName("a");
    const res = await store.putDoc(name, '{"hi":1}', DOC_CONTENT_TYPE);
    expect(res.ok).toBe(true);
    if (!res.ok) throw new Error("unreachable");
    expect(res.url.startsWith(CONTAINER)).toBe(true);
    expect(res.etag).toBeTruthy();
    expect(pod.store.get(res.url)?.contentType).toBe(DOC_CONTENT_TYPE);
    const got = await store.getDoc(name);
    expect(got?.body).toBe('{"hi":1}');
    expect(got?.contentType).toBe(DOC_CONTENT_TYPE);
  });

  it("getDoc returns null for a missing resource", async () => {
    const { store } = makeStore();
    expect(await store.getDoc(keyToResourceName("missing"))).toBeNull();
  });

  it("deleteDoc removes the resource and is idempotent on a missing one", async () => {
    const { store } = makeStore();
    const name = keyToResourceName("a");
    await store.putDoc(name, "{}", DOC_CONTENT_TYPE);
    await store.deleteDoc(name);
    expect(await store.getDoc(name)).toBeNull();
    // Idempotent — deleting again does not throw.
    await expect(store.deleteDoc(name)).resolves.toBeUndefined();
  });

  it("overwrites an existing resource (no precondition)", async () => {
    const { store } = makeStore();
    const name = keyToResourceName("a");
    await store.putDoc(name, '{"v":1}', DOC_CONTENT_TYPE);
    await store.putDoc(name, '{"v":2}', DOC_CONTENT_TYPE);
    expect((await store.getDoc(name))?.body).toBe('{"v":2}');
  });
});

describe("putDoc — conditional writes (concurrency control)", () => {
  // An ETag-honouring fake server: PUT with if-none-match:* fails 412 if present;
  // PUT with if-match:<etag> fails 412 unless the current etag matches.
  function condPod() {
    const store = new Map<string, { body: string; etag: string }>();
    let seq = 0;
    const fetchImpl: typeof globalThis.fetch = async (input, init) => {
      const url = typeof input === "string" ? input : (input as URL | Request).toString();
      const method = (init?.method ?? "GET").toUpperCase();
      const h = new Headers(init?.headers ?? {});
      if (method === "GET") {
        const e = store.get(url);
        if (!e) return new Response(null, { status: 404 });
        return new Response(e.body, {
          status: 200,
          headers: { "content-type": DOC_CONTENT_TYPE, etag: e.etag },
        });
      }
      if (method === "PUT") {
        const existing = store.get(url);
        if (h.get("if-none-match") === "*" && existing) return new Response(null, { status: 412 });
        if (h.has("if-match") && (!existing || existing.etag !== h.get("if-match"))) {
          return new Response(null, { status: 412 });
        }
        const etag = `"e${++seq}"`;
        store.set(url, { body: String(init?.body ?? ""), etag });
        return new Response(null, { status: existing ? 205 : 201, headers: { etag } });
      }
      return new Response(null, { status: 405 });
    };
    return { store, fetchImpl };
  }

  it("if-none-match:* creates atomically and fails (precondition) if it exists", async () => {
    const pod = condPod();
    const store = new SolidDocStore({ container: CONTAINER, fetch: pod.fetchImpl });
    const name = keyToResourceName("a");
    const first = await store.putDoc(name, "{}", DOC_CONTENT_TYPE, { ifNoneMatch: "*" });
    expect(first.ok).toBe(true);
    const second = await store.putDoc(name, "{}", DOC_CONTENT_TYPE, { ifNoneMatch: "*" });
    expect(second.ok).toBe(false);
    if (second.ok) throw new Error("unreachable");
    expect(second.precondition).toBe("failed");
  });

  it("if-match succeeds on the current etag and fails on a stale one", async () => {
    const pod = condPod();
    const store = new SolidDocStore({ container: CONTAINER, fetch: pod.fetchImpl });
    const name = keyToResourceName("a");
    await store.putDoc(name, '{"v":1}', DOC_CONTENT_TYPE, { ifNoneMatch: "*" });
    const got = await store.getDoc(name);
    const etag = got?.etag as string;
    // A matching if-match overwrites.
    const ok = await store.putDoc(name, '{"v":2}', DOC_CONTENT_TYPE, { ifMatch: etag });
    expect(ok.ok).toBe(true);
    // The OLD etag is now stale → 412.
    const stale = await store.putDoc(name, '{"v":3}', DOC_CONTENT_TYPE, { ifMatch: etag });
    expect(stale.ok).toBe(false);
    expect((await store.getDoc(name))?.body).toBe('{"v":2}');
  });
});

describe("the scope guard is enforced on every op (SSRF backstop)", () => {
  it("resourceUrl refuses a raw name that URL-normalises out of the container", async () => {
    const { store } = makeStore();
    // A sanitised document name always stays in-container; resourceUrl additionally
    // guards a RAW name that would traverse out via `..` path normalisation.
    expect(() => store.resourceUrl(keyToResourceName("a"))).not.toThrow();
    expect(() => store.resourceUrl("../escape")).toThrow(/escapes container path/);
  });

  it("urlToResourceName refuses a foreign-origin URL", async () => {
    const { store } = makeStore();
    expect(() => store.urlToResourceName("https://evil.example/app/items/x")).toThrow(
      /escapes container origin/,
    );
  });

  it("rejects a sibling-path URL on urlToResourceName", () => {
    const { store } = makeStore();
    expect(() => store.urlToResourceName("https://alice.pod/app/other/x")).toThrow(
      /escapes container path/,
    );
  });

  it("rejects a nested (non-direct-child) URL", () => {
    const { store } = makeStore();
    expect(() => store.urlToResourceName(`${CONTAINER}sub/x`)).toThrow(/direct child/);
  });
});

describe("redirect refusal on credentialed requests (SSRF / credential-exfil guard)", () => {
  // A credentialed request must NEVER be followed to another Location: the auth
  // header / DPoP proof could be replayed to a foreign origin. Every request is
  // issued with `redirect: "manual"` and any 3xx / opaqueredirect is refused.

  it("issues every request with redirect:'manual'", async () => {
    const seen: (RequestInit | undefined)[] = [];
    const fetchImpl: typeof globalThis.fetch = async (input, init) => {
      seen.push(init);
      const url = typeof input === "string" ? input : (input as URL | Request).toString();
      if (url === CONTAINER) {
        return new Response(
          `@prefix ldp: <http://www.w3.org/ns/ldp#> .\n<${CONTAINER}> a ldp:Container .`,
          { status: 200, headers: { "content-type": "text/turtle" } },
        );
      }
      return new Response("{}", { status: 200, headers: { "content-type": DOC_CONTENT_TYPE } });
    };
    const store = new SolidDocStore({ container: CONTAINER, fetch: fetchImpl });
    await store.getDoc(keyToResourceName("a"));
    await store.putDoc(keyToResourceName("a"), "{}", DOC_CONTENT_TYPE);
    await store.deleteDoc(keyToResourceName("a"));
    await store.listDocUrls();
    expect(seen).toHaveLength(4);
    expect(seen.every((i) => i?.redirect === "manual")).toBe(true);
  });

  it("getDoc refuses a 3xx redirect", async () => {
    const fetchImpl: typeof globalThis.fetch = async () =>
      new Response(null, { status: 302, headers: { location: "https://evil.example/x" } });
    const store = new SolidDocStore({ container: CONTAINER, fetch: fetchImpl });
    await expect(store.getDoc(keyToResourceName("a"))).rejects.toThrow(
      /refusing to follow a redirect/,
    );
  });

  it("putDoc refuses a 3xx redirect", async () => {
    const fetchImpl: typeof globalThis.fetch = async () =>
      new Response(null, { status: 301, headers: { location: "https://evil.example/x" } });
    const store = new SolidDocStore({ container: CONTAINER, fetch: fetchImpl });
    await expect(store.putDoc(keyToResourceName("a"), "{}", DOC_CONTENT_TYPE)).rejects.toThrow(
      /refusing to follow a redirect/,
    );
  });

  it("deleteDoc refuses a 3xx redirect", async () => {
    const fetchImpl: typeof globalThis.fetch = async () =>
      new Response(null, { status: 307, headers: { location: "https://evil.example/x" } });
    const store = new SolidDocStore({ container: CONTAINER, fetch: fetchImpl });
    await expect(store.deleteDoc(keyToResourceName("a"))).rejects.toThrow(
      /refusing to follow a redirect/,
    );
  });

  it("listDocUrls refuses a 3xx redirect on the container GET", async () => {
    const fetchImpl: typeof globalThis.fetch = async () =>
      new Response(null, { status: 308, headers: { location: "https://evil.example/" } });
    const store = new SolidDocStore({ container: CONTAINER, fetch: fetchImpl });
    await expect(store.listDocUrls()).rejects.toThrow(/refusing to follow a redirect/);
  });

  it("refuses a browser-style opaqueredirect response (status 0)", async () => {
    const opaque = {
      status: 0,
      type: "opaqueredirect",
      redirected: false,
      ok: false,
      headers: new Headers(),
    } as unknown as Response;
    const fetchImpl = (async () => opaque) as unknown as typeof globalThis.fetch;
    const store = new SolidDocStore({ container: CONTAINER, fetch: fetchImpl });
    await expect(store.getDoc(keyToResourceName("a"))).rejects.toThrow(
      /refusing to follow a redirect/,
    );
  });

  it("refuses a response whose `redirected` flag is set (defensive)", async () => {
    const redirected = {
      status: 200,
      type: "default",
      redirected: true,
      ok: true,
      headers: new Headers({ "content-type": DOC_CONTENT_TYPE }),
      text: async () => "{}",
    } as unknown as Response;
    const fetchImpl = (async () => redirected) as unknown as typeof globalThis.fetch;
    const store = new SolidDocStore({ container: CONTAINER, fetch: fetchImpl });
    await expect(store.getDoc(keyToResourceName("a"))).rejects.toThrow(
      /refusing to follow a redirect/,
    );
  });
});

describe("bounded response reads (oversized-body / memory-DoS guard)", () => {
  it("refuses a body larger than maxResponseBytes (streamed)", async () => {
    const big = "x".repeat(10_000);
    const fetchImpl: typeof globalThis.fetch = async () =>
      new Response(big, { status: 200, headers: { "content-type": DOC_CONTENT_TYPE } });
    const store = new SolidDocStore({
      container: CONTAINER,
      fetch: fetchImpl,
      maxResponseBytes: 64,
    });
    await expect(store.getDoc(keyToResourceName("a"))).rejects.toThrow(/exceeds the 64-byte limit/);
  });

  it("refuses up front on an advertised Content-Length over the cap", async () => {
    const fetchImpl: typeof globalThis.fetch = async () =>
      new Response("small", {
        status: 200,
        headers: { "content-type": DOC_CONTENT_TYPE, "content-length": "999999" },
      });
    const store = new SolidDocStore({
      container: CONTAINER,
      fetch: fetchImpl,
      maxResponseBytes: 8,
    });
    await expect(store.getDoc(keyToResourceName("a"))).rejects.toThrow(/exceeds the 8-byte limit/);
  });

  it("reads a body within the cap normally", async () => {
    const { pod, store } = makeStore();
    void pod;
    await store.putDoc(keyToResourceName("a"), '{"ok":true}', DOC_CONTENT_TYPE);
    const got = await store.getDoc(keyToResourceName("a"));
    expect(got?.body).toBe('{"ok":true}');
  });

  it("bounds the container listing read too", async () => {
    const big = `x`.repeat(5000);
    const fetchImpl: typeof globalThis.fetch = async () =>
      new Response(big, { status: 200, headers: { "content-type": "text/turtle" } });
    const store = new SolidDocStore({
      container: CONTAINER,
      fetch: fetchImpl,
      maxResponseBytes: 32,
    });
    await expect(store.listDocUrls()).rejects.toThrow(/exceeds the 32-byte limit/);
  });

  // A GET response whose body is an ASYNC-ITERABLE (Node Readable-style) rather
  // than a WHATWG ReadableStream — some auth-fetch shims return this shape. The
  // cap must still be enforced by iterating it, not crash on a missing getReader.
  function asyncIterableGet(
    chunks: (string | Uint8Array)[],
    opts?: { cancelSpy?: () => void; contentLength?: string },
  ): typeof globalThis.fetch {
    return (async () => {
      let cancelled = false;
      const body = {
        async *[Symbol.asyncIterator]() {
          for (const c of chunks) {
            if (cancelled) return;
            yield c;
          }
        },
        destroy() {
          cancelled = true;
          opts?.cancelSpy?.();
        },
      };
      const headers = new Headers({ "content-type": DOC_CONTENT_TYPE, etag: '"e1"' });
      if (opts?.contentLength) headers.set("content-length", opts.contentLength);
      return {
        status: 200,
        type: "default",
        redirected: false,
        ok: true,
        headers,
        body,
        text: async () => {
          throw new Error("text() must not be used when an async-iterable body is present");
        },
      } as unknown as Response;
    }) as unknown as typeof globalThis.fetch;
  }

  it("reads an async-iterable (Node Readable-style) body under the cap", async () => {
    const fetchImpl = asyncIterableGet(['{"a":', "1}"]);
    const store = new SolidDocStore({
      container: CONTAINER,
      fetch: fetchImpl,
      maxResponseBytes: 64,
    });
    const got = await store.getDoc(keyToResourceName("a"));
    expect(got?.body).toBe('{"a":1}');
    expect(got?.etag).toBe('"e1"');
  });

  it("bounds an async-iterable body over the cap (and cancels it)", async () => {
    const cancelSpy = vi.fn();
    const fetchImpl = asyncIterableGet(["xxxx", "yyyy", "zzzz"], { cancelSpy });
    const store = new SolidDocStore({
      container: CONTAINER,
      fetch: fetchImpl,
      maxResponseBytes: 6,
    });
    await expect(store.getDoc(keyToResourceName("a"))).rejects.toThrow(/exceeds the 6-byte limit/);
    expect(cancelSpy).toHaveBeenCalled();
  });

  it("cancels the body before throwing on an over-cap advertised Content-Length", async () => {
    const cancelSpy = vi.fn();
    const fetchImpl = asyncIterableGet(["ignored"], { cancelSpy, contentLength: "1000000" });
    const store = new SolidDocStore({
      container: CONTAINER,
      fetch: fetchImpl,
      maxResponseBytes: 8,
    });
    await expect(store.getDoc(keyToResourceName("a"))).rejects.toThrow(/exceeds the 8-byte limit/);
    expect(cancelSpy).toHaveBeenCalled();
  });
});

describe("maxResponseBytes validation (never silently disable the cap)", () => {
  it("resolveMaxResponseBytes maps every invalid value to the default", () => {
    for (const bad of [
      undefined,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      Number.NEGATIVE_INFINITY,
      0,
      -1,
      -1024,
      3.5,
      Number.MAX_SAFE_INTEGER + 1,
    ]) {
      expect(resolveMaxResponseBytes(bad as number)).toBe(DEFAULT_MAX_RESPONSE_BYTES);
    }
  });

  it("resolveMaxResponseBytes keeps a valid finite positive integer", () => {
    expect(resolveMaxResponseBytes(1)).toBe(1);
    expect(resolveMaxResponseBytes(4096)).toBe(4096);
  });

  it("a NaN cap falls back to the enforcing default (does NOT disable the limit)", async () => {
    // content-length 100 MB > the 64 MiB default → still refused, proving NaN
    // did NOT silently disable the cap (a NaN cap would make `> cap` always false).
    const fetchImpl: typeof globalThis.fetch = async () =>
      new Response("x", {
        status: 200,
        headers: { "content-type": DOC_CONTENT_TYPE, "content-length": "100000000" },
      });
    const store = new SolidDocStore({
      container: CONTAINER,
      fetch: fetchImpl,
      maxResponseBytes: Number.NaN,
    });
    await expect(store.getDoc(keyToResourceName("a"))).rejects.toThrow(
      /exceeds the \d+-byte limit/,
    );
  });

  it("an Infinity cap falls back to the enforcing default too", async () => {
    const fetchImpl: typeof globalThis.fetch = async () =>
      new Response("x", {
        status: 200,
        headers: { "content-type": DOC_CONTENT_TYPE, "content-length": "100000000" },
      });
    const store = new SolidDocStore({
      container: CONTAINER,
      fetch: fetchImpl,
      maxResponseBytes: Number.POSITIVE_INFINITY,
    });
    await expect(store.getDoc(keyToResourceName("a"))).rejects.toThrow(
      /exceeds the \d+-byte limit/,
    );
  });
});

describe("listDocUrls", () => {
  it("returns [] for an empty container", async () => {
    const { store } = makeStore();
    expect(await store.listDocUrls()).toEqual([]);
  });

  it("returns [] when the container 404s", async () => {
    const fetch404: typeof globalThis.fetch = async () => new Response(null, { status: 404 });
    const store = new SolidDocStore({ container: CONTAINER, fetch: fetch404 });
    expect(await store.listDocUrls()).toEqual([]);
  });

  it("lists only document resources — skips meta, sub-containers, and foreign names", async () => {
    const { pod, store } = makeStore();
    // Two real documents.
    await store.putDoc(keyToResourceName("a"), "{}", DOC_CONTENT_TYPE);
    await store.putDoc(keyToResourceName("b"), "{}", DOC_CONTENT_TYPE);
    // The metadata resource (must be filtered out).
    await store.putDoc(META_RESOURCE_NAME, "{}", DOC_CONTENT_TYPE);
    // A foreign (non-doc) resource and a sub-container injected directly.
    pod.store.set(`${CONTAINER}README`, {
      body: new Uint8Array(),
      contentType: "text/plain",
      etag: '"x"',
    });
    pod.store.set(`${CONTAINER}sub/`, {
      body: new Uint8Array(),
      contentType: "text/turtle",
      etag: '"y"',
    });
    const listed = await store.listDocUrls();
    expect(listed).toHaveLength(2);
    expect(listed.every((u) => u.includes("/doc."))).toBe(true);
    expect(listed.some((u) => u.endsWith(META_RESOURCE_NAME))).toBe(false);
    // Sorted + round-trips back to the keys.
    const keys = listed.map((u) => resourceNameToKey(store.urlToResourceName(u))).sort();
    expect(keys).toEqual(["a", "b"]);
  });
});
