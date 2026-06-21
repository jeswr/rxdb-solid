// AUTHORED-BY Claude Opus 4.8 (Fable unavailable) — re-review/upgrade candidate.
//
// Test-only fake pod (NOT exported from the package). A Map-backed `fetch` that
// behaves enough like an LDP/Solid server for the store + provider tests:
//   - PUT  with `If-None-Match: *` → 412 if the resource exists, else 201.
//   - PUT  without that header     → overwrite (205) / create (201).
//   - GET  a resource             → 200 with its stored bytes + content-type, else 404.
//   - GET  the container          → 200 Turtle listing of ldp:contains members.
//   - DELETE                       → 204, or 404 if absent.
// Bytes are stored verbatim (Uint8Array) so binary-update integrity is exercised
// end-to-end, not approximated.

interface Entry {
  body: Uint8Array;
  contentType: string;
  etag: string;
}

export interface FakePod {
  /** The raw backing store (url → entry) for assertions. */
  readonly store: Map<string, Entry>;
  /** The fake authenticated fetch to inject into the store/provider. */
  readonly fetchImpl: typeof globalThis.fetch;
  /** Count of requests by method, for asserting no-echo / write counts. */
  readonly calls: { getCount: number; putCount: number; deleteCount: number; otherCount: number };
}

/** Read a request body (the `RequestInit["body"]` union) into bytes. */
async function bodyToBytes(body: RequestInit["body"]): Promise<Uint8Array> {
  if (body == null) return new Uint8Array();
  if (body instanceof Uint8Array) return body;
  if (body instanceof ArrayBuffer) return new Uint8Array(body);
  if (ArrayBuffer.isView(body)) {
    return new Uint8Array(body.buffer, body.byteOffset, body.byteLength);
  }
  if (typeof body === "string") return new TextEncoder().encode(body);
  // Blob / ReadableStream / FormData — route through Response to normalise.
  const buf = await new Response(body).arrayBuffer();
  return new Uint8Array(buf);
}

/**
 * Build a fake pod whose container is `container`. The container is implicitly
 * present (GET on it always lists current members), as a real LDP container is.
 */
export function makePod(container: string): FakePod {
  const store = new Map<string, Entry>();
  const calls = { getCount: 0, putCount: 0, deleteCount: 0, otherCount: 0 };
  let etagSeq = 0;
  const nextEtag = () => `"etag-${++etagSeq}"`;

  const fetchImpl: typeof globalThis.fetch = async (input, init) => {
    const url = typeof input === "string" ? input : (input as URL | Request).toString();
    const method = (init?.method ?? "GET").toUpperCase();
    const headers = new Headers(init?.headers ?? {});

    if (method === "GET") {
      calls.getCount++;
      // Container listing.
      if (url === container) {
        const members = [...store.keys()].filter((u) => u !== container && u.startsWith(container));
        const contains = members.map((u) => `<${u}>`).join(", ");
        const body = `@prefix ldp: <http://www.w3.org/ns/ldp#> .
<${container}> a ldp:Container, ldp:BasicContainer${contains ? ` ;\n  ldp:contains ${contains}` : ""} .`;
        return new Response(body, {
          status: 200,
          headers: { "content-type": "text/turtle", etag: nextEtag() },
        });
      }
      const entry = store.get(url);
      if (!entry) return new Response(null, { status: 404 });
      // Response copies the bytes; serve a fresh view so the caller can't mutate ours.
      return new Response(new Uint8Array(entry.body), {
        status: 200,
        headers: { "content-type": entry.contentType, etag: entry.etag },
      });
    }

    if (method === "PUT") {
      calls.putCount++;
      const existing = store.get(url);
      if (headers.get("if-none-match") === "*" && existing) {
        return new Response(null, { status: 412 });
      }
      const bytes = await bodyToBytes(init?.body);
      const etag = nextEtag();
      store.set(url, {
        body: bytes,
        contentType: headers.get("content-type") ?? "application/octet-stream",
        etag,
      });
      return new Response(null, { status: existing ? 205 : 201, headers: { etag } });
    }

    if (method === "DELETE") {
      calls.deleteCount++;
      if (!store.has(url)) return new Response(null, { status: 404 });
      store.delete(url);
      return new Response(null, { status: 204 });
    }

    calls.otherCount++;
    return new Response(null, { status: 405 });
  };

  return { store, fetchImpl, calls };
}
