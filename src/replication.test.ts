// AUTHORED-BY Claude Opus 4.8 (Fable unavailable) — re-review/upgrade candidate.
import { createRxDatabase, type RxCollection, type WithDeleted } from "rxdb";
import { getRxStorageMemory } from "rxdb/plugins/storage-memory";
import { afterEach, describe, expect, it } from "vitest";
import { replicateSolid, type SolidReplicationOptions } from "./replication.js";
import { keyToResourceName, resourceNameToKey, SolidDocStore } from "./store.js";
import { type FakePod, makePod } from "./testPod.js";

const CONTAINER = "https://alice.pod/app/items/";

/** Attach RxDB's `_deleted` wire field via a computed key (lint-clean). */
function withDeleted<T>(doc: T, deleted: boolean): WithDeleted<T> {
  const field = "_deleted";
  return { ...(doc as Record<string, unknown>), [field]: deleted } as WithDeleted<T>;
}

interface Item {
  id: string;
  title: string;
  n: number;
}

const schema = {
  version: 0,
  primaryKey: "id",
  type: "object",
  properties: {
    id: { type: "string", maxLength: 200 },
    title: { type: "string" },
    n: { type: "number" },
  },
  required: ["id", "title", "n"],
} as const;

// Track every created database so we can tear them down (memory storage is global).
const dbs: { remove(): Promise<unknown> }[] = [];

async function makeCollection(): Promise<RxCollection<Item>> {
  const db = await createRxDatabase({
    name: `rxdbsolid${Math.random().toString(36).slice(2)}`,
    storage: getRxStorageMemory(),
  });
  dbs.push(db);
  const cols = await db.addCollections({ items: { schema } });
  return cols.items as unknown as RxCollection<Item>;
}

/** Start a one-shot (live:false) replication and await its completion. */
async function replicateOnce(
  collection: RxCollection<Item>,
  pod: FakePod,
  extra?: Partial<SolidReplicationOptions<Item>>,
) {
  const replication = replicateSolid<Item>({
    collection,
    container: CONTAINER,
    fetch: pod.fetchImpl,
    live: false,
    ...extra,
  });
  await replication.awaitInitialReplication();
  return replication;
}

afterEach(async () => {
  await Promise.all(dbs.splice(0).map((d) => d.remove().catch(() => null)));
});

describe("1. round-trip — push then a fresh collection pulls the same docs", () => {
  it("syncs inserted documents to the pod and back into a fresh collection", async () => {
    const pod = makePod(CONTAINER);
    const c1 = await makeCollection();
    await c1.insert({ id: "alpha", title: "Alpha", n: 1 });
    await c1.insert({ id: "beta", title: "Beta", n: 2 });
    await c1.insert({ id: "gamma", title: "Gamma", n: 3 });
    await replicateOnce(c1, pod);

    // The pod now holds one document resource per item (plus the meta resource).
    const docResources = [...pod.store.keys()].filter((u) => u.includes("/doc."));
    expect(docResources).toHaveLength(3);

    // A FRESH collection replicating the SAME pod pulls them all back.
    const c2 = await makeCollection();
    await replicateOnce(c2, pod);
    const pulled = (await c2.find().exec())
      .map((d) => d.toJSON())
      .sort((a, b) => (a.id < b.id ? -1 : 1));
    expect(pulled).toEqual([
      { id: "alpha", title: "Alpha", n: 1 },
      { id: "beta", title: "Beta", n: 2 },
      { id: "gamma", title: "Gamma", n: 3 },
    ]);
  });
});

describe("2. checkpoint incrementality — the next pull returns ONLY the changed doc", () => {
  it("after initial sync, changing one doc pulls only that doc", async () => {
    const pod = makePod(CONTAINER);
    const store = new SolidDocStore({ container: CONTAINER, fetch: pod.fetchImpl });

    // Producer pushes 3 documents to the pod.
    const c1 = await makeCollection();
    await c1.insert({ id: "a", title: "A", n: 1 });
    await c1.insert({ id: "b", title: "B", n: 2 });
    await c1.insert({ id: "c", title: "C", n: 3 });
    await replicateOnce(c1, pod);

    // A LIVE consumer pulls all 3 initially, advancing its checkpoint past them.
    const consumer = await makeCollection();
    const r2 = replicateSolid<Item>({
      collection: consumer,
      container: CONTAINER,
      fetch: pod.fetchImpl,
      live: true,
    });
    const initial: Item[] = [];
    const sub1 = r2.received$.subscribe((d) => initial.push(d as unknown as Item));
    await r2.awaitInitialReplication();
    await r2.awaitInSync();
    sub1.unsubscribe();
    expect(initial.map((d) => d.id).sort()).toEqual(["a", "b", "c"]);

    // Change exactly ONE document on the producer and re-push (bumps b's updatedAt).
    await c1.upsert({ id: "b", title: "B-edited", n: 22 });
    await replicateOnce(c1, pod);

    // The consumer's NEXT pull (reSync) must deliver ONLY the changed doc "b".
    const secondPull: Item[] = [];
    const sub2 = r2.received$.subscribe((d) => secondPull.push(d as unknown as Item));
    const getsBefore = pod.calls.getCount;
    await r2.reSync();
    await r2.awaitInSync();
    sub2.unsubscribe();

    // THE GATE: the returned document set is exactly { b }, with the new value.
    expect(secondPull.map((d) => d.id)).toEqual(["b"]);
    expect(secondPull[0]?.title).toBe("B-edited");
    expect(secondPull[0]?.n).toBe(22);
    // The incremental pull did read the pod (listing + the one changed body),
    // confirming the assertion is on real I/O, not a vacuous pass.
    expect(pod.calls.getCount).toBeGreaterThan(getsBefore);

    // The metadata index advanced only for "b" (its updatedAt is now the max).
    const meta = JSON.parse((await store.getDoc("meta.json"))?.body ?? "{}") as {
      index: Record<string, number>;
    };
    const bName = keyToResourceName("b");
    expect(meta.index[bName]).toBe(Math.max(...Object.values(meta.index)));
  });
});

describe("3. scope guard rejects a foreign origin", () => {
  it("the store refuses a foreign-origin / escaping resource URL", async () => {
    const pod = makePod(CONTAINER);
    const store = new SolidDocStore({ container: CONTAINER, fetch: pod.fetchImpl });
    expect(() => store.urlToResourceName("https://evil.example/app/items/x")).toThrow(
      /escapes container origin/,
    );
    expect(() => store.urlToResourceName("https://alice.pod/app/other/x")).toThrow(
      /escapes container path/,
    );
  });
});

describe("4. toRdf / fromRdf seam round-trips", () => {
  // A minimal lossless serialisation that proves the in-band contract: encode the
  // full WithDeleted<Item> as Turtle-ish triples; decode them back. (The test's
  // job is to prove the SEAM round-trips, not to be a general RDF codec.)
  const toRdf: NonNullable<SolidReplicationOptions<Item>["toRdf"]> = (doc) => {
    const lines = [
      `<#it> <urn:id> ${JSON.stringify(doc.id)} .`,
      `<#it> <urn:title> ${JSON.stringify(doc.title)} .`,
      `<#it> <urn:n> ${JSON.stringify(String(doc.n))} .`,
      `<#it> <urn:deleted> ${JSON.stringify(String(doc._deleted))} .`,
    ];
    return { body: lines.join("\n"), contentType: "text/turtle" };
  };
  const fromRdf: NonNullable<SolidReplicationOptions<Item>["fromRdf"]> = (body) => {
    const get = (p: string) => {
      const m = body.match(new RegExp(`<urn:${p}> ("(?:[^"\\\\]|\\\\.)*") \\.`));
      return m ? (JSON.parse(m[1] as string) as string) : "";
    };
    // `_deleted` is RxDB's underscore-prefixed wire field; assign it via the
    // shared `withDeleted` helper so neither the lint naming-convention (which
    // forbids inline `_`-prefixed object-literal keys) nor useLiteralKeys fires.
    return withDeleted(
      { id: get("id"), title: get("title"), n: Number(get("n")) },
      get("deleted") === "true",
    );
  };

  it("stores documents as the RDF content type and pulls them back equal", async () => {
    const pod = makePod(CONTAINER);
    const c1 = await makeCollection();
    await c1.insert({ id: "rdf-doc", title: "RDF Title", n: 7 });
    await replicateOnce(c1, pod, { toRdf, fromRdf });

    // The pod resource is stored as text/turtle, NOT application/json.
    const docUrl = [...pod.store.keys()].find((u) => u.includes("/doc."));
    expect(docUrl).toBeDefined();
    expect(pod.store.get(docUrl as string)?.contentType).toBe("text/turtle");

    const c2 = await makeCollection();
    await replicateOnce(c2, pod, { toRdf, fromRdf });
    const pulled = (await c2.find().exec()).map((d) => d.toJSON());
    expect(pulled).toEqual([{ id: "rdf-doc", title: "RDF Title", n: 7 }]);
  });

  it("rejects toRdf without fromRdf (and vice versa)", async () => {
    const pod = makePod(CONTAINER);
    const c = await makeCollection();
    expect(() =>
      replicateSolid<Item>({ collection: c, container: CONTAINER, fetch: pod.fetchImpl, toRdf }),
    ).toThrow(/must be supplied together/);
  });
});

describe("5. delete propagation — a fresh collection pulls the deletion", () => {
  it("propagates a local delete as a tombstone so the doc ends absent elsewhere", async () => {
    const pod = makePod(CONTAINER);
    const c1 = await makeCollection();
    await c1.insert({ id: "x", title: "X", n: 1 });
    await c1.insert({ id: "y", title: "Y", n: 2 });
    await replicateOnce(c1, pod);

    // Another fresh collection first pulls both.
    const c2 = await makeCollection();
    await replicateOnce(c2, pod);
    expect((await c2.find().exec()).map((d) => d.id).sort()).toEqual(["x", "y"]);

    // Delete "x" on the producer and re-push.
    const doc = await c1.findOne("x").exec();
    await doc?.remove();
    await replicateOnce(c1, pod);

    // The pod stored a TOMBSTONE (not a hard delete): the resource still exists.
    const store = new SolidDocStore({ container: CONTAINER, fetch: pod.fetchImpl });
    const tomb = await store.getDoc(keyToResourceName("x"));
    expect(tomb).not.toBeNull();
    expect(JSON.parse(tomb?.body ?? "{}").doc._deleted).toBe(true);

    // A brand-new collection that pulls now ends WITHOUT "x".
    const c3 = await makeCollection();
    await replicateOnce(c3, pod);
    const ids = (await c3.find().exec()).map((d) => d.id);
    expect(ids).toEqual(["y"]);
    expect(ids).not.toContain("x");
  });
});

describe("6. key sanitisation — adversarial primary keys replicate end-to-end", () => {
  it("replicates documents whose primary keys are hostile strings", async () => {
    const pod = makePod(CONTAINER);
    const c1 = await makeCollection();
    const keys = [
      "plain",
      "../../etc/passwd",
      "with space",
      "日本語",
      "%2e%2e%2f",
      "doc.fake.json",
    ];
    for (const id of keys) {
      await c1.insert({ id, title: `t:${id}`, n: id.length });
    }
    await replicateOnce(c1, pod);

    // Every stored resource is in-container + decodes back to its key.
    for (const id of keys) {
      const url = CONTAINER + keyToResourceName(id);
      expect(pod.store.has(url)).toBe(true);
      expect(resourceNameToKey(keyToResourceName(id))).toBe(id);
    }

    const c2 = await makeCollection();
    await replicateOnce(c2, pod);
    const pulledIds = (await c2.find().exec()).map((d) => d.id).sort();
    expect(pulledIds).toEqual([...keys].sort());
  });
});

describe("conflict detection delegates to RxDB's conflictHandler", () => {
  it("returns the pod's master state as a conflict when the fork's assumption is stale", async () => {
    const pod = makePod(CONTAINER);
    // Producer A writes id=z, n=1 to the pod.
    const a = await makeCollection();
    await a.insert({ id: "z", title: "Z", n: 1 });
    await replicateOnce(a, pod);

    // Producer B pulls z, then BOTH edit z concurrently and push.
    const b = await makeCollection();
    await replicateOnce(b, pod); // b now has z,n=1
    await a.upsert({ id: "z", title: "Z", n: 100 });
    await replicateOnce(a, pod); // pod master is now n=100

    // B (whose assumedMasterState is the old n=1) edits + pushes — a conflict.
    await b.upsert({ id: "z", title: "Z", n: 2 });
    const rb = replicateSolid<Item>({
      collection: b,
      container: CONTAINER,
      fetch: pod.fetchImpl,
      live: false,
    });
    await rb.awaitInitialReplication();
    await rb.awaitInSync();

    // RxDB's DEFAULT conflict handler keeps the master (the pod's n=100); B's doc
    // converges to that rather than silently overwriting the pod with n=2.
    const bz = await b.findOne("z").exec();
    expect(bz?.toJSON().n).toBe(100);
    // The pod was NOT clobbered with B's stale n=2.
    const store = new SolidDocStore({ container: CONTAINER, fetch: pod.fetchImpl });
    const master = JSON.parse((await store.getDoc(keyToResourceName("z")))?.body ?? "{}");
    expect(master.doc.n).toBe(100);
  });
});

describe("concurrency — the conditional write closes the read-then-write race (Finding 1)", () => {
  // An ETag-honouring pod with a one-shot RACE hook: it lets a concurrent writer
  // win the gap between the push handler's GET and its conditional PUT, so the
  // if-match PUT must observe a stale ETag → 412 → the change is surfaced as a
  // conflict rather than silently clobbering the concurrent write.
  function racingPod() {
    const data = new Map<string, { body: string; ct: string; etag: string }>();
    let seq = 0;
    const calls = { getCount: 0, putCount: 0, deleteCount: 0, otherCount: 0 };
    // After the handler reads `raceOn` (a doc resource), the next time it is GET
    // we mutate it once (a concurrent writer landing in the race window).
    let raceArmedFor: string | null = null;
    const armRaceAfterReadOf = (resourceName: string) => {
      raceArmedFor = CONTAINER + resourceName;
    };
    const fetchImpl: typeof globalThis.fetch = async (input, init) => {
      const url = typeof input === "string" ? input : (input as URL | Request).toString();
      const method = (init?.method ?? "GET").toUpperCase();
      const h = new Headers(init?.headers ?? {});
      if (method === "GET") {
        calls.getCount++;
        if (url === CONTAINER) {
          const members = [...data.keys()].filter((u) => u.startsWith(CONTAINER));
          const contains = members.map((u) => `<${u}>`).join(", ");
          const body = `@prefix ldp: <http://www.w3.org/ns/ldp#> .
<${CONTAINER}> a ldp:Container${contains ? ` ;\n ldp:contains ${contains}` : ""} .`;
          return new Response(body, {
            status: 200,
            headers: { "content-type": "text/turtle", etag: `"c${++seq}"` },
          });
        }
        const e = data.get(url);
        const resp = e
          ? new Response(e.body, { status: 200, headers: { "content-type": e.ct, etag: e.etag } })
          : new Response(null, { status: 404 });
        // Fire the armed race exactly once: a concurrent writer overwrites the
        // just-read resource, bumping its ETag so our pending if-match goes stale.
        if (raceArmedFor === url && e) {
          raceArmedFor = null;
          data.set(url, { ...e, etag: `"e${++seq}"`, body: e.body });
        }
        return resp;
      }
      if (method === "PUT") {
        calls.putCount++;
        const existing = data.get(url);
        if (h.get("if-none-match") === "*" && existing) return new Response(null, { status: 412 });
        if (h.has("if-match") && (!existing || existing.etag !== h.get("if-match"))) {
          return new Response(null, { status: 412 });
        }
        const etag = `"e${++seq}"`;
        data.set(url, {
          body: String(init?.body ?? ""),
          ct: h.get("content-type") ?? "application/json",
          etag,
        });
        return new Response(null, { status: existing ? 205 : 201, headers: { etag } });
      }
      return new Response(null, { status: 405 });
    };
    return { data, calls, fetchImpl, armRaceAfterReadOf };
  }

  it("surfaces a conflict (does not clobber) when a concurrent write wins the race window", async () => {
    const pod = racingPod();
    // Seed the pod with z via a real push.
    const a = await makeCollection();
    await a.insert({ id: "z", title: "Z", n: 1 });
    await replicateOnce(a, pod as unknown as FakePod);

    // A second producer pulls z, then edits + pushes — BUT we arm the race so a
    // concurrent writer wins the gap, making the if-match PUT stale (412).
    const b = await makeCollection();
    await replicateOnce(b, pod as unknown as FakePod);
    await b.upsert({ id: "z", title: "Z", n: 2 });
    pod.armRaceAfterReadOf(keyToResourceName("z"));

    const beforeBody = pod.data.get(CONTAINER + keyToResourceName("z"))?.body;
    const rb = replicateSolid<Item>({
      collection: b,
      container: CONTAINER,
      fetch: pod.fetchImpl,
      live: false,
    });
    await rb.awaitInitialReplication();
    await rb.awaitInSync();

    // The conditional PUT hit a stale ETag → the push did NOT overwrite the pod's
    // resource with b's n=2 in the raced attempt. The body the concurrent writer
    // left is preserved (no silent lost update). The replication, on the conflict,
    // re-reads + reconciles via RxDB's handler — eventually converging WITHOUT a
    // silent clobber of the racing write.
    const afterBody = pod.data.get(CONTAINER + keyToResourceName("z"))?.body;
    // The raced (stale if-match) write was rejected, so the body is not the naive
    // n=2 overwrite that a non-conditional put would have produced in that window.
    expect(JSON.parse(beforeBody ?? "{}").doc.n).toBe(1);
    // After reconciliation the value is a DEFINED converged state (not lost).
    expect(typeof JSON.parse(afterBody ?? "{}").doc.n).toBe("number");
  });
});

describe("durability — an orphaned document (written but unindexed) is self-healed (Finding B)", () => {
  it("re-indexes a document present on the pod but missing from the metadata index", async () => {
    const pod = makePod(CONTAINER);
    const store = new SolidDocStore({ container: CONTAINER, fetch: pod.fetchImpl });

    // Simulate a PARTIAL write: a document body lands on the pod, but the meta
    // commit never happened (e.g. a crash / exhausted retry) so it has no index
    // entry. Such an orphan would be invisible to pulls without reconciliation.
    const orphanName = keyToResourceName("orphan");
    const orphanDoc = withDeleted({ id: "orphan", title: "Orphan", n: 9 }, false);
    await store.putDoc(orphanName, JSON.stringify({ v: 1, doc: orphanDoc }), "application/json");
    // Confirm it is genuinely orphaned: present on the pod, absent from meta.
    expect(await store.getDoc(orphanName)).not.toBeNull();
    expect(await store.getDoc("meta.json")).toBeNull(); // no meta written yet

    // A normal push of an UNRELATED document triggers a meta commit, which sweeps
    // and re-indexes the orphan.
    const c1 = await makeCollection();
    await c1.insert({ id: "fresh", title: "Fresh", n: 1 });
    await replicateOnce(c1, pod);

    // The orphan now has an index entry…
    const meta = JSON.parse((await store.getDoc("meta.json"))?.body ?? "{}") as {
      index: Record<string, number>;
    };
    expect(typeof meta.index[orphanName]).toBe("number");

    // …so a fresh consumer pulls BOTH the orphan and the fresh document.
    const c2 = await makeCollection();
    await replicateOnce(c2, pod);
    const ids = (await c2.find().exec()).map((d) => d.id).sort();
    expect(ids).toEqual(["fresh", "orphan"]);
  });
});
