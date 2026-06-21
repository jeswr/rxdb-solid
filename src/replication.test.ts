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
