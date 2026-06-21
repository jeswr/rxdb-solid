// AUTHORED-BY Claude Opus 4.8 (Fable unavailable) — re-review/upgrade candidate.
//
// Drift guard: `dist/` is COMMITTED (so the package is GitHub-installable under
// `ignore-scripts=true` with no build step), which means it can silently drift
// from `src/`. This check rebuilds into a temp dir and diffs the committed
// `dist/` against the fresh build — a mismatch fails the gate, forcing the
// committer to rebuild + commit `dist/` alongside any `src/` change (the suite
// rule for GitHub-installable packages).
import { execFileSync } from "node:child_process";
import { mkdtempSync, readdirSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";

const root = new URL("..", import.meta.url).pathname;
const committedDist = join(root, "dist");

function listFiles(dir) {
  const out = [];
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) out.push(...listFiles(full));
    else out.push(full);
  }
  return out;
}

// Compare only the meaningful build outputs: .js and .d.ts. Sourcemaps (.map)
// and the trailing `//# sourceMappingURL=` footer embed paths and are not
// load-bearing for the install-without-build guarantee, so they are excluded.
const isMeaningful = (f) => (f.endsWith(".js") || f.endsWith(".d.ts")) && !f.endsWith(".d.ts.map");
const stripFooter = (s) => s.replace(/\n\/\/# sourceMappingURL=.*\n?$/, "\n");

const tmp = mkdtempSync(join(tmpdir(), "ys-dist-"));
try {
  execFileSync("npx", ["tsc", "-p", "tsconfig.build.json", "--outDir", tmp], {
    cwd: root,
    stdio: "inherit",
  });

  const committed = listFiles(committedDist)
    .map((p) => relative(committedDist, p))
    .filter(isMeaningful)
    .sort();
  const fresh = listFiles(tmp)
    .map((p) => relative(tmp, p))
    .filter(isMeaningful)
    .sort();

  const missing = fresh.filter((f) => !committed.includes(f));
  const extra = committed.filter((f) => !fresh.includes(f));
  const errors = [];
  if (missing.length) errors.push(`dist/ is MISSING built files: ${missing.join(", ")}`);
  if (extra.length)
    errors.push(`dist/ has STALE files not produced by a fresh build: ${extra.join(", ")}`);

  for (const f of fresh) {
    if (!committed.includes(f)) continue;
    const a = stripFooter(readFileSync(join(committedDist, f), "utf8"));
    const b = stripFooter(readFileSync(join(tmp, f), "utf8"));
    if (a !== b) errors.push(`dist/${f} differs from a fresh build (rebuild + commit dist/).`);
  }

  if (errors.length) {
    console.error("check:dist FAILED — committed dist/ is out of sync with src/:");
    for (const e of errors) console.error(`  - ${e}`);
    console.error("Fix: `npm run build` then `git add dist/`.");
    process.exit(1);
  }
  console.log("check:dist OK — committed dist/ matches a fresh build.");
} finally {
  rmSync(tmp, { recursive: true, force: true });
}
