// The READMEs' ```ts check blocks as modules that `pnpm typecheck` compiles (examples/acme's tsconfig
// includes tests/). Each block is a module of its own; names it uses from prelude.ts without declaring
// them are imported for it. Regenerate with `node examples/acme/tests/docs/snippets.ts`;
// tests/unit/readme.test.ts fails when the generated files are stale.
import { readdirSync, readFileSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import path from "node:path";

const root = path.resolve(import.meta.dirname, "../../../..");
export const generatedDir = path.join(import.meta.dirname, "generated");
const readmes = [
	"README.md",
	"packages/fabrial/README.md",
	"packages/slack/README.md",
	"packages/github/README.md",
	"packages/linear/README.md",
	"examples/acme/README.md",
];

const prelude = readFileSync(path.join(import.meta.dirname, "prelude.ts"), "utf8");
const exported = new Set([
	...[...prelude.matchAll(/^export (?:const|function|class) (\w+)/gm)].map((m) => m[1]!),
	...[...prelude.matchAll(/^export \{([^}]+)\}/gm)].flatMap((m) =>
		m[1]!.split(",").map((name) => name.trim()),
	),
]);

/** Top-level declarations and imports of a snippet. */
function declared(code: string): Set<string> {
	const names = new Set<string>();
	for (const m of code.matchAll(
		/^(?:export )?(?:async )?(?:const|let|function|class|interface|type) (\w+)/gm,
	))
		names.add(m[1]!);
	for (const m of code.matchAll(/^import (?:type )?\{([^}]+)\}/gm))
		for (const name of m[1]!.split(","))
			names.add(
				name
					.trim()
					.split(/\s+as\s+/)
					.pop()!,
			);
	return names;
}

/** File name → content of every generated snippet module. */
export function generate(): Map<string, string> {
	const files = new Map<string, string>();
	for (const readme of readmes) {
		const lines = readFileSync(path.join(root, readme), "utf8").split("\n");
		const slug = readme.replace(/\/?README\.md$/, "").replaceAll("/", "-") || "root";
		for (let i = 0; i < lines.length; i++) {
			if (!/^```ts check\s*$/.test(lines[i]!)) continue;
			const end = lines.findIndex((line, j) => j > i && line.startsWith("```"));
			if (end < 0) throw new Error(`${readme}:${i + 1}: unclosed block`);
			const code = lines.slice(i + 1, end).join("\n");
			const own = declared(code);
			const used = new Set(code.match(/\b[A-Za-z_]\w*\b/g));
			const needed = [...exported].filter((name) => used.has(name) && !own.has(name));
			files.set(
				`${slug}-${i + 1}.ts`,
				[
					`// Generated from ${readme}:${i + 2} by ../snippets.ts. Do not edit.`,
					"/* oxlint-disable */",
					...(needed.length ? [`import { ${needed.join(", ")} } from "../prelude.ts";`] : []),
					code,
					"export {};",
					"",
				].join("\n"),
			);
			i = end;
		}
	}
	return files;
}

export function written(): Map<string, string> {
	let names: string[] = [];
	try {
		names = readdirSync(generatedDir);
	} catch {}
	return new Map(names.map((name) => [name, readFileSync(path.join(generatedDir, name), "utf8")]));
}

if (import.meta.filename === path.resolve(process.argv[1] ?? "")) {
	rmSync(generatedDir, { recursive: true, force: true });
	mkdirSync(generatedDir);
	for (const [name, content] of generate()) writeFileSync(path.join(generatedDir, name), content);
}
