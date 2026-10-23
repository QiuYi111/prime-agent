#!/usr/bin/env node
// Guards the runtime contract downstream hosts import from a published build.
// The broker used to exist in src/ but not in dist/, which only surfaced as a
// MODULE_NOT_FOUND in the consuming deployment.
import assert from "node:assert/strict";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const dist = fileURLToPath(new URL("../dist", import.meta.url));

function files(dir) {
	const found = [];
	for (const name of readdirSync(dir)) {
		const path = join(dir, name);
		if (statSync(path).isDirectory()) found.push(...files(path));
		else if (name.endsWith(".js")) found.push(path);
	}
	return found;
}

const entry = join(dist, "index.js");
const exported = await import(pathToFileURL(entry).href);
assert.equal(
	typeof exported.startDelegatedProviderBroker,
	"function",
	"dist/index.js must export startDelegatedProviderBroker",
);

const built = files(dist);
const combined = built.map((path) => readFileSync(path, "utf8")).join("\n");
const bundled = built.filter((path) => path.includes("/bundle/")).map((path) => readFileSync(path, "utf8")).join("\n");

/** The bundler rewrites separators (10_000 -> 1e4), so compare the numeric value. */
function constant(text, name) {
	const match = new RegExp(`(?:const|var) ${name} = ([0-9_eE+.]+);`).exec(text);
	assert.ok(match, `built output must define ${name}`);
	return Number(match[1].replaceAll("_", ""));
}

for (const [name, value] of [
	["MAX_REQUESTS_PER_BROKER", 10_000],
	["MAX_OUTPUT_TOKENS_PER_REQUEST", 8_192],
	["MAX_TOTAL_OUTPUT_TOKENS", 1_000_000],
]) {
	assert.equal(constant(combined, name), value, `dist must define ${name} = ${value}`);
	assert.equal(constant(bundled, name), value, `dist/bundle must define ${name} = ${value}`);
}

assert.ok(
	!combined.includes("Broker accepts only serial requests"),
	"the broker must serve concurrent requests",
);

console.log(
	JSON.stringify({
		checkedFiles: built.length,
		exports: ["startDelegatedProviderBroker"],
		brokerLimits: { requests: 10000, perRequest: 8192, total: 1000000 },
		serialRequestGuard: "absent",
	}),
);
