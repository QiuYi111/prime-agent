import { spawn } from "node:child_process";
import { PassThrough } from "node:stream";
import { describe, expect, it } from "vitest";
import { readSessionIssuerBootstrapFrame } from "../src/cli/session-issuer-bootstrap.js";
import { durableDaemonCreateCommand } from "../src/modes/daemon/daemon-worker-protocol.js";

const bootstrapModule = new URL("../src/cli/session-issuer-bootstrap.ts", import.meta.url).href;
const testIssuer = "A".repeat(43);

function runBootstrap(input: string | undefined): Promise<{ code: number | null; stdout: string; stderr: string }> {
	const script = `
		import { consumeSessionIssuerBootstrap, takeSessionIssuerCapability } from ${JSON.stringify(bootstrapModule)};
		try {
			await consumeSessionIssuerBootstrap();
			process.stdout.write(takeSessionIssuerCapability() ?? "missing");
		} catch (error) {
			process.stderr.write(error instanceof Error ? error.message : String(error));
			process.exitCode = 2;
		}
	`;
	const child = spawn(process.execPath, ["--import", "tsx", "--input-type=module", "--eval", script], {
		cwd: process.cwd(),
		env: { ...process.env, NODE_OPTIONS: "", PI_CAD_SESSION_ISSUER_FD: "3" },
		stdio: ["ignore", "pipe", "pipe", "pipe"],
	});
	let stdout = "";
	let stderr = "";
	child.stdout.setEncoding("utf8").on("data", (chunk) => (stdout += chunk));
	child.stderr.setEncoding("utf8").on("data", (chunk) => (stderr += chunk));
	child.stdio[3]!.on("error", () => {});
	if (input !== undefined) child.stdio[3]!.write(input);
	child.stdio[3]!.end();
	return new Promise((resolve, reject) => {
		const timeout = setTimeout(() => {
			child.kill("SIGKILL");
			reject(new Error("bootstrap test child timed out"));
		}, 5_000);
		child.once("error", reject);
		child.once("exit", (code) => {
			clearTimeout(timeout);
			resolve({ code, stdout, stderr });
		});
	});
}

describe("session issuer bootstrap", () => {
	it("consumes a valid one-line capability and closes the inherited descriptor", async () => {
		const result = await runBootstrap(`${testIssuer}\n`);
		expect(result.code).toBe(0);
		expect(result.stdout).toBe(testIssuer);
	});

	it("fails on EOF and malformed frames", async () => {
		const empty = await runBootstrap(undefined);
		expect(empty.code).toBe(2);
		expect(empty.stderr).toContain("ended before its frame");
		const malformed = await runBootstrap("not-a-capability\n");
		expect(malformed.code).toBe(2);
		expect(malformed.stderr).toContain("invalid frame length");
	});

	it("bounds an open pipe with no bootstrap data", async () => {
		const input = new PassThrough();
		const startedAt = Date.now();
		const pending = readSessionIssuerBootstrapFrame(input, 100);
		await expect(pending).rejects.toThrow("bootstrap timed out");
		expect(Date.now() - startedAt).toBeLessThan(1_000);
	});

	it("does not persist the one-shot capability in worker descriptors", () => {
		const durable = durableDaemonCreateCommand({
			type: "create",
			sessionIssuerCapability: testIssuer,
			config: { cwd: "/tmp", agentDir: "/tmp" },
		});
		expect(durable).toEqual({ type: "create" });
		expect(JSON.stringify(durable)).not.toContain(testIssuer);
	});
});
