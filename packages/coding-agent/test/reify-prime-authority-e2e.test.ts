import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

describe("Reify Prime session authority end-to-end", () => {
	it.skipIf(process.platform !== "linux" || !process.env.PI_CAD_TEST_REIFY_ROOT)(
		"uses the real Reify extension, Prime AgentSessions, Python cad.client, and authority sidecar",
		async () => {
			const loader = fileURLToPath(new URL("../../../node_modules/tsx/dist/loader.mjs", import.meta.url));
			const orchestrator = fileURLToPath(new URL("./fixtures/reify-prime-authority-orchestrator.ts", import.meta.url));
			const child = spawn(process.execPath, ["--import", loader, orchestrator], {
				cwd: process.cwd(),
				env: {
					PATH: process.env.PATH,
					HOME: process.env.HOME,
					TMPDIR: process.env.TMPDIR ?? tmpdir(),
					PI_CAD_TEST_REIFY_ROOT: process.env.PI_CAD_TEST_REIFY_ROOT,
				},
				stdio: ["ignore", "pipe", "pipe"],
			});
			const stdout: Buffer[] = [], stderr: Buffer[] = [];
			child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
			child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
			const code = await new Promise<number | null>((accept, reject) => {
				child.once("error", reject);
				child.once("close", accept);
			});
			expect(code, Buffer.concat(stderr).toString("utf8").slice(-6000)).toBe(0);
			const output = Buffer.concat(stdout).toString("utf8").trim().split("\n").at(-1);
			expect(output).toBeTruthy();
			const evidence = JSON.parse(output!) as Record<string, unknown>;
			expect(evidence.idA).not.toBe(evidence.idB);
			expect(evidence.runA).not.toBe(evidence.runB);
			expect(evidence.forgedFromA).toBe(evidence.runA);
			expect(evidence.currentB).toBe(evidence.runB);
			expect(evidence.resumedRun).toBe(evidence.runA);
			expect(evidence.childBefore).toBeNull();
			expect(evidence.childRun).not.toBe(evidence.runA);
			expect(evidence.childRun).not.toBe(evidence.runB);
		},
		180_000,
	);
});
