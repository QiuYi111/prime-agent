import { mkdtemp, mkdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const reifyRoot = process.env.PI_CAD_TEST_REIFY_ROOT;
if (!reifyRoot) throw new Error("PI_CAD_TEST_REIFY_ROOT must point to the isolated Reify checkout");
process.env.PI_CAD_CANONICAL_PROJECT_DIR = await mkdtemp(join(tmpdir(), "pi-cad-canonical-"));
const root = await mkdtemp(join(tmpdir(), "reify-prime-e2e-"));
const project = join(root, "project"), runtime = join(root, "runtime"), agentDir = join(root, "prime-agent");
const sessionsDir = join(root, "sessions");
await Promise.all([mkdir(project), mkdir(runtime), mkdir(agentDir), mkdir(sessionsDir)]);
const { startAuthoritySidecar } = await import(join(reifyRoot, "src/authority/sidecar.ts"));
const sidecar = await startAuthoritySidecar({ cwd: project, runtimeDirectory: runtime });
const loader = fileURLToPath(new URL("../../../../node_modules/tsx/dist/loader.mjs", import.meta.url));
const childScript = fileURLToPath(new URL("./reify-prime-authority-child.ts", import.meta.url));

async function runPrime(mode: "create" | "resume", extra: Record<string, string> = {}): Promise<Record<string, unknown>> {
	const env: NodeJS.ProcessEnv = {
		PATH: process.env.PATH,
		HOME: process.env.HOME,
		TMPDIR: process.env.TMPDIR ?? tmpdir(),
		PRIME_AGENT_KERNEL_PYTHON: process.env.PRIME_AGENT_KERNEL_PYTHON ?? join(process.env.HOME ?? "/home/jingyi", ".prime/agent/kernel-venv/bin/python"),
		PI_CAD_REIFY_ROOT: reifyRoot,
		PI_CAD_E2E_MODE: mode,
		PI_CAD_E2E_PROJECT: project,
		PI_CAD_E2E_AGENT_DIR: agentDir,
		PI_CAD_E2E_SESSIONS_DIR: sessionsDir,
		PI_CAD_AUTHOR_SOCKET: sidecar.authorSocket,
		PI_CAD_AUTHOR_CAPABILITY_FILE: sidecar.authorCapabilityFile,
		PI_CAD_SESSION_ISSUER_FD: "3",
		PI_CAD_PROJECT_CWD: project,
		PI_CAD_REPO: reifyRoot,
		PYTHONPATH: resolve(reifyRoot, "skills/cad/src"),
		...extra,
	};
	const child = spawn(process.execPath, ["--import", loader, childScript], {
		cwd: process.cwd(),
		env,
		stdio: ["ignore", "pipe", "pipe", "pipe"],
	});
	const stdout: Buffer[] = [], stderr: Buffer[] = [];
	let size = 0;
	for (const stream of [child.stdout, child.stderr]) {
		stream.on("data", (chunk: Buffer) => {
			size += chunk.length;
			if (size > 256 * 1024) child.kill("SIGKILL");
			else (stream === child.stdout ? stdout : stderr).push(chunk);
			if (stream === child.stderr) process.stderr.write(chunk);
		});
	}
	child.stdio[3].on("error", () => undefined);
	child.stdio[3].end(sidecar.sessionIssuerCapability);
	const code = await new Promise<number | null>((accept, reject) => {
		child.once("error", reject);
		child.once("close", accept);
	});
	if (code !== 0) throw new Error(`Prime E2E child (${mode}) exited ${code}: ${Buffer.concat(stderr).toString("utf8").slice(-6000)}`);
	const output = Buffer.concat(stdout).toString("utf8").trim().split("\n").at(-1);
	if (!output) throw new Error(`Prime E2E child (${mode}) returned no result`);
	return JSON.parse(output) as Record<string, unknown>;
}

try {
	const created = await runPrime("create");
	const resumed = await runPrime("resume", {
		PI_CAD_E2E_RESUME_FILE: String(created.fileA),
		PI_CAD_E2E_RUN_A: String(created.runA),
		PI_CAD_E2E_RUN_B: String(created.runB),
	});
	console.log(JSON.stringify({ ...created, ...resumed }));
} finally {
	await sidecar.close();
	await rm(root, { recursive: true, force: true });
	await rm(process.env.PI_CAD_CANONICAL_PROJECT_DIR!, { recursive: true, force: true });
}
