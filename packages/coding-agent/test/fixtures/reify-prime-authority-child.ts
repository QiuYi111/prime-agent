import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { createAgentSessionFromServices, createAgentSessionServices } from "../../src/core/agent-session-services.js";
import { createEventBus } from "../../src/core/event-bus.js";
import { createExtensionRuntime, loadExtensionFromFactory } from "../../src/core/extensions/loader.js";
import { createSyntheticSourceInfo } from "../../src/core/source-info.js";
import { SessionManager } from "../../src/core/session-manager.js";
import { SettingsManager } from "../../src/core/settings-manager.js";
import { IpythonKernelProvisioner } from "../../src/core/tools/ipython.js";

type LiveSession = {
	manager: SessionManager;
	session: { dispose(): void; _createKernelHostHandlers(): Record<string, (payload: Record<string, unknown>) => Promise<Record<string, unknown>>> };
	provisioner: IpythonKernelProvisioner;
};

const root = process.env.PI_CAD_E2E_AGENT_DIR;
const mode = process.env.PI_CAD_E2E_MODE;
if (!root || (mode !== "create" && mode !== "resume")) throw new Error("missing Pi-CAD E2E configuration");

const cwd = process.env.PI_CAD_E2E_PROJECT!;
const agentDir = process.env.PI_CAD_E2E_AGENT_DIR!;
const sessionsDir = process.env.PI_CAD_E2E_SESSIONS_DIR!;
const reifyRepo = process.env.PI_CAD_REIFY_ROOT!;
const live: LiveSession[] = [];

async function makeSession(manager: SessionManager): Promise<LiveSession> {
	const services = await createAgentSessionServices({
		cwd,
		agentDir,
		settingsManager: SettingsManager.create(cwd, agentDir),
		noBuiltinHerdrReporter: true,
		telemetryDisabled: true,
		resourceLoaderOptions: { noExtensions: true, noPromptTemplates: true, noThemes: true },
	});
	const runtime = createExtensionRuntime();
	const extensionModule = await import(pathToFileURL(join(reifyRepo, "src/integrations/prime/extension.ts")).href);
	const extension = await loadExtensionFromFactory(
		(pi) => extensionModule.default(pi),
		cwd,
		createEventBus(),
		runtime,
		"pi-cad-real-extension",
	);
	const loader = Object.create(services.resourceLoader) as typeof services.resourceLoader;
	loader.getExtensions = () => ({ extensions: [extension], errors: [], runtime });
	services.resourceLoader = loader;
	const { session } = await createAgentSessionFromServices({ services, sessionManager: manager });
	const provisioner = new IpythonKernelProvisioner(cwd, {
		hostHandlers: (session as unknown as LiveSession["session"])._createKernelHostHandlers(),
		sessionId: manager.getSessionId(),
	});
	const result = { manager, session: session as unknown as LiveSession["session"], provisioner };
	live.push(result);
	return result;
}

function persistBinding(manager: SessionManager, run: Record<string, unknown>): void {
	const sessionId = manager.getSessionId();
	if (typeof run.runId !== "string" || typeof run.workflowHash !== "string") throw new Error("workflow start omitted its binding identity");
	manager.appendCustomEntry("pi-cad.workflow-binding", {
		schema: 1,
		sessionId,
		runId: run.runId,
		workflowHash: run.workflowHash,
		boundAt: new Date().toISOString(),
	});
	manager.flushNow();
}

async function pythonRequest(session: LiveSession, source: string): Promise<any> {
	const kernel = await session.provisioner.ensure();
	const result = await kernel.execute(source);
	if (result.status !== "ok") throw new Error(`Python CAD request failed: ${result.stderr || result.stdout}`);
	const line = result.stdout.trim().split("\n").at(-1);
	if (!line) throw new Error("Python CAD request returned no JSON result");
	return JSON.parse(line);
}

async function createSessionAndRun(name: string): Promise<{ live: LiveSession; run: any }> {
	const manager = SessionManager.create(cwd, join(sessionsDir, name));
	const session = await makeSession(manager);
	const run = await pythonRequest(session, `import asyncio, json\nfrom cad.client import request\nvalue = await request("workflow-start", id="mechanical.default")\nprint(json.dumps(value, sort_keys=True))`);
	persistBinding(manager, run);
	return { live: session, run };
}

try {
	if (mode === "create") {
		const { live: a, run: runA } = await createSessionAndRun("sessions-a");
		const idA = a.manager.getSessionId();
		const { live: b, run: runB } = await createSessionAndRun("sessions-b");
		const idB = b.manager.getSessionId();
		if (idA === idB || runA.runId === runB.runId) throw new Error("Prime sessions did not receive separate workflow runs");
		const observedA = await pythonRequest(a, `import asyncio, json, rlm\nvalue = await rlm.host_request("pi-cad.authority", {"request": {"schema": 1, "op": "workflow-current", "sessionId": ${JSON.stringify(idB)}, "runId": ${JSON.stringify(runB.runId)}, "sessionCapability": "forged", "binding": {"schema": 1, "sessionId": ${JSON.stringify(idB)}, "runId": ${JSON.stringify(runB.runId)}, "workflowHash": ${JSON.stringify(runB.workflowHash)}, "boundAt": "2026-10-15T00:00:00.000Z"}}})\nprint(json.dumps(value, sort_keys=True))`);
		const currentB = await pythonRequest(b, `import asyncio, json\nfrom cad.client import request\nvalue = await request("workflow-current")\nprint(json.dumps(value, sort_keys=True))`);
		if (observedA?.result?.runId !== runA.runId || currentB?.runId !== runB.runId) {
			throw new Error(`concurrent AgentSessions read another conversation's run: ${JSON.stringify({ idA, idB, runA: runA?.runId, runB: runB?.runId, forgedFromA: observedA?.result?.runId, currentB: currentB?.runId })}`);
		}
		console.log(JSON.stringify({ idA, idB, runA: runA.runId, runB: runB.runId, forgedFromA: observedA.result.runId, currentB: currentB.runId,
			fileA: a.manager.getSessionFile(), fileB: b.manager.getSessionFile() }));
	} else {
		const fileA = process.env.PI_CAD_E2E_RESUME_FILE!;
		const managerA = SessionManager.open(fileA, join(sessionsDir, "sessions-a"), cwd);
		const idA = managerA.getSessionId();
		const a = await makeSession(managerA);
		const resumed = await pythonRequest(a, `import asyncio, json\nfrom cad.client import request\nvalue = await request("workflow-current")\nprint(json.dumps(value, sort_keys=True))`);
		if (resumed?.runId !== process.env.PI_CAD_E2E_RUN_A) throw new Error("resumed AgentSession did not restore its own workflow binding");
		const childManager = SessionManager.forkFrom(fileA, cwd, join(sessionsDir, "sessions-child"));
		const child = await makeSession(childManager);
		const childBefore = await pythonRequest(child, `import asyncio, json\nfrom cad.client import request\nvalue = await request("workflow-current")\nprint(json.dumps(value, sort_keys=True))`);
		if (childBefore !== null) throw new Error("forked AgentSession inherited its parent's workflow run");
		const runC = await pythonRequest(child, `import asyncio, json\nfrom cad.client import request\nvalue = await request("workflow-start", id="mechanical.default")\nprint(json.dumps(value, sort_keys=True))`);
		if (runC?.runId === process.env.PI_CAD_E2E_RUN_A || runC?.runId === process.env.PI_CAD_E2E_RUN_B) {
			throw new Error("forked AgentSession did not receive a distinct workflow run");
		}
		console.log(JSON.stringify({ resumedSessionId: idA, resumedRun: resumed.runId, childSessionId: childManager.getSessionId(), childBefore, childRun: runC.runId }));
	}
} finally {
	await Promise.all(live.map(async (session) => { await session.provisioner.dispose(); session.session.dispose(); }));
}

// Prime's forkserver is process-scoped; let its parent-watch exit after this test worker ends.
process.exit(0);
