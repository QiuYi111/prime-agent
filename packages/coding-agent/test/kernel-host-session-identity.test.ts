import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createAgentSessionFromServices, createAgentSessionServices } from "../src/core/agent-session-services.js";
import { createExtensionRuntime, type Extension } from "../src/core/extensions/index.js";
import { createSyntheticSourceInfo } from "../src/core/source-info.js";
import { SessionManager } from "../src/core/session-manager.js";
import { SettingsManager } from "../src/core/settings-manager.js";
import { IpythonKernelProvisioner } from "../src/core/tools/ipython.js";

describe("Prime kernel host request session identity", () => {
  const cleanup: string[] = [];
  afterEach(() => { while (cleanup.length) rmSync(cleanup.pop()!, { recursive: true, force: true }); });

  it("binds real Python host requests to concurrent, child, and resumed AgentSessions", async () => {
    const root = mkdtempSync(join(tmpdir(), "pi-host-session-"));
    cleanup.push(root);
    const cwd = join(root, "project"), agentDir = join(root, "agent");
    mkdirSync(cwd, { recursive: true }); mkdirSync(agentDir, { recursive: true });
    const managers = [SessionManager.create(cwd, join(root, "sessions-a")), SessionManager.create(cwd, join(root, "sessions-b"))];
    const sessions: Array<{ dispose(): unknown }> = [], kernels: IpythonKernelProvisioner[] = [];

    async function makeSession(manager: SessionManager) {
      const services = await createAgentSessionServices({
        cwd, agentDir, settingsManager: SettingsManager.create(cwd, agentDir),
        noBuiltinHerdrReporter: true, telemetryDisabled: true,
        resourceLoaderOptions: { noExtensions: true, noPromptTemplates: true, noThemes: true },
      });
      const sourceInfo = createSyntheticSourceInfo("<test:session-authority>", { source: "test" });
      const extension: Extension = {
        path: "session-authority", resolvedPath: "/trusted/session-authority.ts", sourceInfo,
        handlers: new Map(), tools: new Map(), messageRenderers: new Map(), commands: new Map(), flags: new Map(), shortcuts: new Map(),
        kernelHostRequests: new Map([["pi-cad.authority", {
          type: "pi-cad.authority", sourceInfo,
          handler: (payload, ctx) => {
            const request = payload.request && typeof payload.request === "object" ? payload.request as Record<string, unknown> : {};
            return { sessionId: ctx.sessionId, runId: "trusted-run:" + ctx.sessionId,
              claimedSessionId: request.sessionId ?? null, claimedRunId: request.runId ?? null };
          },
        }]]),
      };
      const runtime = createExtensionRuntime();
      const loader = Object.create(services.resourceLoader) as typeof services.resourceLoader;
      loader.getExtensions = () => ({ extensions: [extension], errors: [], runtime });
      services.resourceLoader = loader;
      const { session } = await createAgentSessionFromServices({ services, sessionManager: manager });
      sessions.push(session);
      return session;
    }

    async function request(session: unknown, manager: SessionManager, forged: string) {
      const handlers = (session as { _createKernelHostHandlers(): Record<string, (payload: Record<string, unknown>) => Promise<Record<string, unknown>>> })._createKernelHostHandlers();
      const provisioner = new IpythonKernelProvisioner(cwd, { hostHandlers: handlers, sessionId: manager.getSessionId() });
      kernels.push(provisioner);
      const kernel = await provisioner.ensure();
      const result = await kernel.execute(`import json, rlm
value = await rlm.host_request("pi-cad.authority", {"request": {"sessionId": ${JSON.stringify(forged)}, "runId": "forged-run"}})
print(json.dumps(value, sort_keys=True))`);
      expect(result.status).toBe("ok");
      return JSON.parse(result.stdout.trim()) as Record<string, unknown>;
    }

    try {
      const a = await makeSession(managers[0]!);
      const b = await makeSession(managers[1]!);
      const idA = managers[0]!.getSessionId(), idB = managers[1]!.getSessionId();
      expect(idA).not.toBe(idB);
      const [responseA, responseB] = await Promise.all([request(a, managers[0]!, idB), request(b, managers[1]!, idA)]);
      expect(responseA).toEqual({ sessionId: idA, runId: "trusted-run:" + idA, claimedSessionId: idB, claimedRunId: "forged-run" });
      expect(responseB).toEqual({ sessionId: idB, runId: "trusted-run:" + idB, claimedSessionId: idA, claimedRunId: "forged-run" });

      managers[0]!.appendCustomEntry("test.session-binding", { runId: "trusted-run-marker" });
      // The first session entry is intentionally not flushed until a real response;
      // persist the header so this test resumes the same session file/identity.
      managers[0]!.flushNow();
      const fileA = managers[0]!.getSessionFile()!;
      await kernels[0]!.dispose(); kernels.splice(0, 1);
      await (a as unknown as { dispose(): Promise<void> }).dispose(); sessions.splice(sessions.indexOf(a), 1);
      const resumedManager = SessionManager.open(fileA, join(root, "sessions-a"), cwd);
      expect(resumedManager.getSessionId()).toBe(idA);
      const resumed = await makeSession(resumedManager);
      const resumedResponse = await request(resumed, resumedManager, idB);
      expect(resumedResponse.sessionId).toBe(idA);
      expect(resumedResponse.runId).toBe("trusted-run:" + idA);

      const childManager = SessionManager.forkFrom(fileA, cwd, join(root, "sessions-child"));
      const childId = childManager.getSessionId();
      expect(childId).not.toBe(idA);
      const child = await makeSession(childManager);
      const childResponse = await request(child, childManager, idA);
      expect(childResponse.sessionId).toBe(childId);
      expect(childResponse.runId).toBe("trusted-run:" + childId);
    } finally {
      await Promise.all(kernels.map((kernel) => kernel.dispose()));
      await Promise.all(sessions.map((session) => session.dispose()));
    }
  });
});
