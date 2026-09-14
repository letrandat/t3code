// @effect-diagnostics nodeBuiltinImport:off - the suite boots real hosts and reads their evidence files directly.
// @effect-diagnostics preferSchemaOverJson:off - assertions parse the host's own JSON evidence files.
import * as NodeURL from "node:url";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeFSP from "node:fs/promises";
import { afterEach, describe, expect, it } from "vite-plus/test";

import { DevinOpenTurn, type DevinEvent } from "./DevinOpenTurn.ts";

const cleanups: Array<() => unknown> = [];
afterEach(async () => {
  let cleanup = cleanups.pop();
  while (cleanup) {
    await cleanup();
    cleanup = cleanups.pop();
  }
});

function makeOptions(
  cwd: string,
  onEvent: (event: DevinEvent) => void,
  runtimeMode?: DevinOpenTurn["options"]["runtimeMode"],
): DevinOpenTurn["options"] {
  return {
    cwd,
    runRoot: NodePath.join(cwd, "runs"),
    threadId: "test-thread",
    providerInstanceId: "test-provider",
    binary: process.execPath,
    args: [NodeURL.fileURLToPath(new URL("./testFixtures/fakeDevin.mjs", import.meta.url))],
    model: "fake-model",
    compactionThresholdTokens: 240000,
    environment: { ...process.env, HOME: cwd },
    ...(runtimeMode ? { runtimeMode } : {}),
    onEvent,
  };
}

async function setup(sharedCwd?: string, runtimeMode?: DevinOpenTurn["options"]["runtimeMode"]) {
  const cwd =
    sharedCwd ?? (await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-devin-client-")));
  const events: DevinEvent[] = [];
  let wake: (() => void) | undefined;
  const options = makeOptions(
    cwd,
    (event) => {
      events.push(event);
      wake?.();
    },
    runtimeMode,
  );
  const runtime = new DevinOpenTurn(options);
  cleanups.push(() => runtime.close());
  const take = async (...types: DevinEvent["type"][]) => {
    while (true) {
      const index = events.findIndex((event) => types.includes(event.type));
      if (index >= 0) return events.splice(index, 1)[0]!;
      await new Promise<void>((resolve) => {
        wake = resolve;
      });
    }
  };
  await runtime.start();
  return { cwd, runtime, take, options };
}

function makeTake(events: DevinEvent[], wakeRef: { wake?: () => void }) {
  return async (...types: DevinEvent["type"][]) => {
    while (true) {
      const index = events.findIndex((event) => types.includes(event.type));
      if (index >= 0) return events.splice(index, 1)[0]!;
      await new Promise<void>((resolve) => {
        wakeRef.wake = resolve;
      });
    }
  };
}

describe("devin open turn client", () => {
  it("multiple replies and context actions keep one native session/prompt", async () => {
    const { cwd, runtime, take } = await setup();
    await runtime.submit("FIRST");
    expect(((await take("text")) as { text: string }).text).toMatch(/FIRST/);
    const first = await take("waiting");
    await runtime.submit("SECOND");
    expect(((await take("text")) as { text: string }).text).toMatch(/SECOND/);
    expect(await take("waiting")).toEqual(first);
    for (const mode of ["compact"] as const) {
      await runtime.context(mode);
      await expect(runtime.submit("too early")).rejects.toThrow(/busy/);
      const event = await take("context");
      expect(event.type === "context" && event.mode).toBe(mode);
      expect(await take("waiting")).toEqual(first);
    }
    await runtime.submit("THIRD");
    await take("waiting");
    const protocol = (
      await NodeFSP.readFile(NodePath.join(cwd, "runs", runtime.runId, "protocol.jsonl"), "utf8")
    )
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    const sent = protocol.filter((entry) => entry.direction === "sent");
    expect(sent.filter((entry) => entry.message.method === "session/prompt")).toHaveLength(1);
    expect(
      JSON.stringify(sent.find((entry) => entry.message.method === "session/prompt")),
    ).not.toMatch(/FIRST/);
    expect(sent.filter((entry) => entry.message.method === "session/cancel")).toHaveLength(0);
    const config = JSON.parse(
      await NodeFSP.readFile(NodePath.join(cwd, "runs", runtime.runId, "config.json"), "utf8"),
    );
    expect(config.agent.compaction_threshold_tokens).toBe(240000);
  }, 30000);

  it("soft cancel waits for a running tool, blocks the next call, and resumes the same prompt", async () => {
    const { cwd, runtime, take } = await setup();
    await runtime.submit("LONG_TOOL");
    expect(((await take("text")) as { text: string }).text).toBe("TOOL_RUNNING");
    runtime.softCancel();
    runtime.softCancel();
    await take("cancel-requested");
    await expect(runtime.submit("too early")).rejects.toThrow(/busy/);
    await NodeFSP.writeFile(NodePath.join(cwd, "release-tool"), "");
    const stopped = await take("waiting");
    expect(stopped.type === "waiting" && stopped.cancelled).toBe(true);
    const blocked = JSON.parse(
      await NodeFSP.readFile(NodePath.join(cwd, "tool-result.json"), "utf8"),
    );
    expect(blocked.decision).toBe("block");
    expect(blocked.reason).toMatch(/report partial results/);
    await runtime.context("compact");
    await take("context");
    const compacted = await take("waiting");
    expect(compacted.type === "waiting" && compacted.cancelled).toBe(false);
    await runtime.submit("CONTINUE");
    const resumed = await take("waiting");
    expect(resumed.type === "waiting" && resumed.cancelled).toBe(false);
    expect(resumed.type === "waiting" && resumed.promptId).toBe(
      stopped.type === "waiting" && stopped.promptId,
    );
    const log = await NodeFSP.readFile(
      NodePath.join(cwd, "runs", runtime.runId, "protocol.jsonl"),
      "utf8",
    );
    expect(log.match(/"method":"session\/prompt"/g)).toHaveLength(1);
    expect(log).not.toMatch(/session\/cancel/);
  }, 30000);

  it("two runs share cwd while output, compact and shutdown stay separate", async () => {
    const a = await setup();
    const b = await setup(a.cwd);
    expect(a.runtime.runId).not.toBe(b.runtime.runId);
    await Promise.all([a.runtime.submit("TASK_A"), b.runtime.submit("TASK_B")]);
    const [wa, wb] = await Promise.all([a.take("waiting"), b.take("waiting")]);
    expect(wa).not.toEqual(wb);
    expect(((await a.take("text")) as { text: string }).text).toMatch(/TASK_A/);
    expect(((await b.take("text")) as { text: string }).text).toMatch(/TASK_B/);
    await Promise.all([a.runtime.context("compact"), b.runtime.context("compact")]);
    await Promise.all([a.take("context"), b.take("context")]);
    await Promise.all([a.take("waiting"), b.take("waiting")]);
    await a.runtime.close();
    await b.runtime.submit("B_STILL_ALIVE");
    await b.take("waiting");
    expect(((await b.take("text")) as { text: string }).text).toMatch(/B_STILL_ALIVE/);
    for (const item of [a, b]) {
      const state = NodePath.join(item.cwd, "runs", item.runtime.runId);
      const log = await NodeFSP.readFile(NodePath.join(state, "protocol.jsonl"), "utf8");
      expect(log.match(/"method":"session\/prompt"/g)).toHaveLength(1);
      expect(log).not.toMatch(/session\/cancel/);
      const identity = JSON.parse(
        await NodeFSP.readFile(NodePath.join(state, "identity.json"), "utf8"),
      );
      expect(identity.cwd).toBe(a.cwd);
      expect(identity.runId).toBe(item.runtime.runId);
    }
    await expect(NodeFSP.stat(NodePath.join(a.cwd, ".devin-worker"))).rejects.toMatchObject({
      code: "ENOENT",
    });
  }, 30000);

  it("stopping A does not cancel a working B in the same cwd", async () => {
    const a = await setup();
    const b = await setup(a.cwd);
    await Promise.all([
      a.runtime.submit("LONG_TOOL_ISOLATED"),
      b.runtime.submit("LONG_TOOL_ISOLATED"),
    ]);
    await Promise.all([a.take("text"), b.take("text")]);
    a.runtime.softCancel();
    await NodeFSP.writeFile(NodePath.join(a.cwd, "runs", a.runtime.runId, "release-tool"), "");
    const stopped = await a.take("waiting");
    expect(stopped.type === "waiting" && stopped.cancelled).toBe(true);
    await expect(
      NodeFSP.stat(NodePath.join(b.cwd, "runs", b.runtime.runId, "tool-result.json")),
    ).rejects.toMatchObject({ code: "ENOENT" });
    await NodeFSP.writeFile(NodePath.join(b.cwd, "runs", b.runtime.runId, "release-tool"), "");
    const continued = await b.take("waiting");
    expect(continued.type === "waiting" && continued.cancelled).toBe(false);
    const result = JSON.parse(
      await NodeFSP.readFile(
        NodePath.join(b.cwd, "runs", b.runtime.runId, "tool-result.json"),
        "utf8",
      ),
    );
    expect(result.decision).toBe(undefined);
  }, 30000);

  it("numeric permission IDs and unavailable choices retain the pending request", async () => {
    const { runtime, take } = await setup();
    await runtime.submit("ASK_PERMISSION NUMERIC");
    const request = await take("permission");
    expect(request.type).toBe("permission");
    if (request.type !== "permission") return;
    await expect(
      runtime.respondToPermission(request.requestId, "acceptForSession"),
    ).rejects.toThrow(/did not offer/);
    await runtime.respondToPermission(request.requestId, "decline");
    await take("permission-resolved");
    const result = await take("text");
    expect(result.type === "text" && result.text).toBe(
      'PERMISSION_RESULT:{"outcome":{"outcome":"selected","optionId":"deny-id"}}',
    );
    await take("waiting");
    await expect(runtime.respondToPermission(request.requestId, "accept")).rejects.toThrow(
      /no longer pending/,
    );
    await runtime.submit("ASK_PERMISSION NO_OPTIONS");
    const empty = await take("permission");
    if (empty.type !== "permission") throw new Error("Expected a permission request");
    expect(empty.options).toEqual([{ decision: "cancel", label: "Cancel" }]);
    await expect(runtime.respondToPermission(empty.requestId, "accept")).rejects.toThrow(
      /did not offer/,
    );
    await runtime.respondToPermission(empty.requestId, "cancel");
    await take("waiting");
  }, 30000);

  it("full-access auto-selects allow_always without a permission event", async () => {
    const { runtime, take } = await setup(undefined, "full-access");
    await runtime.submit("ASK_PERMISSION");
    const result = await take("text", "permission");
    expect(result.type).toBe("text");
    expect(result.type === "text" && result.text).toBe(
      'PERMISSION_RESULT:{"outcome":{"outcome":"selected","optionId":"always-id"}}',
    );
    await take("waiting");
  }, 30000);

  it("auto-accept edits auto-selects file changes and still asks for commands", async () => {
    const { runtime, take } = await setup(undefined, "auto-accept-edits");
    await runtime.submit("ASK_PERMISSION EDIT");
    const edit = await take("text", "permission");
    expect(edit.type).toBe("text");
    expect(edit.type === "text" && edit.text).toBe(
      'PERMISSION_RESULT:{"outcome":{"outcome":"selected","optionId":"always-id"}}',
    );
    await take("waiting");
    await runtime.submit("ASK_PERMISSION");
    const command = await take("permission");
    expect(command.type).toBe("permission");
    if (command.type !== "permission") return;
    await runtime.respondToPermission(command.requestId, "accept");
    await take("permission-resolved");
    await take("waiting");
  }, 30000);

  it("permission requests for another native session are cancelled", async () => {
    const { runtime, take } = await setup();
    await runtime.submit("ASK_PERMISSION WRONG_SESSION");
    const result = await take("text");
    expect(result.type === "text" && result.text).toBe(
      'PERMISSION_RESULT:{"outcome":{"outcome":"cancelled"}}',
    );
    await take("waiting");
  }, 30000);

  it("detach and attach continue the same native run without a new prompt", async () => {
    const { cwd, runtime, take, options } = await setup();
    await runtime.submit("FIRST");
    expect(((await take("text")) as { text: string }).text).toMatch(/FIRST/);
    const first = await take("waiting");
    const snapshot = runtime.snapshot();
    expect(snapshot.sessionId).toBeDefined();
    runtime.detach();

    const events: DevinEvent[] = [];
    const wakeRef: { wake?: () => void } = {};
    const attached = await DevinOpenTurn.attach({
      ...options,
      onEvent: (event) => {
        events.push(event);
        wakeRef.wake?.();
      },
      attachTo: { runDir: snapshot.runDir, runId: snapshot.runId },
    });
    cleanups.push(() => attached.close());
    const takeAttached = makeTake(events, wakeRef);
    expect(attached.snapshot().phase).toBe("waiting");
    await attached.submit("SECOND");
    expect(((await takeAttached("text")) as { text: string }).text).toMatch(/SECOND/);
    expect(await takeAttached("waiting")).toEqual(first);
    const log = await NodeFSP.readFile(
      NodePath.join(cwd, "runs", runtime.runId, "protocol.jsonl"),
      "utf8",
    );
    expect(log.match(/"method":"session\/prompt"/g)).toHaveLength(1);
    await attached.close();
  }, 30000);

  it("attach refuses a deliberately shut down run without spawning", async () => {
    const { cwd, runtime, take, options } = await setup();
    await runtime.submit("FIRST");
    await take("text");
    await take("waiting");
    await runtime.close();
    await expect(
      DevinOpenTurn.attach({
        ...options,
        onEvent: () => {},
        attachTo: { runDir: NodePath.join(cwd, "runs", runtime.runId), runId: runtime.runId },
      }),
    ).rejects.toThrow(/shut down deliberately/);
    const log = await NodeFSP.readFile(
      NodePath.join(cwd, "runs", runtime.runId, "protocol.jsonl"),
      "utf8",
    );
    expect(log.match(/"method":"session\/prompt"/g)).toHaveLength(1);
  }, 30000);

  it("attach refuses a run directory outside the run root", async () => {
    const { options } = await setup();
    await expect(
      DevinOpenTurn.attach({
        ...options,
        onEvent: () => {},
        attachTo: { runDir: NodePath.join(NodeOS.tmpdir(), "elsewhere", "run"), runId: "run" },
      }),
    ).rejects.toThrow(/outside the Devin run root/);
  }, 30000);

  it("host death while idle surfaces host-lost and reaps the orphan", async () => {
    const { cwd, runtime, take } = await setup();
    await runtime.submit("FIRST");
    await take("text");
    await take("waiting");
    const runDir = NodePath.join(cwd, "runs", runtime.runId);
    const claim = JSON.parse(await NodeFSP.readFile(NodePath.join(runDir, "host.claim"), "utf8"));
    const status = JSON.parse(await NodeFSP.readFile(NodePath.join(runDir, "status.json"), "utf8"));
    process.kill(claim.hostPid, "SIGKILL");
    await take("host-lost");
    await runtime.close();
    expect(() => process.kill(status.childPid, 0)).toThrow();
  }, 30000);
});
