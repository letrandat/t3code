import * as NodeTest from "node:test";
import * as NodeURL from "node:url";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeFSP from "node:fs/promises";

import * as NodeAssert from "node:assert/strict";

import { DevinOpenTurn, type DevinEvent } from "./DevinOpenTurn.ts";

async function setup(
  t: { after: (fn: () => void) => void },
  sharedCwd?: string,
  runtimeMode?: DevinOpenTurn["options"]["runtimeMode"],
) {
  const cwd =
    sharedCwd ?? (await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-devin-NodeTest.test-")));
  const events: DevinEvent[] = [];
  let wake: (() => void) | undefined;
  const runtime = new DevinOpenTurn({
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
    onEvent: (event) => {
      events.push(event);
      wake?.();
    },
  });
  t.after(() => runtime.close());
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
  return { cwd, runtime, take };
}

NodeTest.test(
  "multiple replies and context actions keep one native session/prompt",
  { timeout: 10000 },
  async (t) => {
    const { cwd, runtime, take } = await setup(t);
    await runtime.submit("FIRST");
    NodeAssert.match(((await take("text")) as { text: string }).text, /FIRST/);
    const first = await take("waiting");
    await runtime.submit("SECOND");
    NodeAssert.match(((await take("text")) as { text: string }).text, /SECOND/);
    NodeAssert.deepEqual(await take("waiting"), first);
    for (const mode of ["compact"] as const) {
      await runtime.context(mode);
      await NodeAssert.rejects(runtime.submit("too early"), /busy/);
      const event = await take("context");
      NodeAssert.equal(event.type === "context" && event.mode, mode);
      NodeAssert.deepEqual(await take("waiting"), first);
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
    NodeAssert.equal(sent.filter((entry) => entry.message.method === "session/prompt").length, 1);
    NodeAssert.doesNotMatch(
      JSON.stringify(sent.find((entry) => entry.message.method === "session/prompt")),
      /FIRST/,
    );
    NodeAssert.equal(sent.filter((entry) => entry.message.method === "session/cancel").length, 0);
    const config = JSON.parse(
      await NodeFSP.readFile(NodePath.join(cwd, "runs", runtime.runId, "config.json"), "utf8"),
    );
    NodeAssert.equal(config.agent.compaction_threshold_tokens, 240000);
  },
);

NodeTest.test(
  "soft cancel waits for a running tool, blocks the next call, and resumes the same prompt",
  { timeout: 10000 },
  async (t) => {
    const { cwd, runtime, take } = await setup(t);
    await runtime.submit("LONG_TOOL");
    NodeAssert.equal(((await take("text")) as { text: string }).text, "TOOL_RUNNING");
    runtime.softCancel();
    runtime.softCancel();
    await take("cancel-requested");
    await NodeAssert.rejects(runtime.submit("too early"), /busy/);
    await NodeFSP.writeFile(NodePath.join(cwd, "release-tool"), "");
    const stopped = await take("waiting");
    NodeAssert.equal(stopped.type === "waiting" && stopped.cancelled, true);
    const blocked = JSON.parse(
      await NodeFSP.readFile(NodePath.join(cwd, "tool-result.json"), "utf8"),
    );
    NodeAssert.equal(blocked.decision, "block");
    NodeAssert.match(blocked.reason, /report partial results/);
    await runtime.context("compact");
    await take("context");
    const compacted = await take("waiting");
    NodeAssert.equal(compacted.type === "waiting" && compacted.cancelled, false);
    await runtime.submit("CONTINUE");
    const resumed = await take("waiting");
    NodeAssert.equal(resumed.type === "waiting" && resumed.cancelled, false);
    NodeAssert.equal(
      resumed.type === "waiting" && resumed.promptId,
      stopped.type === "waiting" && stopped.promptId,
    );
    const log = await NodeFSP.readFile(
      NodePath.join(cwd, "runs", runtime.runId, "protocol.jsonl"),
      "utf8",
    );
    NodeAssert.equal(log.match(/"method":"session\/prompt"/g)?.length, 1);
    NodeAssert.doesNotMatch(log, /session\/cancel/);
  },
);

NodeTest.test(
  "two runs share cwd while output, compact and shutdown stay separate",
  { timeout: 10000 },
  async (t) => {
    const a = await setup(t);
    const b = await setup(t, a.cwd);
    NodeAssert.notEqual(a.runtime.runId, b.runtime.runId);
    await Promise.all([a.runtime.submit("TASK_A"), b.runtime.submit("TASK_B")]);
    const [wa, wb] = await Promise.all([a.take("waiting"), b.take("waiting")]);
    NodeAssert.notDeepEqual(wa, wb);
    NodeAssert.match(((await a.take("text")) as { text: string }).text, /TASK_A/);
    NodeAssert.match(((await b.take("text")) as { text: string }).text, /TASK_B/);
    await Promise.all([a.runtime.context("compact"), b.runtime.context("compact")]);
    await Promise.all([a.take("context"), b.take("context")]);
    await Promise.all([a.take("waiting"), b.take("waiting")]);
    a.runtime.close();
    await b.runtime.submit("B_STILL_ALIVE");
    await b.take("waiting");
    NodeAssert.match(((await b.take("text")) as { text: string }).text, /B_STILL_ALIVE/);
    for (const item of [a, b]) {
      const state = NodePath.join(item.cwd, "runs", item.runtime.runId);
      const log = await NodeFSP.readFile(NodePath.join(state, "protocol.jsonl"), "utf8");
      NodeAssert.equal(log.match(/"method":"session\/prompt"/g)?.length, 1);
      NodeAssert.doesNotMatch(log, /session\/cancel/);
      const identity = JSON.parse(
        await NodeFSP.readFile(NodePath.join(state, "identity.json"), "utf8"),
      );
      NodeAssert.equal(identity.cwd, a.cwd);
      NodeAssert.equal(identity.runId, item.runtime.runId);
    }
    await NodeAssert.rejects(NodeFSP.stat(NodePath.join(a.cwd, ".devin-worker")), {
      code: "ENOENT",
    });
  },
);

NodeTest.test(
  "stopping A does not cancel a working B in the same cwd",
  { timeout: 10000 },
  async (t) => {
    const a = await setup(t);
    const b = await setup(t, a.cwd);
    await Promise.all([
      a.runtime.submit("LONG_TOOL_ISOLATED"),
      b.runtime.submit("LONG_TOOL_ISOLATED"),
    ]);
    await Promise.all([a.take("text"), b.take("text")]);
    a.runtime.softCancel();
    await NodeFSP.writeFile(NodePath.join(a.cwd, "runs", a.runtime.runId, "release-tool"), "");
    const stopped = await a.take("waiting");
    NodeAssert.equal(stopped.type === "waiting" && stopped.cancelled, true);
    await NodeAssert.rejects(
      NodeFSP.stat(NodePath.join(b.cwd, "runs", b.runtime.runId, "tool-result.json")),
      { code: "ENOENT" },
    );
    await NodeFSP.writeFile(NodePath.join(b.cwd, "runs", b.runtime.runId, "release-tool"), "");
    const continued = await b.take("waiting");
    NodeAssert.equal(continued.type === "waiting" && continued.cancelled, false);
    const result = JSON.parse(
      await NodeFSP.readFile(
        NodePath.join(b.cwd, "runs", b.runtime.runId, "tool-result.json"),
        "utf8",
      ),
    );
    NodeAssert.equal(result.decision, undefined);
  },
);

NodeTest.test(
  "numeric permission IDs and unavailable choices retain the pending request",
  { timeout: 10000 },
  async (t) => {
    const { runtime, take } = await setup(t);
    await runtime.submit("ASK_PERMISSION NUMERIC");
    const request = await take("permission");
    NodeAssert.equal(request.type, "permission");
    if (request.type !== "permission") return;
    await NodeAssert.rejects(
      runtime.respondToPermission(request.requestId, "acceptForSession"),
      /did not offer/,
    );
    await runtime.respondToPermission(request.requestId, "decline");
    await take("permission-resolved");
    const result = await take("text");
    NodeAssert.equal(
      result.type === "text" && result.text,
      'PERMISSION_RESULT:{"outcome":{"outcome":"selected","optionId":"deny-id"}}',
    );
    await take("waiting");
    await NodeAssert.rejects(
      runtime.respondToPermission(request.requestId, "accept"),
      /no longer pending/,
    );
    await runtime.submit("ASK_PERMISSION NO_OPTIONS");
    const empty = await take("permission");
    if (empty.type !== "permission") throw new Error("Expected a permission request");
    NodeAssert.deepEqual(empty.options, [{ decision: "cancel", label: "Cancel" }]);
    await NodeAssert.rejects(
      runtime.respondToPermission(empty.requestId, "accept"),
      /did not offer/,
    );
    await runtime.respondToPermission(empty.requestId, "cancel");
    await take("waiting");
  },
);

NodeTest.test(
  "full-access auto-selects allow_always without a permission event",
  { timeout: 10000 },
  async (t) => {
    const { runtime, take } = await setup(t, undefined, "full-access");
    await runtime.submit("ASK_PERMISSION");
    const result = await take("text", "permission");
    NodeAssert.equal(result.type, "text");
    NodeAssert.equal(
      result.type === "text" && result.text,
      'PERMISSION_RESULT:{"outcome":{"outcome":"selected","optionId":"always-id"}}',
    );
    await take("waiting");
  },
);

NodeTest.test(
  "auto-accept edits auto-selects file changes and still asks for commands",
  { timeout: 10000 },
  async (t) => {
    const { runtime, take } = await setup(t, undefined, "auto-accept-edits");
    await runtime.submit("ASK_PERMISSION EDIT");
    const edit = await take("text", "permission");
    NodeAssert.equal(edit.type, "text");
    NodeAssert.equal(
      edit.type === "text" && edit.text,
      'PERMISSION_RESULT:{"outcome":{"outcome":"selected","optionId":"always-id"}}',
    );
    await take("waiting");
    await runtime.submit("ASK_PERMISSION");
    const command = await take("permission");
    NodeAssert.equal(command.type, "permission");
    if (command.type !== "permission") return;
    await runtime.respondToPermission(command.requestId, "accept");
    await take("permission-resolved");
    await take("waiting");
  },
);

NodeTest.test(
  "permission requests for another native session are cancelled",
  { timeout: 10000 },
  async (t) => {
    const { runtime, take } = await setup(t);
    await runtime.submit("ASK_PERMISSION WRONG_SESSION");
    const result = await take("text");
    NodeAssert.equal(
      result.type === "text" && result.text,
      'PERMISSION_RESULT:{"outcome":{"outcome":"cancelled"}}',
    );
    await take("waiting");
  },
);
