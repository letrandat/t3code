import * as NodeTest from "node:test";
import * as NodeURL from "node:url";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeFSP from "node:fs/promises";

import * as NodeAssert from "node:assert/strict";

import { DevinOpenTurn, type DevinEvent } from "./DevinOpenTurn.ts";

async function setup(t: { after: (fn: () => void) => void }) {
  const cwd = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-devin-NodeTest.test-"));
  const events: DevinEvent[] = [];
  let wake: (() => void) | undefined;
  const runtime = new DevinOpenTurn({
    cwd,
    binary: process.execPath,
    args: [NodeURL.fileURLToPath(new URL("./testFixtures/fakeDevin.mjs", import.meta.url))],
    model: "fake-model",
    allowNativePrompt: true,
    compactionThresholdTokens: 240000,
    onEvent: (event) => {
      events.push(event);
      wake?.();
    },
  });
  t.after(() => runtime.close());
  const take = async (type: DevinEvent["type"]) => {
    while (true) {
      const index = events.findIndex((event) => event.type === type);
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
    for (const mode of ["compact", "fresh"] as const) {
      await runtime.context(mode);
      await NodeAssert.rejects(runtime.submit("too early"), /busy/);
      const event = await take("context");
      NodeAssert.equal(event.type === "context" && event.mode, mode);
      NodeAssert.deepEqual(await take("waiting"), first);
    }
    await runtime.submit("THIRD");
    await take("waiting");
    const protocol = (
      await NodeFSP.readFile(NodePath.join(cwd, ".devin-worker/protocol.jsonl"), "utf8")
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
      await NodeFSP.readFile(NodePath.join(cwd, ".devin-worker/config.json"), "utf8"),
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
    const log = await NodeFSP.readFile(NodePath.join(cwd, ".devin-worker/protocol.jsonl"), "utf8");
    NodeAssert.equal(log.match(/"method":"session\/prompt"/g)?.length, 1);
    NodeAssert.doesNotMatch(log, /session\/cancel/);
  },
);

NodeTest.test("unapproved runs never launch native ACP", async () => {
  const cwd = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-devin-denied-"));
  const runtime = new DevinOpenTurn({
    cwd,
    binary: "/does/not/exist",
    model: "none",
    allowNativePrompt: false,
    onEvent: () => {},
  });
  await NodeAssert.rejects(runtime.start(), /disabled/);
});
