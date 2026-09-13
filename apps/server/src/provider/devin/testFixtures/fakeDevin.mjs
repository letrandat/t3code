import { createInterface } from "node:readline";
import { readFile, writeFile, unlink } from "node:fs/promises";
import { watch, existsSync } from "node:fs";
import { spawn } from "node:child_process";
import { join, dirname } from "node:path";
const config = JSON.parse(
  await readFile(process.argv[process.argv.indexOf("--config") + 1], "utf8"),
);
const model = process.argv[process.argv.indexOf("--model") + 1];
const send = (message) =>
  process.stdout.write(JSON.stringify({ jsonrpc: "2.0", ...message }) + "\n");
const update = (text) =>
  send({
    method: "session/update",
    params: {
      sessionId: `session-${process.env.DEVIN_CONTROL_RUN_ID}`,
      update: { sessionUpdate: "agent_message_chunk", content: { type: "text", text } },
    },
  });
const hook = (name, extra = {}) =>
  new Promise((resolve) => {
    const child = spawn(config.hooks[name][0].hooks[0].command, {
      shell: true,
      stdio: ["pipe", "pipe", "inherit"],
    });
    let out = "";
    child.stdout.on("data", (chunk) => (out += chunk));
    child.on("exit", () => resolve(JSON.parse(out || "{}")));
    child.stdin.end(
      JSON.stringify({
        hook_event_name: name,
        session_id: `session-${process.env.DEVIN_CONTROL_RUN_ID}`,
        prompt_id: `prompt-${process.env.DEVIN_CONTROL_RUN_ID}`,
        ...extra,
      }),
    );
  });
const waitFile = (path) =>
  new Promise((resolve) => {
    const watcher = watch(dirname(path), () => {
      if (existsSync(path)) {
        watcher.close();
        resolve();
      }
    });
    if (existsSync(path)) {
      watcher.close();
      resolve();
    }
  });
async function run(text) {
  let count = 0;
  while (true) {
    if (text.includes("LONG_TOOL")) {
      update("TOOL_RUNNING");
      const target = text.includes("ISOLATED") ? process.env.DEVIN_CONTROL_DIR : process.cwd();
      await waitFile(join(target, "release-tool"));
      const result = await hook("PreToolUse", { tool_name: "exec" });
      await writeFile(join(target, "tool-result.json"), JSON.stringify(result));
      update(result.decision === "block" ? "PARTIAL_RESULT" : "TOOL_EXECUTED");
    } else update(`Reply ${++count}: ${text}`);
    let response = await hook("Stop");
    if (response.decision !== "block") process.exit(2);
    while (existsSync(`${process.env.DEVIN_CONTROL_DIR}/compact.request`)) {
      await unlink(`${process.env.DEVIN_CONTROL_DIR}/compact.request`);
      const fresh = existsSync(`${process.env.DEVIN_CONTROL_DIR}/fresh.request`);
      if (fresh) await unlink(`${process.env.DEVIN_CONTROL_DIR}/fresh.request`);
      await hook("PostCompaction", {
        summary: fresh
          ? "Fresh task context. Keep the worker instructions."
          : "Retained working summary.",
      });
      response = await hook("Stop");
    }
    text = response.reason;
  }
}
createInterface({ input: process.stdin }).on("line", (line) => {
  const message = JSON.parse(line);
  if (message.method === "initialize") send({ id: message.id, result: { protocolVersion: 1 } });
  if (message.method === "session/new") update("EARLY_CONFIG_UPDATE");
  if (message.method === "session/new")
    send({
      id: message.id,
      result: {
        sessionId: `session-${process.env.DEVIN_CONTROL_RUN_ID}`,
        configOptions: [{ id: "model", currentValue: model }],
      },
    });
  if (message.method === "session/prompt") void run(message.params.prompt[0].text);
});
