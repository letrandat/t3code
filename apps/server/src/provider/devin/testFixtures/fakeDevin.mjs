import * as NodeReadline from "node:readline";
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodeChildProcess from "node:child_process";
import * as NodePath from "node:path";
const config = JSON.parse(
  await NodeFSP.readFile(process.argv[process.argv.indexOf("--config") + 1], "utf8"),
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
    const child = NodeChildProcess.spawn(config.hooks[name][0].hooks[0].command, {
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
    const watcher = NodeFS.watch(NodePath.dirname(path), () => {
      if (NodeFS.existsSync(path)) {
        watcher.close();
        resolve();
      }
    });
    if (NodeFS.existsSync(path)) {
      watcher.close();
      resolve();
    }
  });
const permissions = new Map();
let permissionSequence = 0;
const permission = (text) =>
  new Promise((resolve) => {
    const id = text.includes("NUMERIC")
      ? ++permissionSequence
      : `permission-${++permissionSequence}`;
    permissions.set(id, resolve);
    send({
      id,
      method: "session/request_permission",
      params: {
        sessionId: text.includes("WRONG_SESSION")
          ? "other-session"
          : `session-${process.env.DEVIN_CONTROL_RUN_ID}`,
        toolCall: {
          toolCallId: "exec-1",
          kind: "execute",
          title: "Run command",
          rawInput: { command: "echo approved" },
        },
        options: text.includes("NO_OPTIONS")
          ? []
          : [
              { optionId: "once-id", kind: "allow_once", name: "Allow once" },
              { optionId: "always-id", kind: "allow_always", name: "Always allow" },
              { optionId: "deny-id", kind: "reject_once", name: "Deny" },
            ],
      },
    });
  });
async function run(text) {
  let count = 0;
  while (true) {
    if (text.includes("ASK_PERMISSION")) {
      const result = await permission(text);
      update(`PERMISSION_RESULT:${JSON.stringify(result)}`);
    } else if (text.includes("READ_ATTACHMENT") || text.startsWith("[Attached")) {
      const paths = [...text.matchAll(/is saved at: (.+?)\]/g)].map((match) => match[1]);
      const contents = await Promise.all(
        paths.map(async (path) => {
          const bytes = await NodeFSP.readFile(path);
          return path.endsWith(".png") ? `PNG:${bytes.toString("base64")}` : bytes.toString("utf8");
        }),
      );
      update(`ATTACHMENT_CONTENT:${contents.join("|")}`);
    } else if (text.includes("LONG_TOOL")) {
      update("TOOL_RUNNING");
      const target = text.includes("ISOLATED") ? process.env.DEVIN_CONTROL_DIR : process.cwd();
      await waitFile(NodePath.join(target, "release-tool"));
      const result = await hook("PreToolUse", { tool_name: "exec" });
      await NodeFSP.writeFile(NodePath.join(target, "tool-result.json"), JSON.stringify(result));
      update(result.decision === "block" ? "PARTIAL_RESULT" : "TOOL_EXECUTED");
    } else update(`Reply ${++count}: ${text}`);
    let response = await hook("Stop");
    if (response.decision !== "block") process.exit(2);
    while (NodeFS.existsSync(`${process.env.DEVIN_CONTROL_DIR}/compact.request`)) {
      await NodeFSP.unlink(`${process.env.DEVIN_CONTROL_DIR}/compact.request`);
      const fresh = NodeFS.existsSync(`${process.env.DEVIN_CONTROL_DIR}/fresh.request`);
      if (fresh) await NodeFSP.unlink(`${process.env.DEVIN_CONTROL_DIR}/fresh.request`);
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
NodeReadline.createInterface({ input: process.stdin }).on("line", (line) => {
  const message = JSON.parse(line);
  if (!message.method && permissions.has(message.id)) {
    permissions.get(message.id)(message.result);
    permissions.delete(message.id);
  }
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
