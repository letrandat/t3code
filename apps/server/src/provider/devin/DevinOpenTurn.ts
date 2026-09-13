import * as NodeReadline from "node:readline";
import * as NodeNet from "node:net";
import * as NodeChildProcess from "node:child_process";
import * as NodeURL from "node:url";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeFSP from "node:fs/promises";
import * as NodeCrypto from "node:crypto";

export type DevinEvent =
  | { type: "text"; text: string }
  | { type: "waiting"; promptId: string; sessionId: string; cancelled: boolean }
  | { type: "cancel-requested" }
  | { type: "context"; mode: string; summary: string }
  | { type: "failed"; message: string };

type RecordValue = Record<string, unknown>;
function record(value: unknown): RecordValue {
  return typeof value === "object" && value !== null ? (value as RecordValue) : {};
}

/** Owns a single native prompt. Visible turns finish when the Stop hook connects. */
export class DevinOpenTurn {
  private child?: NodeChildProcess.ChildProcessWithoutNullStreams;
  private server?: NodeNet.Server;
  private waiting: NodeNet.Socket | undefined;
  private cancelRequested = false;
  private phase: "new" | "bootstrapping" | "working" | "waiting" | "context" | "ended" = "new";
  private firstTask: string | undefined;
  private promptId?: string;
  private sessionId?: string;
  private maintenance: "compact" | "fresh" | undefined;
  private pending = new Map<
    number,
    { resolve: (value: RecordValue) => void; reject: (e: Error) => void }
  >();
  private sequence = 0;
  private promptSent = false;
  private state: string;
  private socketPath = NodePath.join(NodeOS.tmpdir(), `t3-devin-${NodeCrypto.randomUUID()}.sock`);
  private options: {
    cwd: string;
    binary: string;
    model: string;
    allowNativePrompt: boolean;
    environment?: NodeJS.ProcessEnv;
    compactionThresholdTokens?: number | undefined;
    onEvent: (event: DevinEvent) => void;
    /** Test peers use an executable plus arguments; production uses the pinned binary. */
    args?: string[];
  };
  constructor(options: DevinOpenTurn["options"]) {
    this.options = options;
    this.state = NodePath.join(options.cwd, ".devin-worker");
  }

  private fail(message: string) {
    this.phase = "ended";
    for (const request of this.pending.values()) request.reject(new Error(message));
    this.pending.clear();
    this.options.onEvent({ type: "failed", message });
  }
  private async send(message: RecordValue) {
    await NodeFSP.appendFile(
      NodePath.join(this.state, "protocol.jsonl"),
      JSON.stringify({ direction: "sent", message }) + "\n",
    );
    if (!this.child?.stdin.writable)
      throw new Error("Native transport is unavailable. No replacement prompt will be sent.");
    this.child.stdin.write(JSON.stringify(message) + "\n");
  }
  private rpc(method: string, params: RecordValue): Promise<RecordValue> {
    const id = ++this.sequence;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      void this.send({ jsonrpc: "2.0", id, method, params }).catch((error: Error) => {
        this.pending.delete(id);
        reject(error);
      });
    });
  }
  private onHook(socket: NodeNet.Socket) {
    const lines = NodeReadline.createInterface({ input: socket });
    lines.once("line", (line) => {
      try {
        const data = record(JSON.parse(line));
        void NodeFSP.appendFile(NodePath.join(this.state, "hooks.jsonl"), line + "\n").catch(() =>
          this.fail("Hook evidence could not be saved."),
        );
        if (data.hook_event_name === "PreToolUse") {
          const response = this.cancelRequested
            ? {
                decision: "block",
                reason:
                  "Task cancellation requested. Stop work, report partial results, and let the Stop hook wait.",
              }
            : {};
          socket.end(JSON.stringify(response) + "\n");
          return;
        }
        const promptId = String(data.prompt_id ?? "");
        if (!promptId || (this.promptId && this.promptId !== promptId)) {
          this.fail("Native prompt identity changed or is missing. Hook remains blocked.");
          return;
        }
        this.promptId = promptId;
        if (data.hook_event_name === "PostCompaction") {
          const summary = String(data.summary ?? "");
          if (this.maintenance === "fresh" && !summary.startsWith("Fresh task context.")) {
            this.fail("Native clear was not confirmed. Hook remains blocked.");
            return;
          }
          this.options.onEvent({ type: "context", mode: this.maintenance ?? "automatic", summary });
          this.maintenance = undefined;
          socket.end("{}\n");
          return;
        }
        if (data.hook_event_name !== "Stop") {
          socket.end("{}\n");
          return;
        }
        if (this.phase === "ended" || this.waiting) {
          this.fail("Unexpected Stop hook; no continuation sent.");
          return;
        }
        this.waiting = socket;
        if (this.maintenance) {
          this.fail("Stop arrived before context maintenance was confirmed.");
          return;
        }
        this.phase = "waiting";
        if (this.firstTask !== undefined) {
          const text = this.firstTask;
          this.firstTask = undefined;
          void this.submit(text).catch((error: Error) => this.fail(error.message));
          return;
        }
        this.options.onEvent({
          type: "waiting",
          promptId,
          sessionId: String(data.session_id ?? this.sessionId),
          cancelled: this.cancelRequested,
        });
      } catch {
        this.fail("Malformed hook event; no continuation sent.");
      }
    });
  }
  async start() {
    if (!this.options.allowNativePrompt)
      throw new Error("Native Devin prompt is disabled. Enable only after approving a test run.");
    if (
      this.options.compactionThresholdTokens !== undefined &&
      (!Number.isSafeInteger(this.options.compactionThresholdTokens) ||
        this.options.compactionThresholdTokens <= 0)
    )
      throw new Error("Compaction threshold must be a positive whole number.");
    if (this.phase !== "new") throw new Error("This native run cannot be restarted.");
    // Exclusive directory ownership also prevents attaching to an existing Devin worker.
    await NodeFSP.mkdir(this.state, { mode: 0o700 });
    this.server = NodeNet.createServer((socket) => this.onHook(socket));
    await new Promise<void>((resolve, reject) => {
      this.server!.once("error", reject);
      this.server!.listen(this.socketPath, resolve);
    });
    await NodeFSP.chmod(this.socketPath, 0o600);
    const quote = (value: string) => "'" + value.replaceAll("'", "'\\''") + "'";
    const command = [
      process.execPath,
      NodeURL.fileURLToPath(new URL("./hook.mjs", import.meta.url)),
      this.socketPath,
    ]
      .map(quote)
      .join(" ");
    const configPath = NodePath.join(this.state, "config.json");
    await NodeFSP.writeFile(
      configPath,
      JSON.stringify({
        auto_update: false,
        subagents_enabled: false,
        ...(this.options.compactionThresholdTokens === undefined
          ? {}
          : { agent: { compaction_threshold_tokens: this.options.compactionThresholdTokens } }),
        hooks: Object.fromEntries(
          ["Stop", "PostCompaction", "PreToolUse"].map((name) => [
            name,
            [{ hooks: [{ type: "command", command, timeout: 315360000 }] }],
          ]),
        ),
      }),
      { mode: 0o600 },
    );
    this.child = NodeChildProcess.spawn(
      this.options.binary,
      [...(this.options.args ?? []), "--config", configPath, "acp", "--model", this.options.model],
      {
        cwd: this.options.cwd,
        detached: true,
        env: this.options.environment ?? process.env,
        stdio: "pipe",
      },
    );
    this.child.on("error", (error) => this.fail(error.message));
    this.child.on("exit", () => this.fail("Native process ended. No replacement prompt was sent."));
    this.child.stderr.on("data", (data: Buffer) => {
      void NodeFSP.appendFile(NodePath.join(this.state, "stderr.log"), data).catch(() => {});
    });
    NodeReadline.createInterface({ input: this.child.stdout }).on("line", (line) => {
      try {
        const message = record(JSON.parse(line));
        void NodeFSP.appendFile(
          NodePath.join(this.state, "protocol.jsonl"),
          JSON.stringify({ direction: "received", message }) + "\n",
        ).catch(() => this.fail("Protocol evidence could not be saved."));
        if (typeof message.id === "number" && this.pending.has(message.id) && !message.method) {
          const request = this.pending.get(message.id)!;
          this.pending.delete(message.id);
          if (message.error) request.reject(new Error(JSON.stringify(message.error)));
          else request.resolve(record(message.result));
        } else if (message.method === "session/update") {
          const update = record(record(message.params).update);
          const content = record(update.content);
          if (
            update.sessionUpdate === "agent_message_chunk" &&
            content.type === "text" &&
            this.phase === "working"
          ) {
            this.options.onEvent({ type: "text", text: String(content.text ?? "") });
          }
        } else if (message.method && message.id !== undefined) {
          // MVP never silently grants a permission requested by the native agent.
          const response =
            message.method === "session/request_permission"
              ? { result: { outcome: { outcome: "cancelled" } } }
              : { error: { code: -32601, message: "Unsupported client request" } };
          void this.send({ jsonrpc: "2.0", id: message.id, ...response }).catch((error: Error) =>
            this.fail(error.message),
          );
        }
      } catch {
        this.fail("Malformed native ACP message.");
      }
    });
    await this.rpc("initialize", {
      protocolVersion: 1,
      clientCapabilities: {},
      clientInfo: { name: "t3-devin", version: "0.1" },
    });
    const session = await this.rpc("session/new", { cwd: this.options.cwd, mcpServers: [] });
    const model =
      (Array.isArray(session.configOptions) ? session.configOptions : [])
        .map(record)
        .find((option) => option.id === "model")?.currentValue ??
      record(session.models).currentModelId;
    if (model !== this.options.model)
      throw new Error(
        `Selected model ${String(model)} differs from ${this.options.model}; no prompt sent.`,
      );
    if (typeof session.sessionId !== "string") throw new Error("ACP did not return a session ID.");
    this.sessionId = session.sessionId;
  }
  async submit(text: string) {
    if (!text.trim()) throw new Error("Message is empty.");
    if (!this.sessionId || !["new", "waiting"].includes(this.phase))
      throw new Error("Devin is busy or its native turn ended.");
    this.cancelRequested = false;
    if (!this.promptSent) {
      this.promptSent = true;
      this.firstTask = text;
      this.phase = "bootstrapping";
      void this.rpc("session/prompt", {
        sessionId: this.sessionId,
        prompt: [
          {
            type: "text",
            text: "Reply READY and let the Stop hook wait. Tasks arrive through that hook. Complete each task, then let the Stop hook wait again. Never start another prompt or poll for work. Do not recover earlier tasks after a context clear.",
          },
        ],
      }).then(
        () => this.fail("Original native prompt completed. No replacement was sent."),
        (error: Error) => this.fail(error.message),
      );
    } else {
      if (!this.waiting) throw new Error("The Stop hook is not waiting.");
      this.phase = "working";
      this.waiting.end(JSON.stringify({ decision: "block", reason: text }) + "\n");
      this.waiting = undefined;
    }
  }
  async context(mode: "compact" | "fresh") {
    if (this.phase !== "waiting" || !this.waiting)
      throw new Error("Context actions require the waiting Stop hook.");
    this.cancelRequested = false;
    if (mode === "fresh") await NodeFSP.writeFile(NodePath.join(this.state, "fresh.request"), "");
    await NodeFSP.writeFile(NodePath.join(this.state, "compact.request"), "");
    this.maintenance = mode;
    this.phase = "context";
    this.waiting.end(
      JSON.stringify({
        decision: "block",
        reason:
          "Perform pending context maintenance, then reply READY and let the Stop hook wait. Do not begin a task.",
      }) + "\n",
    );
    this.waiting = undefined;
  }
  softCancel() {
    if (!["working", "bootstrapping"].includes(this.phase) || this.cancelRequested) return;
    this.cancelRequested = true;
    if (this.phase === "bootstrapping") this.firstTask = undefined;
    this.options.onEvent({ type: "cancel-requested" });
  }
  close() {
    this.phase = "ended";
    if (this.child?.pid) {
      // This process group was created by this instance; includes its waiting hook.
      try {
        process.kill(-this.child.pid, "SIGTERM");
      } catch {
        /* Already exited. */
      }
    }
    this.waiting?.destroy();
    this.server?.close();
  }
}
