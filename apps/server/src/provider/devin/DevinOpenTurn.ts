import { HostProcessPlatform, HostProcessArchitecture } from "@t3tools/shared/hostProcess";
import { buildDevinConfig, verifyDevinBinary } from "./DevinLaunchConfig.ts";
import * as NodeReadline from "node:readline";
import * as NodeNet from "node:net";
import * as NodeChildProcess from "node:child_process";
import { hookSource } from "./hookSource.ts";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeFSP from "node:fs/promises";
import * as NodeCrypto from "node:crypto";

import * as Schema from "effect/Schema";
import * as AcpSchema from "effect-acp/schema";
import type { ProviderApprovalDecision, ProviderApprovalOption } from "@t3tools/contracts";

const decodePermissionRequest = Schema.decodeUnknownSync(AcpSchema.RequestPermissionRequest);

export type DevinEvent =
  | {
      type: "permission";
      requestId: string;
      request: AcpSchema.RequestPermissionRequest;
      options: ReadonlyArray<ProviderApprovalOption>;
    }
  | {
      type: "permission-resolved";
      requestId: string;
      request: AcpSchema.RequestPermissionRequest;
      decision: ProviderApprovalDecision;
    }
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
  private maintenance: "compact" | undefined;
  private pending = new Map<
    number,
    { resolve: (value: RecordValue) => void; reject: (e: Error) => void }
  >();
  private permissions = new Map<
    string,
    {
      id: string | number;
      request: AcpSchema.RequestPermissionRequest;
      choices: Map<ProviderApprovalDecision, AcpSchema.PermissionOption>;
    }
  >();
  private sequence = 0;
  private promptSent = false;
  readonly runId = NodeCrypto.randomUUID();
  private hookToken = NodeCrypto.randomBytes(32).toString("hex");
  private state: string;
  private socketPath = NodePath.join(NodeOS.tmpdir(), `t3-devin-${NodeCrypto.randomUUID()}.sock`);
  private options: {
    cwd: string;
    runRoot: string;
    threadId: string;
    providerInstanceId: string;
    host?: { platform: NodeJS.Platform; arch: NodeJS.Architecture };
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
    this.state = NodePath.join(options.runRoot, this.runId);
  }

  private fail(message: string) {
    this.phase = "ended";
    this.clearPermissions();
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
        const envelope = record(JSON.parse(line));
        if (envelope.token !== this.hookToken) {
          socket.destroy();
          return;
        }
        const data = record(envelope.event);
        if (this.phase === "ended") return;
        if (
          data.session_id !== this.sessionId ||
          !data.prompt_id ||
          (this.promptId && data.prompt_id !== this.promptId)
        ) {
          this.fail("Native hook identity does not match this run. No continuation sent.");
          return;
        }
        void NodeFSP.appendFile(
          NodePath.join(this.state, "hooks.jsonl"),
          JSON.stringify(data) + "\n",
        ).catch(() => this.fail("Hook evidence could not be saved."));
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
          this.options.onEvent({ type: "context", mode: this.maintenance ?? "automatic", summary });
          this.maintenance = undefined;
          socket.end("{}\n");
          return;
        }
        if (data.hook_event_name !== "Stop") {
          socket.end("{}\n");
          return;
        }
        if (this.waiting) {
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
    if (!this.options.args)
      await verifyDevinBinary(
        this.options.binary,
        this.options.host ?? {
          platform: HostProcessPlatform.defaultValue(),
          arch: HostProcessArchitecture.defaultValue(),
        },
      );
    // Never share control state or infer run identity from the project directory.
    if (!NodePath.isAbsolute(this.options.runRoot))
      throw new Error("Devin run root must be absolute.");
    await NodeFSP.mkdir(this.options.runRoot, { recursive: true, mode: 0o700 });
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
      NodePath.join(this.state, "hook.mjs"),
      this.socketPath,
      this.hookToken,
    ]
      .map(quote)
      .join(" ");
    await NodeFSP.writeFile(NodePath.join(this.state, "hook.mjs"), hookSource, { mode: 0o600 });
    const configPath = NodePath.join(this.state, "config.json");
    await NodeFSP.writeFile(
      configPath,
      JSON.stringify(
        await buildDevinConfig({
          cwd: this.options.cwd,
          environment: this.options.environment ?? process.env,
          hookCommand: command,
          model: this.options.model,
          compactionThresholdTokens: this.options.compactionThresholdTokens,
        }),
      ),
      { mode: 0o600 },
    );
    const launchEnvironment = { ...(this.options.environment ?? process.env) };
    for (const key of Object.keys(launchEnvironment)) {
      if (key.startsWith("DEVIN_CONTROL_")) delete launchEnvironment[key];
    }
    Object.assign(launchEnvironment, {
      DEVIN_CONTROL_DIR: this.state,
      DEVIN_CONTROL_RUN_ID: this.runId,
      DEVIN_CONTROL_ABI: "1",
    });
    // exec preserves the shell PID; inherited child settings retain that owner's PID.
    this.child = NodeChildProcess.spawn(
      "/bin/sh",
      [
        "-c",
        'export DEVIN_CONTROL_OWNER_PID=$$; exec "$@"',
        "t3-devin",
        this.options.binary,
        ...(this.options.args ?? []),
        "--config",
        configPath,
        "acp",
        "--model",
        this.options.model,
      ],
      {
        cwd: this.options.cwd,
        detached: true,
        env: launchEnvironment,
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
          const params = record(message.params);
          // Devin sends config updates before session/new returns its identity.
          if (!this.sessionId) return;
          if (params.sessionId !== this.sessionId) {
            this.fail("Native update identity does not match this run.");
            return;
          }
          const update = record(params.update);
          const content = record(update.content);
          if (
            update.sessionUpdate === "agent_message_chunk" &&
            content.type === "text" &&
            this.phase === "working"
          ) {
            this.options.onEvent({ type: "text", text: String(content.text ?? "") });
          }
        } else if (message.method && message.id !== undefined) {
          if (message.method === "session/request_permission") {
            const request = decodePermissionRequest(message.params);
            if (typeof message.id !== "string" && typeof message.id !== "number")
              throw new Error("Invalid permission request ID.");
            if (
              request.sessionId !== this.sessionId ||
              this.cancelRequested ||
              this.phase === "ended"
            ) {
              void this.send({
                jsonrpc: "2.0",
                id: message.id,
                result: { outcome: { outcome: "cancelled" } },
              }).catch((error: Error) => this.fail(error.message));
            } else {
              const requestId = NodeCrypto.randomUUID();
              const choices = new Map<ProviderApprovalDecision, AcpSchema.PermissionOption>();
              for (const option of request.options) {
                const decision =
                  option.kind === "allow_once"
                    ? "accept"
                    : option.kind === "allow_always"
                      ? "acceptAlways"
                      : option.kind === "reject_once"
                        ? "decline"
                        : undefined;
                if (decision && option.optionId.trim() && !choices.has(decision))
                  choices.set(decision, option);
              }
              this.permissions.set(requestId, { id: message.id, request, choices });
              this.options.onEvent({
                type: "permission",
                requestId,
                request,
                options: [
                  ...Array.from(choices, ([decision, option]) => ({
                    decision,
                    label: option.name.trim() || decision,
                  })),
                  { decision: "cancel", label: "Cancel" },
                ],
              });
            }
          } else {
            void this.send({
              jsonrpc: "2.0",
              id: message.id,
              error: { code: -32601, message: "Unsupported client request" },
            }).catch((error: Error) => this.fail(error.message));
          }
        }
      } catch {
        this.fail("Malformed native ACP message.");
      }
    });
    await NodeFSP.writeFile(
      NodePath.join(this.state, "identity.json"),
      JSON.stringify({
        runId: this.runId,
        threadId: this.options.threadId,
        providerInstanceId: this.options.providerInstanceId,
        cwd: this.options.cwd,
        pid: this.child.pid,
        status: "starting",
      }),
      { mode: 0o600 },
    );
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
    await NodeFSP.writeFile(
      NodePath.join(this.state, "identity.json"),
      JSON.stringify({
        runId: this.runId,
        threadId: this.options.threadId,
        providerInstanceId: this.options.providerInstanceId,
        cwd: this.options.cwd,
        pid: this.child.pid,
        sessionId: this.sessionId,
        status: "connected",
      }),
      { mode: 0o600 },
    );
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
            text: "Reply READY and let the Stop hook wait. Tasks arrive through that hook. Complete each task, then let the Stop hook wait again. Never start another prompt or poll for work.",
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
  async context(mode: "compact") {
    if (this.phase !== "waiting" || !this.waiting)
      throw new Error("Context actions require the waiting Stop hook.");
    this.cancelRequested = false;
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
  async respondToPermission(requestId: string, decision: ProviderApprovalDecision) {
    const pending = this.permissions.get(requestId);
    if (!pending) throw new Error("This approval request is no longer pending.");
    const option = pending.choices.get(decision);
    if (decision !== "cancel" && !option)
      throw new Error("Devin did not offer this permission choice.");
    this.permissions.delete(requestId);
    try {
      await this.send({
        jsonrpc: "2.0",
        id: pending.id,
        result: {
          outcome: option
            ? { outcome: "selected", optionId: option.optionId }
            : { outcome: "cancelled" },
        },
      });
    } catch (error) {
      this.options.onEvent({
        type: "permission-resolved",
        requestId,
        request: pending.request,
        decision: "cancel",
      });
      this.fail(error instanceof Error ? error.message : String(error));
      throw error;
    }
    this.options.onEvent({
      type: "permission-resolved",
      requestId,
      request: pending.request,
      decision,
    });
  }
  private clearPermissions() {
    for (const [requestId, pending] of this.permissions) {
      this.options.onEvent({
        type: "permission-resolved",
        requestId,
        request: pending.request,
        decision: "cancel",
      });
    }
    this.permissions.clear();
  }
  softCancel() {
    if (!["working", "bootstrapping"].includes(this.phase) || this.cancelRequested) return;
    this.cancelRequested = true;
    for (const requestId of this.permissions.keys()) {
      void this.respondToPermission(requestId, "cancel").catch(() => {});
    }
    if (this.phase === "bootstrapping") this.firstTask = undefined;
    this.options.onEvent({ type: "cancel-requested" });
  }
  close() {
    this.phase = "ended";
    this.clearPermissions();
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
