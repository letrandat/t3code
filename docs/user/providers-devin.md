# Devin

This first release supports a separately built private-control Devin binary on macOS ARM64. Build Devin 3000.10.21 with `patch_t3.py` from [devin-continuous](https://github.com/letrandat/devin-continuous), keeping its generated manifest next to the binary. In **Settings > Providers**, add Devin, select that binary path and model, and enable native prompts. The existing pool installation can stay in place.

T3 discovers the models offered to your Devin account on the connected server. In **Settings > Providers > Devin > Models**, show or hide models in the picker using the existing visibility controls. Visibility preferences are saved on this device. Settings supplies the default exact model ID; the chat picker can select another allowed model for a new thread. If discovery fails, new threads are blocked. Use **Settings > Providers > Refresh provider status** to retry.

Each T3 thread starts its own Devin run in the actual project directory. Replies reuse that run's open native prompt. Two threads can share a project, but they still edit the same files. Their control files, hooks and logs live in separate folders under the T3 environment's state directory.

Use **Stop** to request a soft cancel. Devin finishes a running tool and blocks the next tool call before returning to its waiting hook. Plain Escape retains its normal composer behavior. Send another message to continue the same native prompt. Send `/compact` while waiting to compact that prompt. `/clear` is not supported. Select a model and its reasoning or service tier before your first message. Changing any of these after the conversation starts requires a new thread.

Attach files or images in the composer, with or without a text message. T3 saves uploads on the connected server and gives Devin file paths to read with its tools. Images are file references, not embedded image content in the native prompt.

Permission requests follow the thread's permission mode. **Full access** auto-approves offered allow options. **Auto-accept edits** auto-approves file changes and still asks for other actions. Other modes wait for you to choose one of Devin's offered options. Switching modes mid-thread applies to later requests without a new thread; only model changes need one. Stop cancels a pending permission request; ending the session clears it.

T3 copies ordinary user settings into each run's configuration and replaces user hooks with its managed hooks. It leaves original settings and `AGENTS.md` files unchanged. Projects with Devin hooks in `.devin/config.json`, `.devin/config.local.json` or `.devin/hooks.v1.json`, including ancestor directories, are refused before a prompt starts. Project hook replacement is outside this first release. Do not enable additional plugin or organization hooks for this provider; those hook sources are not covered by the startup check. Separate MCP configuration remains subject to Devin's normal discovery.

T3 checks the binary manifest and checksum before launch. A failed or ended native run is not silently replaced. This version cannot recover its waiting connection after a T3 server restart or update. Finish active work first; [restart recovery is tracked separately](https://github.com/letrandat/t3code/issues/1).
