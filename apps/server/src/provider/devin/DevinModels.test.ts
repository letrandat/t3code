// @effect-diagnostics nodeBuiltinImport:off - Temporary executable fixtures exercise real process cleanup.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { describe, expect, it } from "vite-plus/test";
import {
  allowedDevinModels,
  buildDevinModelCatalog,
  discoverDevinModels,
  resolveDevinModel,
} from "./DevinModels.ts";

const families = {
  families: [
    {
      family_uid: "swe-1-6",
      family_label: "SWE-1.6",
      variants: [
        { model_uid: "swe-1-6-medium", label: "Medium" },
        { model_uid: "swe-1-6-high", label: "High" },
        { model_uid: "swe-1-6-high-priority", label: "High Fast" },
        { model_uid: "hidden-model", label: "Hidden" },
      ],
    },
  ],
};

const session = {
  configOptions: [
    {
      id: "model",
      currentValue: "swe-1-6-high",
      options: [
        { value: "swe-1-6-medium" },
        { value: "swe-1-6-high" },
        { value: "swe-1-6-high-priority" },
      ],
    },
  ],
};

describe("DevinModels", () => {
  it("joins allowed session IDs to catalog families and never invents native IDs", () => {
    const catalog = buildDevinModelCatalog(families, session, "swe-1-6-high");
    expect(catalog.defaultId).toBe("swe-1-6-high");
    expect(catalog.models).toHaveLength(1);
    expect(catalog.models[0]?.slug).toBe("swe-1-6");
    expect(catalog.models[0]?.name).toBe("SWE-1.6");
    expect(catalog.variants.get("swe-1-6")!.map((variant) => variant.id)).toEqual([
      "swe-1-6-medium",
      "swe-1-6-high",
      "swe-1-6-high-priority",
    ]);
    expect(resolveDevinModel(catalog, { model: "swe-1-6" })).toBe("swe-1-6-high");
    expect(
      resolveDevinModel(catalog, {
        model: "swe-1-6",
        options: [{ id: "reasoningEffort", value: "medium" }],
      }),
    ).toBe("swe-1-6-medium");
    expect(
      resolveDevinModel(catalog, {
        model: "swe-1-6",
        options: [
          { id: "reasoningEffort", value: "high" },
          { id: "serviceTier", value: "fast" },
        ],
      }),
    ).toBe("swe-1-6-high-priority");
    expect(() =>
      resolveDevinModel(catalog, {
        model: "swe-1-6",
        options: [
          { id: "reasoningEffort", value: "medium" },
          { id: "serviceTier", value: "fast" },
        ],
      }),
    ).toThrow(/combination is not allowed/);
    expect(() => resolveDevinModel(catalog, { model: "hidden-model" })).toThrow(/not allowed/);
    expect(() => buildDevinModelCatalog(families, session, "hidden-model")).toThrow(
      /no longer allowed/,
    );
    expect(allowedDevinModels(session).ids.has("hidden-model")).toBe(false);
  });

  it("discovers the fake peer catalog without sending a prompt", async () => {
    const cwd = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-devin-models-"));
    const binaryPath = NodePath.join(cwd, "fake-devin");
    const fake = NodeURL.fileURLToPath(new URL("./testFixtures/fakeDevin.mjs", import.meta.url));
    await NodeFSP.writeFile(binaryPath, `#!/bin/sh\nexec '${process.execPath}' '${fake}' "$@"\n`, {
      mode: 0o700,
    });
    const binaryBytes = await NodeFSP.readFile(binaryPath);
    await NodeFSP.writeFile(
      `${binaryPath}.manifest.json`,
      JSON.stringify({
        control_abi: 1,
        owner_check: "pid",
        clear: false,
        capabilities: ["private-control-directory", "owner-pid", "compact"],
        output_sha256: NodeCrypto.createHash("sha256").update(binaryBytes).digest("hex"),
      }),
    );
    const catalog = await discoverDevinModels(
      binaryPath,
      { HOME: cwd, PATH: process.env.PATH },
      "fake",
    );
    expect(catalog.defaultId).toBe("fake");
    expect(catalog.models.map((model) => model.slug)).toEqual(["fake"]);
    expect(resolveDevinModel(catalog, { model: "fake" })).toBe("fake");
  });
});
