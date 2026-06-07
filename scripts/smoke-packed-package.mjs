import { spawn } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const workdir = await mkdtemp(
  join(tmpdir(), "chat-state-better-sqlite3-smoke-")
);

try {
  const pack = await run(
    "npm",
    ["pack", root, "--pack-destination", workdir, "--json"],
    { cwd: root }
  );
  const jsonStart = pack.stdout.search(/^\[/m);
  const jsonText = jsonStart === -1 ? pack.stdout : pack.stdout.slice(jsonStart);
  const [{ filename }] = JSON.parse(jsonText);
  const tarball = join(workdir, filename);

  await writeFile(
    join(workdir, "package.json"),
    JSON.stringify({ private: true, type: "module" }, null, 2)
  );
  await run(
    "npm",
    ["install", "--silent", tarball, "chat@4.30.0", "typescript@6.0.3"],
    { cwd: workdir }
  );

  const smokeTypeFile = join(workdir, "smoke.ts");
  await writeFile(
    smokeTypeFile,
    `
      import { createBetterSqlite3State } from "chat-state-better-sqlite3";

      const state = createBetterSqlite3State({ enableWal: false, path: ":memory:" });
      await state.connect();
      await state.set("ok", { value: true });
      const value = await state.get<{ value: boolean }>("ok");
      await state.disconnect();

      if (value?.value !== true) {
        throw new Error("packed package state adapter smoke failed");
      }
    `
  );
  const smokeRuntimeFile = join(workdir, "smoke.mjs");
  await writeFile(
    smokeRuntimeFile,
    `
      import { createBetterSqlite3State } from "chat-state-better-sqlite3";

      const state = createBetterSqlite3State({ enableWal: false, path: ":memory:" });
      await state.connect();
      await state.set("ok", { value: true });
      const value = await state.get("ok");
      await state.disconnect();

      if (value?.value !== true) {
        throw new Error("packed package state adapter smoke failed");
      }
    `
  );
  await writeFile(
    join(workdir, "tsconfig.json"),
    JSON.stringify(
      {
        compilerOptions: {
          module: "NodeNext",
          moduleResolution: "NodeNext",
          noEmit: true,
          strict: true,
          target: "ES2023",
        },
        include: ["smoke.ts"],
      },
      null,
      2
    )
  );
  await run("npx", ["tsc", "--noEmit"], { cwd: workdir });
  await run("node", [smokeRuntimeFile], { cwd: workdir });
} finally {
  await rm(workdir, { force: true, recursive: true });
}

async function run(command, args, options) {
  return await new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      ...options,
      env: {
        ...process.env,
        npm_config_dry_run: "false",
      },
      shell: process.platform === "win32",
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";

    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolve({ stdout, stderr });
      } else {
        reject(
          new Error(
            `${command} ${args.join(" ")} failed with exit ${code}\n${stderr}`
          )
        );
      }
    });
  });
}
