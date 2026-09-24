import { loadConfig } from "./config/env.js";
import { run } from "./app/lifecycle.js";

async function main(): Promise<void> {
  const config = loadConfig();
  await run(config);
}

process.on("unhandledRejection", (reason) => {
  // eslint-disable-next-line no-console
  console.error("Unhandled rejection:", reason);
  process.exitCode = 1;
});

main().catch((err: unknown) => {
  // eslint-disable-next-line no-console
  console.error(err instanceof Error ? err.message : err);
  process.exitCode = 1;
});
