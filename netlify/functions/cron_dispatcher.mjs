export const config = {
  schedule: "19 9-22 * * *",
};

import { withTimeout } from "./_error-logger.mjs";

const TARGETS = [
  { name: "cron_jobs_M_D-background" },
  { name: "cron_jobs_BLUE-background" },
  { name: "cron_jobs_KH-background" },
  { name: "cron_jobs_T-background" },
  { name: "cron_jobs_C_1-background" },
  { name: "cron_jobs_W_N-background" },
];

export default withTimeout("cron_dispatcher", async () => {
  const siteUrl = process.env.URL;
  const secret = process.env.CRON_SECRET;

  if (!siteUrl) {
    console.warn("[cron_dispatcher] URL is not set");
    return new Response("Missing URL", { status: 500 });
  }

  await Promise.all(
    TARGETS.map((task) =>
      fetch(`${siteUrl}/.netlify/functions/${task.name}`, {
        method: "POST",
        headers: {
          ...(secret ? { Authorization: `Bearer ${secret}` } : {}),
          "Content-Type": "application/json",
        },
        body: "{}",
      })
        .then((res) => {
          if (!res.ok) {
            throw new Error(`HTTP ${res.status}`);
          }
          console.log(`[cron_dispatcher] triggered ${task.name}`);
        })
        .catch((err) => {
          console.error(`[cron_dispatcher] failed to trigger ${task.name}: ${err.message}`);
        })
    )
  );

  return new Response(`Triggered ${TARGETS.length} background invocations`, { status: 200 });
});