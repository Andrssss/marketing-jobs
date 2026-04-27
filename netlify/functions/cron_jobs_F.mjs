export const config = {
  schedule: "23 9-22 * * *",
};

import { withTimeout } from "./_error-logger.mjs";

export default withTimeout("cron_jobs_F", async () => {
  const siteUrl = process.env.URL;

  if (!siteUrl) {
    console.warn("[cron_jobs_F] URL not set, cannot trigger background function");
    return new Response("Missing env vars", { status: 500 });
  }

  await fetch(`${siteUrl}/.netlify/functions/cron_jobs_F-background`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ startPage: 1 }),
  })
    .then(() => console.log("[cron_jobs_F] triggered background (pages 1–∞)"))
    .catch((err) => console.error(`[cron_jobs_F] failed to trigger background: ${err.message}`));

  return new Response("Background function triggered", { status: 200 });
});
