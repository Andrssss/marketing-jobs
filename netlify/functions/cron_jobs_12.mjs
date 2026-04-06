export const config = {
  schedule: "22 4-23 * * *",
};

/* =========================
   EXPERIENCE ENRICHMENT for aam/karrierhungaria
   → MOVED to cron_experience.mjs (unified pipeline)
   ========================= */

export default async () => {
  return new Response("OK – moved to cron_experience.mjs");
};
