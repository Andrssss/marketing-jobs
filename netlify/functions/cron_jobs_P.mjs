export const config = {
  schedule: "37 9-22 * * *",
};

import { withTimeout } from "./_error-logger.mjs";

/**
 * Unified Profession dispatcher.
 *
 * Each task scrapes one Profession.hu listing URL via a single background
 * invocation. Background functions have a 15 min limit, which is enough for
 * full pagination of any single listing.
 */
const TASKS = [
  // P_1 – Marketing / junior marketing / marketing asszisztens / brand manager
  { jobName: "P_1_a", label: "Profession – Marketing/Media/PR",            url: "https://www.profession.hu/allasok/marketing-media-pr/budapest/1,12,23,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,1" },
  { jobName: "P_1_b", label: "Profession – Junior marketing (1)",          url: "https://www.profession.hu/allasok/budapest/1,0,23,junior%20marketing,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { jobName: "P_1_c", label: "Profession – Junior marketing (5/3)",        url: "https://www.profession.hu/allasok/budapest/1,0,23,junior%20marketing,0,0,0,5,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { jobName: "P_1_d", label: "Profession – Junior marketing (3/3)",        url: "https://www.profession.hu/allasok/budapest/1,0,23,junior%20marketing,0,0,0,3,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { jobName: "P_1_e", label: "Profession – Marketing asszisztens (1)",     url: "https://www.profession.hu/allasok/budapest/1,0,23,marketing%20asszisztens,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { jobName: "P_1_f", label: "Profession – Marketing asszisztens (3)",     url: "https://www.profession.hu/allasok/budapest/1,0,23,marketing%20asszisztens,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { jobName: "P_1_g", label: "Profession – Junior brand manager",          url: "https://www.profession.hu/allasok/budapest/1,0,23,junior%20brand%20manager,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },

  // P_2 – Irodai (1) + piackutató
  { jobName: "P_2_a", label: "Profession – Irodai adminisztrátor (1)",     url: "https://www.profession.hu/allasok/budapest/1,0,23,irodai%20adminisztr%c3%a1tor,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { jobName: "P_2_b", label: "Profession – Irodai asszisztens (1)",        url: "https://www.profession.hu/allasok/budapest/1,0,23,irodai%20asszisztens,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { jobName: "P_2_c", label: "Profession – Irodai munkatárs (1)",          url: "https://www.profession.hu/allasok/budapest/1,0,23,irodai%20munkat%c3%a1rs,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { jobName: "P_2_d", label: "Profession – Irodai ügyintéző (1)",          url: "https://www.profession.hu/allasok/budapest/1,0,23,irodai%20%c3%bcgyint%c3%a9z%c5%91,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { jobName: "P_2_e", label: "Profession – Piackutató",                    url: "https://www.profession.hu/allasok/budapest/1,0,23,piackutat%c3%b3%401%401?keywordsearch" },
  { jobName: "P_2_f", label: "Profession – Piackutatási elemző",           url: "https://www.profession.hu/allasok/budapest/1,0,23,piackutat%c3%a1si%20elemz%c5%91%401%401?keywordsearch" },

  // P_3..P_8 – egy-egy listázás
  { jobName: "P_3",   label: "Profession – Irodai adminisztrátor (3)",     url: "https://www.profession.hu/allasok/budapest/1,0,23,irodai%20adminisztr%c3%a1tor,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { jobName: "P_4",   label: "Profession – Irodai asszisztens (3)",        url: "https://www.profession.hu/allasok/budapest/1,0,23,irodai%20asszisztens,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { jobName: "P_5",   label: "Profession – Irodai munkatárs (3)",          url: "https://www.profession.hu/allasok/budapest/1,0,23,irodai%20munkat%c3%a1rs,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { jobName: "P_6",   label: "Profession – Irodai ügyintéző (3)",          url: "https://www.profession.hu/allasok/budapest/1,0,23,irodai%20%c3%bcgyint%c3%a9z%c5%91,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { jobName: "P_7",   label: "Profession – Marketing keyword",             url: "https://www.profession.hu/allasok/budapest/1,0,23,marketing,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { jobName: "P_8",   label: "Profession – Junior marketing (3)",          url: "https://www.profession.hu/allasok/budapest/1,0,23,junior%20marketing,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
];

export default withTimeout("cron_jobs_P", async () => {
  const siteUrl = process.env.URL;
  const secret = process.env.CRON_SECRET;

  if (!siteUrl || !secret) {
    console.warn("[cron_jobs_P] URL or CRON_SECRET not set, cannot trigger background functions");
    return new Response("Missing env vars", { status: 500 });
  }

  await Promise.all(
    TASKS.map((task) =>
      fetch(`${siteUrl}/.netlify/functions/cron_jobs_P-background`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${secret}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(task),
      })
        .then(() => console.log(`[cron_jobs_P] triggered ${task.jobName}`))
        .catch((err) => console.error(`[cron_jobs_P] failed to trigger ${task.jobName}: ${err.message}`))
    )
  );

  return new Response(`Triggered ${TASKS.length} background invocations`, { status: 200 });
});
