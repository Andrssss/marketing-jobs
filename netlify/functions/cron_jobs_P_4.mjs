export const config = {
  schedule: "40 5 * * *",
};

import { processProfessionSources } from "./_profession_core.mjs";

const SOURCES = [
  { key: "profession-intern", label: "Profession – Irodai asszisztens (3)", url: "https://www.profession.hu/allasok/budapest/1,0,23,irodai%20asszisztens,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
];

export default (request) =>
  processProfessionSources(SOURCES, "cron_jobs_P_4", request);
