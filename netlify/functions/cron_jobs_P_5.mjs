export const config = {
  schedule: "41 5 * * *",
};

import { processProfessionSources } from "./_profession_core.mjs";

const SOURCES = [
  { key: "profession-intern", label: "Profession – Irodai munkatárs (3)", url: "https://www.profession.hu/allasok/budapest/1,0,23,irodai%20munkat%c3%a1rs,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
];

export default (request) =>
  processProfessionSources(SOURCES, "cron_jobs_P_5", request);
