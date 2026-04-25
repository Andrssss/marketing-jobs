export const config = {
  schedule: "39 5 * * *",
};

import { processProfessionSources } from "./_profession_core.mjs";

const SOURCES = [
  { key: "profession-intern", label: "Profession – Irodai adminisztrátor (3)", url: "https://www.profession.hu/allasok/budapest/1,0,23,irodai%20adminisztr%c3%a1tor,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
];

export default (request) =>
  processProfessionSources(SOURCES, "cron_jobs_P_3", request);
