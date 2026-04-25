export const config = {
  schedule: "44 5 * * *",
};

import { processProfessionSources } from "./_profession_core.mjs";

const SOURCES = [
  { key: "profession-intern", label: "Profession – Junior marketing (3)", url: "https://www.profession.hu/allasok/budapest/1,0,23,junior%20marketing,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
];

export default (request) =>
  processProfessionSources(SOURCES, "cron_jobs_P_8", request);
