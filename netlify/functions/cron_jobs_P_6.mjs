export const config = {
  schedule: "42 9-22 * * *",
};

import { processProfessionSources } from "./_profession_core.mjs";

const SOURCES = [
  { key: "profession-intern", label: "Profession – Irodai ügyintéző (3)", url: "https://www.profession.hu/allasok/budapest/1,0,23,irodai%20%c3%bcgyint%c3%a9z%c5%91,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
];

export default (request) =>
  processProfessionSources(SOURCES, "cron_jobs_P_6", request);
