export const config = {
  schedule: "37 5 * * *",
};

import { processProfessionSources } from "./_profession_core.mjs";

const SOURCES = [
  { key: "profession-intern", label: "Profession – Marketing/Media/PR", url: "https://www.profession.hu/allasok/marketing-media-pr/budapest/1,12,23,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,1" },
  { key: "profession-intern", label: "Profession – Junior marketing (1)", url: "https://www.profession.hu/allasok/budapest/1,0,23,junior%20marketing,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { key: "profession-intern", label: "Profession – Junior marketing (5/3)", url: "https://www.profession.hu/allasok/budapest/1,0,23,junior%20marketing,0,0,0,5,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { key: "profession-intern", label: "Profession – Junior marketing (3/3)", url: "https://www.profession.hu/allasok/budapest/1,0,23,junior%20marketing,0,0,0,3,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { key: "profession-intern", label: "Profession – Marketing asszisztens (1)", url: "https://www.profession.hu/allasok/budapest/1,0,23,marketing%20asszisztens,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { key: "profession-intern", label: "Profession – Marketing asszisztens (3)", url: "https://www.profession.hu/allasok/budapest/1,0,23,marketing%20asszisztens,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { key: "profession-intern", label: "Profession – Junior brand manager", url: "https://www.profession.hu/allasok/budapest/1,0,23,junior%20brand%20manager,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
];

export default (request) =>
  processProfessionSources(SOURCES, "cron_jobs_P_1", request);
