export const config = {
  schedule: "38 9-22 * * *",
};

import { processProfessionSources } from "./_profession_core.mjs";

const SOURCES = [
  { key: "profession-intern", label: "Profession – Irodai adminisztrátor (1)", url: "https://www.profession.hu/allasok/budapest/1,0,23,irodai%20adminisztr%c3%a1tor,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { key: "profession-intern", label: "Profession – Irodai asszisztens (1)", url: "https://www.profession.hu/allasok/budapest/1,0,23,irodai%20asszisztens,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { key: "profession-intern", label: "Profession – Irodai munkatárs (1)", url: "https://www.profession.hu/allasok/budapest/1,0,23,irodai%20munkat%c3%a1rs,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { key: "profession-intern", label: "Profession – Irodai ügyintéző (1)", url: "https://www.profession.hu/allasok/budapest/1,0,23,irodai%20%c3%bcgyint%c3%a9z%c5%91,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1" },
  { key: "profession-intern", label: "Profession – Piackutató", url: "https://www.profession.hu/allasok/budapest/1,0,23,piackutat%c3%b3%401%401?keywordsearch" },
  { key: "profession-intern", label: "Profession – Piackutatási elemző", url: "https://www.profession.hu/allasok/budapest/1,0,23,piackutat%c3%a1si%20elemz%c5%91%401%401?keywordsearch" },
];

export default (request) =>
  processProfessionSources(SOURCES, "cron_jobs_P_2", request);
