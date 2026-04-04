export const config = {
  schedule: "12 4-23 * * *",
};

/* =========================
  DreamJobs: category=52 (Marketing, PR), location=2925 (Budapest)
  MelonJobs: job-categories=91 (Adminisztrátor, Dokumentumkezelő), 196 (Marketing, Média, PR vezető)
*/


import { Pool } from "pg";
import https from "https";
import http from "http";
import zlib from "zlib";
import { load as cheerioLoad } from "cheerio";
import { loadFilters } from "./load_filters.mjs";

let _filters = [];

const connectionString = process.env.NETLIFY_DATABASE_URL;
if (!connectionString) throw new Error("NETLIFY_DATABASE_URL is not set");

const pool = new Pool({
  connectionString,
  ssl: { rejectUnauthorized: false },
});

const MAX_PAGES = 20;

const DREAMJOBS_API_URLS = [
  "https://api.dreamjobs.hu/api/v1/jobs?region=hu&page=1&tags%5Bjob-category%5D%5B%5D=52&tags%5Boffice-location%5D%5B%5D=2925&scope%5B%5D=isNotBlue&per_page=50",
];

const MELONJOBS_API_URL =
  "https://melonjobs.hu/wp-json/wp/v2/job-listings?job-categories=91,196&per_page=100&page=1";

/* ── shared helpers ─────────────────────────────────────────── */

function normalizeWhitespace(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function normalizeText(value) {
  return String(value ?? "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function normalizeUrl(raw) {
  try {
    const url = new URL(raw);
    url.hash = "";
    ["utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "fbclid", "gclid"].forEach((key) =>
      url.searchParams.delete(key)
    );
    return url.toString().replace(/\?$/, "");
  } catch {
    return raw;
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function fetchText(url, redirectLeft = 5) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(url);
    const lib = parsedUrl.protocol === "https:" ? https : http;

    const req = lib.request(
      parsedUrl,
      {
        method: "GET",
        headers: {
          "User-Agent": "JobWatcher/1.0",
          Accept: "application/json,text/plain;q=0.9,*/*;q=0.8",
          "Accept-Language": "hu-HU,hu;q=0.9,en;q=0.8",
          "Accept-Encoding": "gzip,deflate,br",
        },
        timeout: 50000,
      },
      (res) => {
        const code = res.statusCode || 0;

        if ([301, 302, 303, 307, 308].includes(code)) {
          const location = res.headers.location;
          if (!location) return reject(new Error(`HTTP ${code} (no Location) for ${url}`));
          if (redirectLeft <= 0) return reject(new Error(`Too many redirects for ${url}`));
          const nextUrl = new URL(location, url).toString();
          res.resume();
          return resolve(fetchText(nextUrl, redirectLeft - 1));
        }

        const encoding = String(res.headers["content-encoding"] || "").toLowerCase();
        let stream = res;

        if (encoding.includes("gzip")) stream = res.pipe(zlib.createGunzip());
        else if (encoding.includes("deflate")) stream = res.pipe(zlib.createInflate());
        else if (encoding.includes("br")) stream = res.pipe(zlib.createBrotliDecompress());

        let body = "";
        stream.setEncoding("utf8");
        stream.on("data", (chunk) => {
          body += chunk;
        });
        stream.on("end", () => {
          if (code >= 200 && code < 300) resolve(body);
          else reject(new Error(`HTTP ${code} for ${url}`));
        });
        stream.on("error", reject);
      }
    );

    req.on("timeout", () => req.destroy(new Error(`Timeout for ${url}`)));
    req.on("error", reject);
    req.end();
  });
}

async function fetchJson(url) {
  const text = await fetchText(url);
  return JSON.parse(text);
}

function htmlToText(html) {
  const $ = cheerioLoad(`<div>${html ?? ""}</div>`);
  return normalizeWhitespace($.text());
}

async function upsertJob(client, sourceKey, item) {
  await client.query(
    `INSERT INTO marketing_job_posts
      (source, title, url, experience, first_seen)
     VALUES ($1,$2,$3,$4,NOW())
     ON CONFLICT (source, url) WHERE url IS NOT NULL
     DO UPDATE SET
       title = EXCLUDED.title,
       experience = COALESCE(EXCLUDED.experience, marketing_job_posts.experience);`,
    [sourceKey, item.title, item.url, item.experience ?? "-"]
  );
}

async function deleteExistingBlacklistedDreamJobs(client) {
  const { rowCount } = await client.query(
    `DELETE FROM marketing_job_posts
      WHERE source = 'dreamjobs'
        AND EXISTS (
          SELECT 1 FROM marketing_filters f
          WHERE LOWER(marketing_job_posts.title) LIKE '%' || LOWER(f.word) || '%'
        );`
  );

  return rowCount || 0;
}

/* ── DreamJobs ──────────────────────────────────────────────── */

function buildDreamJobsUrl(job) {
  const lang = /^[a-z]{2}$/i.test(String(job?.primary_lang || "")) ? String(job.primary_lang).toLowerCase() : "hu";
  const companySlug = normalizeWhitespace(job?.company?.slug);
  const localizedSlug =
    normalizeWhitespace(job?.slugs?.[`slug_${lang}`]) ||
    normalizeWhitespace(job?.slugs?.slug_hu) ||
    normalizeWhitespace(job?.slug);

  if (!companySlug || !localizedSlug) return null;

  return normalizeUrl(`https://dreamjobs.hu/${lang}/job/${companySlug}/${localizedSlug}`);
}

function pickJobTitle(job) {
  const lang = /^[a-z]{2}$/i.test(String(job?.primary_lang || "")) ? String(job.primary_lang).toLowerCase() : "hu";
  return normalizeWhitespace(job?.name?.[lang]) || normalizeWhitespace(job?.name?.hu) || normalizeWhitespace(job?.name?.en) || null;
}

function extractDreamJobs(payload) {
  const rows = Array.isArray(payload?.data) ? payload.data : [];

  return rows
    .map((job) => ({
      title: pickJobTitle(job),
      url: buildDreamJobsUrl(job),
      experience: normalizeWhitespace(job?.tags?.job_level?.slug) || null,
    }))
    .filter((item) => item.title && item.url);
}

async function fetchAllDreamJobs() {
  const jobs = [];
  const seen = new Set();

  for (const apiUrl of DREAMJOBS_API_URLS) {
    const baseUrl = new URL(apiUrl);
    const perPage = Number.parseInt(baseUrl.searchParams.get("per_page") || "50", 10) || 50;

    for (let page = 1; page <= MAX_PAGES; page += 1) {
      baseUrl.searchParams.set("page", String(page));
      const payload = await fetchJson(baseUrl.toString());
      const pageJobs = extractDreamJobs(payload);

      if (pageJobs.length === 0) break;

      for (const job of pageJobs) {
        const key = normalizeUrl(job.url);
        if (!seen.has(key)) {
          seen.add(key);
          jobs.push(job);
        }
      }

      if (pageJobs.length < perPage) break;
    }
  }

  return jobs;
}

/* ── MelonJobs ──────────────────────────────────────────────── */

function inferExperience(title, description) {
  const normalized = normalizeText(`${title ?? ""} ${description ?? ""}`);
  if (!titleNotBlacklisted(title, description)) return "senior";
  if (/\bmedior\b/.test(normalized)) return "medior";
  if (/\bjunior\b|\bpalyakezdo\b|\bentry level\b/.test(normalized)) return "junior";
  return null;
}

function titleNotBlacklisted(title, desc) {
  const combined = normalizeText(`${title ?? ""} ${desc ?? ""}`);
  return !_filters.some(kw => combined.includes(normalizeText(kw)));
}

function isBudapestLocation(location) {
  const normalized = normalizeText(location);
  return normalized.includes("budapest");
}

function extractMelonJobs(payload) {
  const rows = Array.isArray(payload) ? payload : [];

  return rows
    .map((job) => {
      const title = htmlToText(job?.title?.rendered);
      const description = htmlToText(job?.content?.rendered);
      const url = normalizeUrl(job?.link || "");
      const location = normalizeWhitespace(job?.meta?._job_location);

      return {
        title,
        description,
        url,
        location,
        experience: inferExperience(title, description),
      };
    })
    .filter((job) => job.title && job.url)
    .filter((job) => isBudapestLocation(job.location))
    .filter((job) => titleNotBlacklisted(job.title, job.description));
}

async function fetchAllMelonJobs() {
  const jobs = [];
  const baseUrl = new URL(MELONJOBS_API_URL);
  const perPage = Number.parseInt(baseUrl.searchParams.get("per_page") || "100", 10) || 100;

  for (let page = 1; page <= MAX_PAGES; page += 1) {
    baseUrl.searchParams.set("page", String(page));
    const payload = await fetchJson(baseUrl.toString());
    const pageJobs = extractMelonJobs(payload);

    if (pageJobs.length === 0 && (!Array.isArray(payload) || payload.length === 0)) break;

    jobs.push(...pageJobs);

    if (!Array.isArray(payload) || payload.length < perPage) break;
  }

  return jobs;
}

/* ── handler ────────────────────────────────────────────────── */

export default async () => {
  _filters = await loadFilters();
  const client = await pool.connect();

  try {
    /* DreamJobs */
    const removedBlacklisted = await deleteExistingBlacklistedDreamJobs(client);
    console.log(`dreamjobs: removed ${removedBlacklisted} existing blacklisted jobs`);

    const dreamJobs = (await fetchAllDreamJobs()).filter((job) => titleNotBlacklisted(job.title, job.experience || ""));
    console.log(`dreamjobs: ${dreamJobs.length} jobs found`);

    for (const job of dreamJobs) {
      await upsertJob(client, "dreamjobs", job);
    }
    console.log(`dreamjobs: ${dreamJobs.length} jobs processed`);

    /* MelonJobs */
    const melonJobs = await fetchAllMelonJobs();
    console.log(`melonjobs: ${melonJobs.length} jobs found`);

    for (const job of melonJobs) {
      await upsertJob(client, "melonjobs", job);
    }
    console.log(`melonjobs: ${melonJobs.length} jobs processed`);

    return new Response("OK");
  } finally {
    client.release();
  }
};