export const config = {
  schedule: "12 4-23 * * *",
};

/* =========================
  DreamJobs: category=52 (Marketing, PR), location=2925 (Budapest)
  MelonJobs: job-categories=91 (Adminisztrátor, Dokumentumkezelő), 196 (Marketing, Média, PR vezető)
  KUKA: Marketing+Communication & Administration, Budapest
*/


import { Pool } from "pg";
import https from "https";
import http from "http";
import zlib from "zlib";
import { load as cheerioLoad } from "cheerio";

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

const KUKA_API_URLS = [
  "https://jobs.kuka.com/tile-search-results/?q=&locationsearch=Budapest&optionsFacetsDD_department=Marketing+and+Communication",
  "https://jobs.kuka.com/tile-search-results/?q=&locationsearch=Budapest&optionsFacetsDD_department=Administration",
];

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

const SENIOR_KEYWORDS = [
  "senior",
  "szenior",
  "lead",
  "principal",
  "staff",
  "architect",
  "expert",
  "vezető fejlesztő",
  "tech lead",
  "gyakornok",
  "intern",
  "internship",
  "trainee",
  "diákmunka",
  "diakmunka",
  "igazgató",
  "vezető",
];

function inferExperience(title, description) {
  const normalized = normalizeText(`${title ?? ""} ${description ?? ""}`);

  if (SENIOR_KEYWORDS.some((kw) => normalized.includes(normalizeText(kw)))) return "senior";
  if (/\bmedior\b/.test(normalized)) return "medior";
  if (/\bjunior\b|\bpalyakezdo\b|\bentry level\b/.test(normalized)) return "junior";

  return null;
}

function isSeniorLike(title, description) {
  const normalized = normalizeText(`${title ?? ""} ${description ?? ""}`);
  return SENIOR_KEYWORDS.some((kw) => normalized.includes(normalizeText(kw)));
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
    .filter((job) => !isSeniorLike(job.title, job.description));
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

/* ── KUKA ───────────────────────────────────────────────────── */

function inferKukaExperience(title) {
  const normalized = normalizeText(title);
  if (SENIOR_KEYWORDS.some((kw) => normalized.includes(normalizeText(kw))))
    return "senior";
  if (/\bmedior\b|\bmid\b/.test(normalized)) return "medior";
  if (/\bjunior\b|\bpalyakezdo\b|\bentry.?level\b|\btrainee\b|\bintern\b|\bgyakornok\b/.test(normalized))
    return "junior";
  return null;
}

function extractKukaJobs(html) {
  const $ = cheerioLoad(html);
  const jobs = [];
  const seen = new Set();

  $("li[data-url]").each((_i, el) => {
    const $el = $(el);
    const path = $el.attr("data-url");
    if (!path) return;

    const url = normalizeUrl(`https://jobs.kuka.com${path.replace(/&amp;/g, "&")}`);
    if (seen.has(url)) return;
    seen.add(url);

    const title = normalizeWhitespace(
      $el.find(".jobTitle-link").first().text() ||
        $el.find(".title a").first().text() ||
        $el.find("a[href]").first().text()
    );
    if (!title) return;

    jobs.push({
      title,
      url,
      experience: inferKukaExperience(title),
    });
  });

  return jobs;
}

function extractKukaYearExperience(html) {
  const idx = html.indexOf("What you need to succeed");
  if (idx === -1) return null;

  const section = html.substring(idx, idx + 3000);
  const $ = cheerioLoad(section);
  const text = $.text();

  const patterns = [
    /\b\d+\s?\+?\s?(?:év|years?|éves|yrs?)\b/gi,
    /\b\d+\s?[-–]\s?\d+\s?(?:év|years?|éves|yrs?)\b/gi,
    /\bseveral\s+years?\b/gi,
    /\bminimum\s?\d+\s?(?:év|years?)\b/gi,
    /\bat\s+least\s+\d+\s?(?:years?|év)\b/gi,
  ];

  const matches = [];
  for (const regex of patterns) {
    const found = text.match(regex);
    if (found) matches.push(...found);
  }

  if (matches.length === 0) return null;

  const maxReasonable = 15;
  const filtered = matches.filter((m) => {
    const nums = m.match(/\d+/g)?.map((n) => parseInt(n, 10)) || [];
    return nums.length === 0 || nums.every((n) => n <= maxReasonable);
  });

  if (filtered.length === 0) return null;

  return [...new Set(
    filtered.map((m) => m.replace(/\s+/g, " ").trim().toLowerCase())
  )].join(", ");
}

async function fetchAllKukaJobs() {
  const jobs = [];
  const seen = new Set();

  for (const apiUrl of KUKA_API_URLS) {
    const html = await fetchText(apiUrl);
    for (const job of extractKukaJobs(html)) {
      if (!seen.has(job.url)) {
        seen.add(job.url);
        jobs.push(job);
      }
    }
  }

  for (let i = 0; i < jobs.length; i++) {
    const job = jobs[i];
    try {
      const jobHtml = await fetchText(job.url);
      const yearExp = extractKukaYearExperience(jobHtml);
      if (yearExp) {
        console.log(`kuka: ${job.title} → experience from page: ${yearExp}`);
        job.experience = yearExp;
      }
    } catch (err) {
      console.log(`kuka: failed to fetch detail for ${job.title}: ${err.message}`);
    }
    if (i < jobs.length - 1) await sleep(500);
  }

  return jobs;
}

/* ── handler ────────────────────────────────────────────────── */

export default async () => {
  const client = await pool.connect();

  try {
    /* DreamJobs */
    const dreamJobs = (await fetchAllDreamJobs()).filter((job) => !isSeniorLike(job.title, ""));
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

    /* KUKA */
    const kukaJobs = (await fetchAllKukaJobs()).filter((job) => !isSeniorLike(job.title, ""));
    console.log(`kuka: ${kukaJobs.length} jobs found`);

    for (const job of kukaJobs) {
      await upsertJob(client, "kuka", job);
    }
    console.log(`kuka: ${kukaJobs.length} jobs processed`);

    return new Response("OK");
  } finally {
    client.release();
  }
};