export const config = {
  schedule: "28 9-22 * * *",
};

/* =========================
  "https://hu.talent.com,
*/



import { Pool } from "pg";
import https from "https";
import http from "http";
import zlib from "zlib";
import { load as cheerioLoad } from "cheerio";
import { loadFilters } from "./load_filters.mjs";
import { logFetchError } from "./_error-logger.mjs";

let _filters = [];

const connectionString = process.env.NETLIFY_DATABASE_URL;
if (!connectionString) throw new Error("NETLIFY_DATABASE_URL is not set");

const pool = new Pool({
  connectionString,
  ssl: { rejectUnauthorized: false },
});

const TALENT_SEARCH_URLS = [
  "https://hu.talent.com/jobs?k=marketing&l=Budapest%2C+HU&date=1",
  "https://hu.talent.com/jobs?k=irodai&l=Budapest%2C+HU&date=1",
  "https://hu.talent.com/jobs?k=Market+Research&l=Budapest%2C+HU&date=1",
  "https://hu.talent.com/jobs?k=elemző&l=Budapest%2C+HU&date=1",

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
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
          Accept: "text/html,application/xhtml+xml,*/*;q=0.8",
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

async function upsertJob(client, sourceKey, item) {
  await client.query(
    `INSERT INTO marketing_job_posts
      (source, title, url, first_seen)
     VALUES ($1,$2,$3,NOW())
     ON CONFLICT (source, url) WHERE url IS NOT NULL
     DO NOTHING;`,
    [sourceKey, item.title, item.url]
  );
}

/* ── talent.com ─────────────────────────────────────────────── */

function extractTalentJobs(html) {
  const $ = cheerioLoad(html);
  const jobs = [];
  const seen = new Set();

  $("h2").each((_i, el) => {
    const $h2 = $(el);
    const title = normalizeWhitespace($h2.text());
    if (!title) return;

    let viewHref = null;
    let $card = $h2;
    for (let j = 0; j < 8; j++) {
      $card = $card.parent();
      const link = $card.find('a[href*="/view?id="]').first();
      if (link.length) {
        viewHref = link.attr("href");
        break;
      }
    }

    if (!viewHref) return;

    const url = normalizeUrl(
      viewHref.startsWith("http") ? viewHref : `https://hu.talent.com${viewHref}`
    );

    if (seen.has(url)) return;
    seen.add(url);

    jobs.push({
      title,
      url,
    });
  });

  return jobs;
}

async function fetchAllTalentJobs() {
  const allJobs = [];
  const seen = new Set();

  for (const searchUrl of TALENT_SEARCH_URLS) {
    try {
      const html = await fetchText(searchUrl);
      const jobs = extractTalentJobs(html);

      for (const job of jobs) {
        const canonical = normalizeUrl(job.url);
        if (!seen.has(canonical)) {
          seen.add(canonical);
          allJobs.push(job);
        }
      }

      console.log(`talent: ${searchUrl.match(/k=([^&]+)/)?.[1]} → ${jobs.length} jobs`);
    } catch (err) {
      console.log(`talent: failed ${searchUrl}: ${err.message}`);
      if (/HTTP\s+[45]\d{2}/i.test(err.message)) {
        await logFetchError("cron_jobs_T", { url: searchUrl, message: err.message });
      }
    }

    await sleep(1000);
  }

  return allJobs;
}

/* ── handler ────────────────────────────────────────────────── */

export default async () => {
  _filters = await loadFilters();
  const client = await pool.connect();

  try {
    /* talent.com */
    function titleNotBlacklisted(title) {
      const t = normalizeText(title);
      return !_filters.some(word => t.includes(normalizeText(word)));
    }
    const rawJobs = (await fetchAllTalentJobs()).filter((job) => titleNotBlacklisted(job.title));
    console.log(`talent: ${rawJobs.length} unique jobs found (after filter)`);

    for (const job of rawJobs) {
      await upsertJob(client, "talent", job);
    }
    console.log(`talent: ${rawJobs.length} jobs processed`);

    return new Response("OK");
  } finally {
    client.release();
  }
};