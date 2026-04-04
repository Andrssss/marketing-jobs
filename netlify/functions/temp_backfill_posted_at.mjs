export const config = {
  schedule: "50 * * * *",
};

/**
 * TEMP SCRIPT – Backfill posted_at for LinkedIn jobs.
 * Scrapes search results, matches by job ID, updates DB.
 * Delete after first successful run.
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

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/** Extract LinkedIn numeric job ID from URL, e.g. "…-4387149617?…" → "4387149617" */
function extractJobId(url) {
  try {
    const path = new URL(url).pathname; // /jobs/view/slug-name-1234567890
    const m = path.match(/-(\d{5,})(?:\/|$)/);
    return m ? m[1] : null;
  } catch {
    return null;
  }
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
          "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
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
          if (!location) return reject(new Error(`HTTP ${code} (no Location)`));
          if (redirectLeft <= 0) return reject(new Error(`Too many redirects`));
          res.resume();
          return resolve(fetchText(new URL(location, url).toString(), redirectLeft - 1));
        }

        const enc = String(res.headers["content-encoding"] || "").toLowerCase();
        let stream = res;
        if (enc.includes("gzip")) stream = res.pipe(zlib.createGunzip());
        else if (enc.includes("deflate")) stream = res.pipe(zlib.createInflate());
        else if (enc.includes("br")) stream = res.pipe(zlib.createBrotliDecompress());

        let body = "";
        stream.setEncoding("utf8");
        stream.on("data", (chunk) => (body += chunk));
        stream.on("end", () => {
          if (code >= 200 && code < 300) resolve(body);
          else reject(new Error(`HTTP ${code}`));
        });
        stream.on("error", reject);
      }
    );
    req.on("timeout", () => req.destroy(new Error("Timeout")));
    req.on("error", reject);
    req.end();
  });
}

function extractJobsWithDates(html) {
  const $ = cheerioLoad(html);
  const jobs = [];

  $("ul.jobs-search__results-list li").each((_, el) => {
    const url = $(el).find("a.base-card__full-link").attr("href");
    const timeEl = $(el).find("time");
    const postedAt = timeEl.attr("datetime") || null;
    const jobId = url ? extractJobId(url) : null;
    if (jobId && postedAt) {
      jobs.push({ jobId, postedAt });
    }
  });

  return jobs;
}

/* LinkedIn search URLs – broadest filters (no time restriction) for maximum coverage */
const SEARCH_URLS = [
  // Marketing
  "https://www.linkedin.com/jobs/search/?keywords=Marketing&location=Budapest",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=2&keywords=Marketing&location=Budapest&origin=JOB_SEARCH_PAGE_JOB_FILTER",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=1&keywords=Marketing&location=Budapest&origin=JOB_SEARCH_PAGE_JOB_FILTER",
  // Online Marketing
  "https://www.linkedin.com/jobs/search/?keywords=Online%20Marketing&location=Budapest",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=2&keywords=Online%20Marketing&location=Budapest&origin=JOB_SEARCH_PAGE_JOB_FILTER",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=1&keywords=Online%20Marketing&location=Budapest&origin=JOB_SEARCH_PAGE_JOB_FILTER",
  // Market Analysis
  "https://www.linkedin.com/jobs/search/?keywords=Market%20Analysis&location=Budapest",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=2&keywords=Market%20Analysis&location=Budapest&origin=JOB_SEARCH_PAGE_JOB_FILTER",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=1&keywords=Market%20Analysis&location=Budapest&origin=JOB_SEARCH_PAGE_JOB_FILTER",
  // Market Research
  "https://www.linkedin.com/jobs/search/?keywords=Market%20Research&location=Budapest",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=2&keywords=Market%20Research&location=Budapest&origin=JOB_SEARCH_PAGE_JOB_FILTER",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=1&keywords=Market%20Research&location=Budapest&origin=JOB_SEARCH_PAGE_JOB_FILTER",
  // Test
  "https://www.linkedin.com/jobs/search/?keywords=test&location=Budapest",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=2&keywords=test&location=Budapest&origin=JOB_SEARCH_PAGE_JOB_FILTER",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=1&keywords=test&location=Budapest&origin=JOB_SEARCH_PAGE_JOB_FILTER",
  // Teszt
  "https://www.linkedin.com/jobs/search/?keywords=teszt&location=Budapest",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=2&keywords=teszt&location=Budapest&origin=JOB_SEARCH_PAGE_JOB_FILTER",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=1&keywords=teszt&location=Budapest&origin=JOB_SEARCH_PAGE_JOB_FILTER",
];

export default async () => {
  const client = await pool.connect();
  const dateMap = new Map(); // jobId → postedAt

  try {
    // Step 1: Scrape LinkedIn search results for posted dates
    for (const searchUrl of SEARCH_URLS) {
      try {
        const html = await fetchText(searchUrl);
        const jobs = extractJobsWithDates(html);
        for (const j of jobs) {
          if (!dateMap.has(j.jobId)) {
            dateMap.set(j.jobId, j.postedAt);
          }
        }
        console.log(`backfill: ${searchUrl.match(/keywords=([^&]+)/)?.[1] || "?"} → ${jobs.length} jobs`);
      } catch (err) {
        console.log(`backfill: FAIL ${searchUrl.match(/keywords=([^&]+)/)?.[1] || "?"}: ${err.message}`);
      }
      await sleep(1500);
    }

    console.log(`backfill: ${dateMap.size} unique jobs with dates from search results`);

    // Step 2: Get ALL LinkedIn jobs (update wrong dates too, not just NULLs)
    const { rows } = await client.query(
      `SELECT id, url FROM marketing_job_posts WHERE source = 'LinkedIn'`
    );

    console.log(`backfill: ${rows.length} total LinkedIn jobs in DB`);

    // Step 3: Match by job ID and update
    let updated = 0;
    for (const row of rows) {
      const jobId = extractJobId(row.url);
      if (jobId && dateMap.has(jobId)) {
        await client.query(
          `UPDATE marketing_job_posts SET posted_at = $1 WHERE id = $2`,
          [dateMap.get(jobId), row.id]
        );
        updated++;
      }
    }

    // Debug samples
    const sampleScraped = [...dateMap.entries()].slice(0, 3);
    const sampleDb = rows.slice(0, 3).map(r => ({ id: r.id, jobId: extractJobId(r.url) }));
    console.log(`backfill: sample scraped: ${JSON.stringify(sampleScraped)}`);
    console.log(`backfill: sample DB: ${JSON.stringify(sampleDb)}`);

    const msg = `backfill: updated ${updated}/${rows.length}`;
    console.log(msg);

    return new Response(JSON.stringify({ ok: true, scraped: dateMap.size, total: rows.length, updated }), {
      headers: { "Content-Type": "application/json" },
    });
  } finally {
    client.release();
  }
};
