export const config = {
  schedule: "50 * * * *",
};

/**
 * TEMP SCRIPT – Backfill posted_at for LinkedIn jobs from the last 5h.
 * Scheduled function – delete after first successful run.
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

function normalizeText(s) {
  return String(s ?? "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function canonicalizeLinkedInJobUrl(raw) {
  try {
    const u = new URL(raw);
    if (u.hostname.includes("linkedin.com") && u.pathname.startsWith("/jobs/view/")) {
      const lastPart = u.pathname.split("/jobs/view/")[1];
      const canonicalSlug = lastPart.replace(/-\d+$/, "");
      return `https://www.linkedin.com/jobs/view/${canonicalSlug}`;
    }
    return raw;
  } catch {
    return raw;
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

function extractLinkedInJobsWithDates(html) {
  const $ = cheerioLoad(html);
  const jobs = [];

  $("ul.jobs-search__results-list li").each((_, el) => {
    const url = $(el).find("a.base-card__full-link").attr("href");
    const timeEl = $(el).find("time");
    const postedAt = timeEl.attr("datetime") || null;
    if (url && postedAt) {
      jobs.push({ url, canonicalUrl: canonicalizeLinkedInJobUrl(url), postedAt });
    }
  });

  return jobs;
}

/* Representative LinkedIn search URLs from the cron jobs */
const SEARCH_URLS = [
  // Marketing
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=2&f_TPR=r86400&keywords=Marketing&location=Budapest",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=1&f_TPR=r86400&keywords=Marketing&location=Budapest",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=2&f_TPR=r604800&keywords=Marketing&location=Budapest",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=1&f_TPR=r604800&keywords=Marketing&location=Budapest",
  "https://www.linkedin.com/jobs/search/?keywords=Marketing&location=Budapest",
  // Online Marketing
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=2&f_TPR=r86400&keywords=Online%20Marketing&location=Budapest",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=1&f_TPR=r86400&keywords=Online%20Marketing&location=Budapest",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=2&f_TPR=r604800&keywords=Online%20Marketing&location=Budapest",
  "https://www.linkedin.com/jobs/search/?keywords=Online%20Marketing&location=Budapest",
  // Market Analysis
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=2&f_TPR=r86400&keywords=Market%20Analysis&location=Budapest",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=1&f_TPR=r86400&keywords=Market%20Analysis&location=Budapest",
  "https://www.linkedin.com/jobs/search/?keywords=Market%20Analysis&location=Budapest",
  // Market Research
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=2&f_TPR=r86400&keywords=Market%20Research&location=Budapest",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=1&f_TPR=r86400&keywords=Market%20Research&location=Budapest",
  "https://www.linkedin.com/jobs/search/?keywords=Market%20Research&location=Budapest",
  // Test
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=2&f_TPR=r86400&keywords=test&location=Budapest",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=1&f_TPR=r86400&keywords=test&location=Budapest",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=2&f_TPR=r86400&keywords=teszt&location=Budapest",
  "https://www.linkedin.com/jobs/search/?distance=10&f_E=1&f_TPR=r86400&keywords=teszt&location=Budapest",
];

export default async () => {
  const client = await pool.connect();
  const jobMap = new Map(); // canonicalUrl -> postedAt

  try {
    // Step 1: Scrape LinkedIn for posted dates
    for (const searchUrl of SEARCH_URLS) {
      try {
        const html = await fetchText(searchUrl);
        const jobs = extractLinkedInJobsWithDates(html);
        for (const j of jobs) {
          if (!jobMap.has(j.canonicalUrl)) {
            jobMap.set(j.canonicalUrl, j.postedAt);
          }
        }
        console.log(`backfill: ${searchUrl.match(/keywords=([^&]+)/)?.[1] || "?"} → ${jobs.length} jobs with dates`);
      } catch (err) {
        console.log(`backfill: failed ${searchUrl}: ${err.message}`);
      }
      await sleep(1500);
    }

    console.log(`backfill: ${jobMap.size} unique LinkedIn jobs with posted dates`);

    // Step 2: Get existing LinkedIn jobs from last 5h that have no posted_at
    const { rows } = await client.query(
      `SELECT id, url, canonical_url
       FROM marketing_job_posts
       WHERE source = 'LinkedIn'
         AND first_seen >= NOW() - INTERVAL '5 hours'
         AND posted_at IS NULL`
    );

    console.log(`backfill: ${rows.length} LinkedIn jobs from last 5h without posted_at`);

    // Step 3: Update
    let updated = 0;
    for (const row of rows) {
      const canonical = row.canonical_url || canonicalizeLinkedInJobUrl(row.url);
      const postedAt = jobMap.get(canonical);
      if (postedAt) {
        await client.query(
          `UPDATE marketing_job_posts SET posted_at = $1 WHERE id = $2`,
          [postedAt, row.id]
        );
        updated++;
      }
    }

    const msg = `backfill: updated ${updated}/${rows.length} rows with posted_at`;
    console.log(msg);

    return new Response(JSON.stringify({ ok: true, scraped: jobMap.size, candidates: rows.length, updated }), {
      headers: { "Content-Type": "application/json" },
    });
  } finally {
    client.release();
  }
};
