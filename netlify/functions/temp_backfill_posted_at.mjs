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

function extractPostedDate(html) {
  const $ = cheerioLoad(html);
  // Job detail page: <time datetime="2026-03-20">
  const timeEl = $("time[datetime]").first();
  if (timeEl.length) return timeEl.attr("datetime");
  return null;
}

export default async () => {
  const client = await pool.connect();

  try {
    // Step 1: Get all LinkedIn jobs from DB that need posted_at
    const { rows } = await client.query(
      `SELECT id, url, title
       FROM marketing_job_posts
       WHERE source = 'LinkedIn'
         AND posted_at IS NULL`
    );

    console.log(`backfill: ${rows.length} LinkedIn jobs without posted_at`);
    if (rows.length === 0) {
      return new Response(JSON.stringify({ ok: true, total: 0, updated: 0 }), {
        headers: { "Content-Type": "application/json" },
      });
    }

    // Step 2: Visit each job URL from the DB, extract date
    let updated = 0;
    let noDate = 0;
    let failed = 0;
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      try {
        const html = await fetchText(row.url);
        const postedAt = extractPostedDate(html);
        if (postedAt) {
          await client.query(
            `UPDATE marketing_job_posts SET posted_at = $1 WHERE id = $2`,
            [postedAt, row.id]
          );
          updated++;
          console.log(`backfill: [${i + 1}/${rows.length}] ✓ ${row.title} → ${postedAt}`);
        } else {
          noDate++;
          console.log(`backfill: [${i + 1}/${rows.length}] no date: ${row.title}`);
        }
      } catch (err) {
        failed++;
        console.log(`backfill: [${i + 1}/${rows.length}] FAIL: ${row.title} – ${err.message}`);
      }
      if (i < rows.length - 1) await sleep(1000);
    }

    const msg = `backfill: done – updated ${updated}, no date ${noDate}, failed ${failed} / ${rows.length} total`;
    console.log(msg);

    return new Response(JSON.stringify({ ok: true, total: rows.length, updated, noDate, failed }), {
      headers: { "Content-Type": "application/json" },
    });
  } finally {
    client.release();
  }
};
