#!/usr/bin/env node

/**
 * Build one-file PETSS JSON for the map:
 *   data/petss_forecasts_all_mllw.json
 *
 * Strategy:
 * 1) Try to fetch the latest available PETSS East Coast text product.
 * 2) Parse station blocks heuristically.
 * 3) Write one consolidated JSON keyed by PETSS station ID.
 *
 * Notes:
 * - This is built for your East Coast / Mid-Atlantic gauge list.
 * - PETSS text formatting can vary. This parser is designed to be resilient,
 *   but if NOAA changes the text layout, only the parser function should need edits.
 */

const fs = require("fs");
const path = require("path");

const OUT_DIR = path.join(process.cwd(), "data");
const OUT_FILE = path.join(OUT_DIR, "petss_forecasts_all_mllw.json");

// Exact PETSS IDs from your workbook-driven map
const STATIONS = [
  "8537374","8536889","est0008","8535901","8535581","8535419","8535221",
  "8534975","8534836","est4836","8534638","8534139","8533935","8533541",
  "8533615","est0006","8532786","8532591","8532337","8531804","8531592",
  "8531232","8536110","8534720","8531680","8551910","8545240","8539094",
  "8540433","8519483","8546252","8548989"
];

// PETSS East Coast text products
const REGION = "est";
const CYCLES = ["18", "12", "06", "00"];

// Try today first, then yesterday, then 2 days back
function yyyymmddUTC(date) {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, "0");
  const d = String(date.getUTCDate()).padStart(2, "0");
  return `${y}${m}${d}`;
}

function isoFromParts(year, month, day, hour) {
  const mm = String(month).padStart(2, "0");
  const dd = String(day).padStart(2, "0");
  const hh = String(hour).padStart(2, "0");
  return `${year}-${mm}-${dd}T${hh}:00:00Z`;
}

function buildCandidateUrls() {
  const urls = [];
  for (let back = 0; back <= 2; back++) {
    const d = new Date(Date.now() - back * 24 * 3600 * 1000);
    const ymd = yyyymmddUTC(d);
    for (const cycle of CYCLES) {
      urls.push({
        ymd,
        cycle,
        url: `https://www.ftp.ncep.noaa.gov/data/nccf/com/petss/prod/petss.${ymd}/petss.t${cycle}z.mean.stormtide.${REGION}.txt`
      });
    }
  }
  return urls;
}

async function fetchText(url) {
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return await res.text();
}

/**
 * Heuristic PETSS parser
 *
 * The PETSS text products are fixed-format text. Since the exact spacing
 * can vary, this parser:
 * - scans line by line
 * - detects station IDs when one of our target IDs appears
 * - collects date/hour/value triples that appear nearby
 *
 * Accepted time patterns:
 *   YYYY MM DD HH value
 *   MM DD HH value
 *   MM/DD HH value
 *   F003 style lines with a following valid time
 *
 * If a station is found but no points are extracted, it remains empty.
 */
function parsePetssText(text, issuedUtcGuess) {
  const lines = text.replace(/\r/g, "").split("\n");
  const results = Object.fromEntries(STATIONS.map(id => [id, { points: [] }]));

  // Build fast lookup
  const stationSet = new Set(STATIONS);

  let currentStation = null;
  let currentYear = new Date(issuedUtcGuess).getUTCFullYear();

  // Common patterns
  const fullDatePattern = /(\d{4})\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,2})\s+(-?\d+(?:\.\d+)?)/;
  const shortDatePattern = /(\d{1,2})[\/\-\s]+(\d{1,2})\s+(\d{1,2})\s+(-?\d+(?:\.\d+)?)/;

  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];
    const line = raw.trim();
    if (!line) continue;

    // Find whether this line introduces one of our station IDs
    const matchedStation = STATIONS.find(id => line.includes(id));
    if (matchedStation) {
      currentStation = matchedStation;
    }

    if (!currentStation) continue;

    // Full yyyy mm dd hh value
    let m = line.match(fullDatePattern);
    if (m) {
      const year = Number(m[1]);
      const month = Number(m[2]);
      const day = Number(m[3]);
      const hour = Number(m[4]);
      const value = Number(m[5]);
      if (Number.isFinite(value)) {
        results[currentStation].points.push({
          t: isoFromParts(year, month, day, hour),
          fcst: value
        });
      }
      continue;
    }

    // Short mm dd hh value, assume issued year
    m = line.match(shortDatePattern);
    if (m) {
      const month = Number(m[1]);
      const day = Number(m[2]);
      const hour = Number(m[3]);
      const value = Number(m[4]);
      if (Number.isFinite(value)) {
        results[currentStation].points.push({
          t: isoFromParts(currentYear, month, day, hour),
          fcst: value
        });
      }
      continue;
    }

    // Sometimes PETSS lines can be like:
    // station ... 03/16 18 5.21 03/16 21 5.88 03/17 00 6.42
    const triplets = [...line.matchAll(/(\d{1,2})\/(\d{1,2})\s+(\d{1,2})\s+(-?\d+(?:\.\d+)?)/g)];
    if (triplets.length) {
      for (const t of triplets) {
        const month = Number(t[1]);
        const day = Number(t[2]);
        const hour = Number(t[3]);
        const value = Number(t[4]);
        if (Number.isFinite(value)) {
          results[currentStation].points.push({
            t: isoFromParts(currentYear, month, day, hour),
            fcst: value
          });
        }
      }
    }
  }

  // De-dup and sort
  for (const id of STATIONS) {
    const seen = new Map();
    for (const p of results[id].points) {
      if (!p.t || !Number.isFinite(p.fcst)) continue;
      seen.set(p.t, p.fcst);
    }
    results[id].points = [...seen.entries()]
      .map(([t, fcst]) => ({ t, fcst }))
      .sort((a, b) => new Date(a.t) - new Date(b.t));
  }

  return results;
}

function countPopulatedStations(stationsObj) {
  return Object.values(stationsObj).filter(v => Array.isArray(v.points) && v.points.length > 0).length;
}

async function findLatestPetssText() {
  const candidates = buildCandidateUrls();

  for (const c of candidates) {
    try {
      const text = await fetchText(c.url);
      // quick sanity check
      if (!text || text.length < 200) continue;

      const issuedUtc = `${c.ymd.slice(0,4)}-${c.ymd.slice(4,6)}-${c.ymd.slice(6,8)}T${c.cycle}:00:00Z`;
      const parsed = parsePetssText(text, issuedUtc);
      const populated = countPopulatedStations(parsed);

      console.log(`Tried ${c.url} -> populated ${populated}/${STATIONS.length}`);

      // accept the first candidate that yields anything at all
      if (populated > 0) {
        return {
          sourceUrl: c.url,
          issuedUtc,
          stations: parsed
        };
      }
    } catch (err) {
      console.log(`Missed ${c.url}: ${err.message}`);
    }
  }

  throw new Error("Could not fetch or parse any PETSS text product.");
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });

  const result = await findLatestPetssText();

  const output = {
    issued_utc: result.issuedUtc,
    source_url: result.sourceUrl,
    stations: result.stations
  };

  fs.writeFileSync(OUT_FILE, JSON.stringify(output, null, 2) + "\n", "utf8");

  const populated = countPopulatedStations(output.stations);
  console.log(`Wrote ${OUT_FILE}`);
  console.log(`Populated stations: ${populated}/${STATIONS.length}`);

  if (populated === 0) {
    process.exitCode = 1;
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
