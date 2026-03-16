#!/usr/bin/env node

const https = require("https");
const fs = require("fs");
const path = require("path");

const OUT_DIR = path.join(process.cwd(), "data");
const OUT_FILE = path.join(OUT_DIR, "petss_forecasts_all_mllw.json");

const STATIONS = [
  "8537374","8536889","est0008","8535901","8535581","8535419","8535221",
  "8534975","8534836","est4836","8534638","8534139","8533935","8533541",
  "8533615","est0006","8532786","8532591","8532337","8531804","8531592",
  "8531232","8536110","8534720","8531680","8551910","8545240","8539094",
  "8540433","8519483","8546252","8548989"
];

const CYCLES = ["18", "12", "06", "00"];

/*
  NOAA documents PETSS East Coast text products as:
  petss.tCCz.mean.stormtide.est.txt

  We fetch from the HTTP-served NCO products location rather than the old ftp host.
*/
function buildCandidateUrls() {
  return CYCLES.map(cycle => ({
    cycle,
    url: `https://www.nco.ncep.noaa.gov/pmb/products/petss/petss.t${cycle}z.mean.stormtide.est.txt`
  }));
}

function fetchText(url, timeoutMs = 20000) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { timeout: timeoutMs }, res => {
      if (res.statusCode !== 200) {
        reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        res.resume();
        return;
      }

      let data = "";
      res.setEncoding("utf8");
      res.on("data", chunk => { data += chunk; });
      res.on("end", () => resolve(data));
    });

    req.on("timeout", () => {
      req.destroy(new Error(`Timeout after ${timeoutMs} ms for ${url}`));
    });

    req.on("error", reject);
  });
}

function isoFromParts(year, month, day, hour) {
  return `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}T${String(hour).padStart(2, "0")}:00:00Z`;
}

function initStationMap() {
  return Object.fromEntries(STATIONS.map(id => [id, { points: [] }]));
}

function dedupeAndSort(points) {
  const seen = new Map();

  for (const p of points) {
    if (!p || !p.t || !Number.isFinite(p.fcst)) continue;
    seen.set(p.t, p.fcst);
  }

  return [...seen.entries()]
    .map(([t, fcst]) => ({ t, fcst }))
    .sort((a, b) => new Date(a.t) - new Date(b.t));
}

function countPopulatedStations(stations) {
  return Object.values(stations).filter(v => Array.isArray(v.points) && v.points.length > 0).length;
}

/*
  Parser notes:
  - PETSS station text is not a clean JSON or CSV format.
  - We key off the exact PETSS station IDs in your workbook.
  - Once a station ID is encountered, nearby lines are scanned for date/value patterns.
  - Supported patterns include:
      YYYY MM DD HH value
      MM DD HH value
      MM/DD HH value
      repeated inline MM/DD HH value triplets
*/
function parsePetssText(text, issuedUtc) {
  const stations = initStationMap();
  const lines = text.replace(/\r/g, "").split("\n");
  const year = new Date(issuedUtc).getUTCFullYear();

  let currentStation = null;

  const fullPattern = /(\d{4})\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,2})\s+(-?\d+(?:\.\d+)?)/g;
  const shortPattern = /(^|\s)(\d{1,2})[\/\-\s]+(\d{1,2})\s+(\d{1,2})\s+(-?\d+(?:\.\d+)?)(?=\s|$)/g;

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) continue;

    const stationMatch = STATIONS.find(id => line.toLowerCase().includes(id.toLowerCase()));
    if (stationMatch) {
      currentStation = stationMatch;
    }

    if (!currentStation) continue;

    let matchedAny = false;

    for (const m of line.matchAll(fullPattern)) {
      const yyyy = Number(m[1]);
      const month = Number(m[2]);
      const day = Number(m[3]);
      const hour = Number(m[4]);
      const fcst = Number(m[5]);

      if (Number.isFinite(fcst)) {
        stations[currentStation].points.push({
          t: isoFromParts(yyyy, month, day, hour),
          fcst
        });
        matchedAny = true;
      }
    }

    if (matchedAny) continue;

    for (const m of line.matchAll(shortPattern)) {
      const month = Number(m[2]);
      const day = Number(m[3]);
      const hour = Number(m[4]);
      const fcst = Number(m[5]);

      if (Number.isFinite(fcst)) {
        stations[currentStation].points.push({
          t: isoFromParts(year, month, day, hour),
          fcst
        });
        matchedAny = true;
      }
    }

    if (matchedAny) continue;

    // Handle inline repeating triplets like:
    // 03/16 18 5.21 03/16 21 5.88 03/17 00 6.42
    const inlineTriplets = [...line.matchAll(/(\d{1,2})\/(\d{1,2})\s+(\d{1,2})\s+(-?\d+(?:\.\d+)?)/g)];
    if (inlineTriplets.length) {
      for (const m of inlineTriplets) {
        const month = Number(m[1]);
        const day = Number(m[2]);
        const hour = Number(m[3]);
        const fcst = Number(m[4]);

        if (Number.isFinite(fcst)) {
          stations[currentStation].points.push({
            t: isoFromParts(year, month, day, hour),
            fcst
          });
        }
      }
    }
  }

  for (const id of STATIONS) {
    stations[id].points = dedupeAndSort(stations[id].points);
  }

  return stations;
}

async function fetchLatestWorkingProduct() {
  const candidates = buildCandidateUrls();

  for (const c of candidates) {
    try {
      const text = await fetchText(c.url);
      if (!text || text.length < 100) continue;

      const issuedUtc = new Date().toISOString();
      const stations = parsePetssText(text, issuedUtc);
      const populated = countPopulatedStations(stations);

      console.log(`Tried ${c.url} -> populated ${populated}/${STATIONS.length}`);

      if (populated > 0) {
        return {
          sourceUrl: c.url,
          cycle: c.cycle,
          issuedUtc,
          stations
        };
      }
    } catch (err) {
      console.log(`Missed ${c.url}: ${err.message}`);
    }
  }

  throw new Error("Could not fetch or parse any PETSS East Coast text product.");
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });

  const result = await fetchLatestWorkingProduct();

  const output = {
    issued_utc: result.issuedUtc,
    source_url: result.sourceUrl,
    cycle_utc: result.cycle,
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
