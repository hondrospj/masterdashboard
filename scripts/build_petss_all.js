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

// PETSS text products available in the live NOMADS dated folders
const CYCLES = ["18", "12", "06", "00"];
const PRODUCT_PATH = "e90.stormtide.east.txt"; // switch to e10 if you want the lower envelope
const BASE_INDEX = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/petss/prod/";

function getText(url, timeoutMs = 20000) {
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

function initStations() {
  return Object.fromEntries(STATIONS.map(id => [id, { points: [] }]));
}

function dedupeSort(points) {
  const map = new Map();
  for (const p of points) {
    if (p?.t && Number.isFinite(p?.fcst)) map.set(p.t, p.fcst);
  }
  return [...map.entries()]
    .map(([t, fcst]) => ({ t, fcst }))
    .sort((a, b) => new Date(a.t) - new Date(b.t));
}

function countPopulated(stations) {
  return Object.values(stations).filter(v => Array.isArray(v.points) && v.points.length > 0).length;
}

async function findLatestPetssDate() {
  const html = await getText(BASE_INDEX);
  const matches = [...html.matchAll(/petss\.(\d{8})\//g)].map(m => m[1]);
  const unique = [...new Set(matches)].sort();
  if (!unique.length) {
    throw new Error("Could not find any petss.YYYYMMDD directories in NOMADS index.");
  }
  return unique[unique.length - 1];
}

async function fetchLatestWorkingText() {
  const ymd = await findLatestPetssDate();

  for (const cycle of CYCLES) {
    const url = `${BASE_INDEX}petss.${ymd}/petss.t${cycle}z.${PRODUCT_PATH}`;
    try {
      const text = await getText(url);
      if (text && text.length > 100) {
        return { ymd, cycle, url, text };
      }
    } catch (err) {
      console.log(`Missed ${url}: ${err.message}`);
    }
  }

  throw new Error(`Could not fetch any PETSS text file for ${ymd}.`);
}

/*
  Heuristic parser for PETSS text.

  It keys off your exact PETSS station IDs, then looks for nearby
  month/day/hour/value patterns. This is resilient to spacing changes.
*/
function parsePetssText(text, issuedUtc) {
  const stations = initStations();
  const lines = text.replace(/\r/g, "").split("\n");
  const issuedYear = new Date(issuedUtc).getUTCFullYear();

  let currentStation = null;

  const fullPattern = /(\d{4})\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,2})\s+(-?\d+(?:\.\d+)?)/g;
  const shortPattern = /(^|\s)(\d{1,2})[\/\-\s]+(\d{1,2})\s+(\d{1,2})\s+(-?\d+(?:\.\d+)?)(?=\s|$)/g;
  const inlineTripletPattern = /(\d{1,2})\/(\d{1,2})\s+(\d{1,2})\s+(-?\d+(?:\.\d+)?)/g;

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) continue;

    const foundStation = STATIONS.find(id => line.toLowerCase().includes(id.toLowerCase()));
    if (foundStation) currentStation = foundStation;
    if (!currentStation) continue;

    let matched = false;

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
        matched = true;
      }
    }

    if (matched) continue;

    for (const m of line.matchAll(shortPattern)) {
      const month = Number(m[2]);
      const day = Number(m[3]);
      const hour = Number(m[4]);
      const fcst = Number(m[5]);

      if (Number.isFinite(fcst)) {
        stations[currentStation].points.push({
          t: isoFromParts(issuedYear, month, day, hour),
          fcst
        });
        matched = true;
      }
    }

    if (matched) continue;

    for (const m of line.matchAll(inlineTripletPattern)) {
      const month = Number(m[1]);
      const day = Number(m[2]);
      const hour = Number(m[3]);
      const fcst = Number(m[4]);

      if (Number.isFinite(fcst)) {
        stations[currentStation].points.push({
          t: isoFromParts(issuedYear, month, day, hour),
          fcst
        });
      }
    }
  }

  for (const id of STATIONS) {
    stations[id].points = dedupeSort(stations[id].points);
  }

  return stations;
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });

  const file = await fetchLatestWorkingText();
  const issuedUtc = new Date().toISOString();
  const stations = parsePetssText(file.text, issuedUtc);
  const populated = countPopulated(stations);

  const output = {
    issued_utc: issuedUtc,
    source_url: file.url,
    source_date_utc: file.ymd,
    source_cycle_utc: file.cycle,
    source_product: PRODUCT_PATH,
    stations
  };

  fs.writeFileSync(OUT_FILE, JSON.stringify(output, null, 2) + "\n", "utf8");

  console.log(`Wrote ${OUT_FILE}`);
  console.log(`Source: ${file.url}`);
  console.log(`Populated stations: ${populated}/${STATIONS.length}`);

  if (populated === 0) {
    throw new Error("Fetched PETSS file, but parsed 0 populated stations.");
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
