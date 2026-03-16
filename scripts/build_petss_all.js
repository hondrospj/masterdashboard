const https = require("https");
const fs = require("fs");

const stations = [
"8537374","8536889","est0008","8535901","8535581","8535419","8535221",
"8534975","8534836","est4836","8534638","8534139","8533935","8533541",
"8533615","est0006","8532786","8532591","8532337","8531804","8531592",
"8531232","8536110","8534720","8531680","8551910","8545240","8539094",
"8540433","8519483","8546252","8548989"
];

function fetchText(url) {
  return new Promise((resolve, reject) => {
    https.get(url, res => {
      let data = "";
      res.on("data", chunk => data += chunk);
      res.on("end", () => resolve(data));
    }).on("error", reject);
  });
}

async function main(){

  const url = "https://www.ftp.ncep.noaa.gov/data/nccf/com/petss/prod/petss.t18z.mean.stormtide.est.txt";

  const text = await fetchText(url);

  const output = {
    issued_utc: new Date().toISOString(),
    stations: {}
  };

  stations.forEach(id=>{
    output.stations[id] = {points:[]};
  });

  fs.writeFileSync(
    "data/petss_forecasts_all_mllw.json",
    JSON.stringify(output,null,2)
  );

  console.log("PETSS JSON built");
}

main();
