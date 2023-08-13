"use strict";

const fs = require("fs");
const csv = require("fast-csv");
const env = require("env-var");
const mqtt = require("mqtt");

const CsvFile = env.get("CSV_FILE").required().asString();
const MqttUrl = env.get("MQTT_URL").required().asString();
const MqttUsername = env.get("MQTT_USERNAME").asString();
const MqttPassword = env.get("MQTT_PASSWORD").asString();

// returns the milliseconds of the day
function getMillisecondsOfDay(date) {
  return (
    (date.getHours() * 60 * 60 + date.getMinutes() * 60 + date.getSeconds()) *
      1000 +
    date.getMilliseconds()
  );
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function run() {
  const mqttClient = mqtt.connect(MqttUrl, {
    username: MqttUsername,
    password: MqttPassword,
  });

  mqttClient.on("connect", () => {
    const csvOptions = {
      headers: true,
      delimiter: ",",
    };
    const readable = fs.createReadStream(CsvFile)
      .pipe(csv.parse(csvOptions))
      .on("error", (error) => console.error(error))
      .on("data", async (row) => {
        const now = getMillisecondsOfDay(new Date());
        const time = getMillisecondsOfDay(
          new Date(row.timestamp)
        );
        const diff = time - now;
        if (diff >= 0) {
          readable.pause()
          await sleep(diff)

          const telemetry = {}          
          for (const key of Object.keys(row)) {
            if (key === "timestamp") continue;
            const name = key.replace(" ", "_");
            telemetry[name] = [{
              power: +row[key]
            }]
          }

          mqttClient.publish('v1/gateway/telemetry', JSON.stringify(telemetry))

          readable.resume()
        }
      })
  });

  mqttClient.on("error", (err) => {
    console.error(err);
    process.exit(1);
  });
}

run()
  .then(() => console.log(`Replaying ${CsvFile}`))
  .catch((e) => console.error(e));
