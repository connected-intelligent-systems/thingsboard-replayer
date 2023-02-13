"use strict";

const fs = require("fs");
const csv = require("fast-csv");
const crypto = require("crypto");
const env = require("env-var");
const mqtt = require("mqtt");

const CsvFile = env.get("CSV_FILE").required().asString();
const MqttUrl = env.get("MQTT_URL").required().asString();
const MqttUsername = env.get("MQTT_USERNAME").asString();
const MqttPassword = env.get("MQTT_PASSWORD").asString();
const MqttTopic = env
  .get("MQTT_TOPIC")
  .default("syntised")
  .required()
  .asString();

function buildDiscoveryMessage({ name, deviceId }) {
  return {
    device_class: "power",
    state_topic: `${MqttTopic}/device/${deviceId}/${name}`,
    name: `power`,
    unit_of_measurement: "W",
    value_template: "{{ value_json.value }}",
    unique_id: `${deviceId}-${name}`,
    device: {
      identifiers: `${deviceId}-${name}`,
      name: `Simulated Power Plug - ${name}`,
      model: "SynTiSeD",
      manufacturer: "DFKI",
    },
  };
}

// returns the milliseconds of the day
function getMillisecondsOfDay(date) {
  return (
    (date.getHours() * 60 * 60 + date.getMinutes() * 60 + date.getSeconds()) *
      1000 +
    date.getMilliseconds()
  );
}

class TimeBuffer {
  constructor() {
    this._data = [];
    this._currentIndex = 0;
  }

  addBuffer(row) {
    this._data.push(row);
  }

  skip() {
    // skip current values until relative time of day is found
    let done = false;
    do {
      const time = new Date(this.current.timestamp);
      done = getMillisecondsOfDay(time) < getMillisecondsOfDay(new Date());
      if (done) {
        this.next();
      }
    } while (done);
  }

  next() {
    if (this._currentIndex + 1 > this._data.length - 1) {
      this._currentIndex = 0;
    }
    this._currentIndex++;
  }

  waitForNext(callback) {
    setTimeout(() => {
      if (callback !== undefined) {
        callback(this.current);
      }
      this.next();
      this.waitForNext(callback);
    }, this.duration);
  }

  get current() {
    return this._data[this._currentIndex];
  }

  get duration() {
    const now = getMillisecondsOfDay(new Date());
    const time = getMillisecondsOfDay(
      new Date(this._data[this._currentIndex].timestamp)
    );
    const diff = time - now;
    if (diff < 0) {
      return 0;
    }
    return diff;
  }

  get length() {
    return this._data.length;
  }
}

function createHash(data, len) {
  return crypto
    .createHash("shake256", { outputLength: len })
    .update(data)
    .digest("hex");
}

async function run() {
  const mqttClient = mqtt.connect(MqttUrl, {
    username: MqttUsername,
    password: MqttPassword,
  });

  mqttClient.on("connect", () => {
    const deviceId = createHash(CsvFile, 10);
    const csvOptions = {
      headers: true,
      delimiter: ",",
    };
    let discoverySent = false;
    const buffer = new TimeBuffer();
    fs.createReadStream(CsvFile)
      .pipe(csv.parse(csvOptions))
      .on("error", (error) => console.error(error))
      .on("data", (row) => {
        buffer.addBuffer(row);
      })
      .on("end", () => {
        buffer.skip();
        buffer.waitForNext(async (buffer) => {
          // iterate over all columns of the csv
          for (const key of Object.keys(buffer)) {
            if (key === "timestamp") continue;
            const name = key.replace(" ", "_");
            if (discoverySent === false) {
              mqttClient.publish(
                `homeassistant/sensor/${name}/config`,
                JSON.stringify(buildDiscoveryMessage({ name, deviceId })),
                {
                  retain: true,
                }
              );
            }
            const topic = `${MqttTopic}/device/${deviceId}/${name}`;
            mqttClient.publish(
              topic,
              JSON.stringify({
                value: +buffer[key],
                time: new Date(buffer.timestamp),
              })
            );
          }
          discoverySent = true;
        });
      });
  });

  mqttClient.on("error", (err) => {
    console.error(err);
    process.exit(1);
  });
}

run()
  .then(() => console.log(`Replaying ${CsvFile}`))
  .catch((e) => console.error(e));
