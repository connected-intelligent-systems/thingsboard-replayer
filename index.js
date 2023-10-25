'use strict'

const fs = require('fs')
const csv = require('fast-csv')
const env = require('env-var')
const mqtt = require('mqtt')
const crypto = require('crypto')
const path = require('path')

const CsvTimestampFormats = Object.freeze({
  ISO: 'ISO',
  TS: 'TS'
})

const CsvFile = env.get('CSV_FILE').required().asString()
const CsvTimestampColumn = env
  .get('CSV_TIMESTAMP_COLUMN')
  .default('Time')
  .required()
  .asString()
const CsvTimestampFormat = env
  .get('CSV_TIMESTAMP_FORMAT')
  .default(CsvTimestampFormats.ISO)
  .required()
  .asEnum([CsvTimestampFormats.ISO, CsvTimestampFormats.TS])
const CsvIgnoreColumns = env
  .get('CSV_IGNORE_COLUMNS')
  .default('Unix')
  .asArray()
const MqttUrl = env.get('MQTT_URL').required().asString()
const MqttUsername = env.get('MQTT_USERNAME').asString()
const MqttPassword = env.get('MQTT_PASSWORD').asString()
const ThingModel = env
  .get('THING_MODEL')
  .required()
  .default(
    'https://raw.githubusercontent.com/salberternst/thing-models/main/home_assistant/power.json'
  )
  .asString()
const MaxWaitTime = env
  .get('MAX_WAIT_TIME')
  .required()
  .default('60000')
  .asIntPositive()

/**
 * Returns the milliseconds relative to the day
 * @param {Date} date A JavaScript date object
 * @return {number} Milliseconds of the day
 */
function getMillisecondsOfDay (date) {
  return (
    (date.getHours() * 60 * 60 + date.getMinutes() * 60 + date.getSeconds()) *
      1000 +
    date.getMilliseconds()
  )
}

/**
 * Sleeps for an amount of times
 * @param {number} ms Number of milliseconds to sleep
 */
function sleep (ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms)
  })
}

/**
 * Calculates a unique device id for the row name. Currently calculates
 * the sha1 hash of the concatenated name and the mqtt username.
 * @param {string} name Name of the row
 * @return {string} Unique device ids
 */
function getUniqueDeviceId (name) {
  return crypto
    .createHash('sha1')
    .update(`${name}:${MqttUsername}`)
    .digest('hex')
}

/**
 * Returns the base name of an url to extract the file name.
 * @param {string} urlStr Urls
 * @return {string} Base name of the urls
 */
function getBasenameFromUrl (urlStr) {
  const url = new URL(urlStr)
  return path.parse(url.pathname).name
}

/**
 * Extract a unique but human-readable type from the thing-model The unique type
 * can be used to filter actions in the thingsboard rule engine.
 * @param {string} thingModel Url to the thing model
 * @return {string} Unique type id
 */
function getUniqueTypeId (thingModel) {
  if (thingModel === undefined) {
    return 'default'
  } else {
    const hashedModelUrl = crypto
      .createHash('sha1')
      .update(thingModel)
      .digest('hex')
    return `${getBasenameFromUrl(thingModel)}#${hashedModelUrl}`
  }
}

/**
 * Send attributes of the row. Currently sets the thing model and some thing metadata
 * @param {MqttClient} mqttClient mqtt client to use
 * @param {string[]} row Row from the csv
 */
function sendAttributes (mqttClient, row) {
  const attributes = {}
  for (const key of Object.keys(row)) {
    if (key === CsvTimestampColumn || CsvIgnoreColumns.includes(key)) continue
    const deviceId = getUniqueDeviceId(key)
    attributes[deviceId] = {
      'thing-model': ThingModel,
      'thing-metadata': {
        description: key
      }
    }
  }

  mqttClient.publish('v1/gateway/attributes', JSON.stringify(attributes))
}

/**
 * Send the connect message for every column.
 * @param {MqttClient} mqttClient mqtt client to use
 * @param {string[]} row Row from the csv
 */
function sendConnect (mqttClient, row) {
  for (const key of Object.keys(row)) {
    if (key === CsvTimestampColumn || CsvIgnoreColumns.includes(key)) continue
    const deviceId = getUniqueDeviceId(key)
    mqttClient.publish(
      'v1/gateway/connect',
      JSON.stringify({
        device: deviceId,
        type: getUniqueTypeId(ThingModel)
      })
    )
  }
}

/**
 * Send the telemetry data for every column.
 * @param {MqttClient} mqttClient mqtt client to uses
 * @param {string[]} row Row from the csv
 */
function sendTelemetry (mqttClient, row) {
  const telemetry = {}
  for (const key of Object.keys(row)) {
    if (key === CsvTimestampColumn || CsvIgnoreColumns.includes(key)) continue
    const deviceId = getUniqueDeviceId(key)
    telemetry[deviceId] = [
      {
        power: +row[key]
      }
    ]
  }
  mqttClient.publish('v1/gateway/telemetry', JSON.stringify(telemetry))
}

/**
 * Return the date from the select date column and date settings.
 * @param {string[]} row Row from the csv
 * @return {Date} The returned date object
 */
function getDate (row) {
  const date = row[CsvTimestampColumn]
  if (date === undefined) {
    throw new Error('Invalid timestamp column')
  }

  if (CsvTimestampFormat === CsvTimestampFormats.ISO) {
    return new Date(date)
  } else {
    return new Date(+date)
  }
}

async function run () {
  const mqttClient = mqtt.connect(MqttUrl, {
    username: MqttUsername,
    password: MqttPassword
  })

  mqttClient.on('connect', () => {
    let sentAttributes = false
    const csvOptions = {
      headers: true,
      delimiter: ','
    }
    const readable = fs
      .createReadStream(CsvFile)
      .pipe(csv.parse(csvOptions))
      .on('error', (error) => console.error(error))
      .on('data', async (row) => {
        const now = getMillisecondsOfDay(new Date())
        const time = getMillisecondsOfDay(getDate(row))
        const diff = time - now
        if (diff >= 0 && diff < MaxWaitTime) {
          readable.pause()

          await sleep(diff)

          if (sentAttributes === false) {
            sendConnect(mqttClient, row)
            sendAttributes(mqttClient, row)
            sentAttributes = true
          }

          sendTelemetry(mqttClient, row)

          readable.resume()
        }
      })
  })

  mqttClient.on('error', (err) => {
    console.error(err)
    process.exit(1)
  })
}

run()
  .then(() => console.log(`Replaying ${CsvFile}`))
  .catch((e) => console.error(e))
