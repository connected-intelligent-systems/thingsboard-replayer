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
const MaxWaitTime = env
  .get('MAX_WAIT_TIME')
  .required()
  .default('60000')
  .asIntPositive()
const UseRealtime = env.get('USE_REALTIME').required().default('true').asBool()

/**
 * Read thing metadata from env variables
 * @return {array} An array of thing metadata
 */
function readThingMetadataFromEnv () {
  const metadata = []
  const regexs = [
    'ID',
    'TITLE',
    'DESCRIPTION',
    'MODEL',
    'MANUFACTURER',
    'CATEGORY',
    'PROPERTY_NAME',
    'TYPE'
  ].map((n) => new RegExp(`^COLUMN_(\\d+)_(${n})$`))
  Object.keys(process.env).forEach((key) => {
    regexs.forEach((regex) => {
      const results = key.match(regex)
      if (results !== null) {
        const index = +results[1]
        const name = results[2].toLowerCase()
        if (metadata[index] === undefined) {
          metadata[index] = {}
        }
        metadata[index][name] = process.env[key]
      }
    })
  })
  return metadata
}

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
 * @param {Array} thingMetadata Thing Metadata
 */
function sendAttributes (mqttClient, row, thingMetadata) {
  const attributes = {}
  for (const [index, key] of Object.keys(row).entries()) {
    if (key === CsvTimestampColumn || CsvIgnoreColumns.includes(key)) continue
    const deviceId = thingMetadata[index]?.id || getUniqueDeviceId(key)
    // if different columns use the same id then they should also use the same model and metadata
    // the first column defines the metadata -> ignore the rests
    if (attributes[deviceId] === undefined) {
      attributes[deviceId] = {
        'thing-model': thingMetadata[index]?.model,
        'thing-metadata': {
          title: thingMetadata[index]?.title,
          description: thingMetadata[index]?.description || key,
          manufacturer: thingMetadata[index]?.manufacturer,
          category: thingMetadata[index]?.category
        }
      }
    }
  }

  mqttClient.publish('v1/gateway/attributes', JSON.stringify(attributes))
}

/**
 * Send the connect message for every column.
 * @param {MqttClient} mqttClient mqtt client to use
 * @param {string[]} row Row from the csv
 * @param {Array} thingMetadata Thing Metadata
 */
function sendConnect (mqttClient, row, thingMetadata) {
  for (const [index, key] of Object.keys(row).entries()) {
    if (key === CsvTimestampColumn || CsvIgnoreColumns.includes(key)) continue
    mqttClient.publish(
      'v1/gateway/connect',
      JSON.stringify({
        device: thingMetadata[index]?.id || getUniqueDeviceId(key),
        type:
          thingMetadata[index]?.type ||
          getUniqueTypeId(thingMetadata[index]?.model)
      })
    )
  }
}

/**
 * Send the telemetry data for every column.
 * @param {MqttClient} mqttClient mqtt client to uses
 * @param {string[]} row Row from the csv
 * @param {Array} thingMetadata Thing Metadata
 */
function sendTelemetry (mqttClient, row, thingMetadata) {
  const telemetry = {}
  for (const [index, key] of Object.keys(row).entries()) {
    if (key === CsvTimestampColumn || CsvIgnoreColumns.includes(key)) continue
    const deviceId = thingMetadata[index]?.id || getUniqueDeviceId(key)
    const propertyName = thingMetadata[index]?.property_name || key
    if (telemetry[deviceId] === undefined) {
      telemetry[deviceId] = [
        {
          ...(UseRealtime === false && { ts: getDate(row).getTime() }),
          [propertyName]: {}
        }
      ]
    }
    telemetry[deviceId][0][propertyName] = +row[key]
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
  const thingMetadata = readThingMetadataFromEnv()
  const mqttClient = mqtt.connect(MqttUrl, {
    username: MqttUsername,
    password: MqttPassword
  })

  mqttClient.on('connect', async () => {
    let sentAttributes = false
    const csvOptions = {
      headers: true,
      delimiter: ','
    }
    const csvStream = fs.createReadStream(CsvFile).pipe(csv.parse(csvOptions))

    for await (const row of csvStream) {
      if (UseRealtime) {
        const now = getMillisecondsOfDay(new Date())
        const time = getMillisecondsOfDay(getDate(row))
        const diff = time - now
        if (diff >= 0 && diff < MaxWaitTime) {
          await sleep(diff)
        } else {
          continue
        }
      }

      if (sentAttributes === false) {
        sendConnect(mqttClient, row, thingMetadata)
        sendAttributes(mqttClient, row, thingMetadata)
        sentAttributes = true
      }

      sendTelemetry(mqttClient, row, thingMetadata)
    }

    mqttClient.end()
  })

  mqttClient.on('error', (err) => {
    console.error(err)
    process.exit(1)
  })
}

run()
  .then(() => console.log(`Replaying ${CsvFile}`))
  .catch((e) => console.error(e))
