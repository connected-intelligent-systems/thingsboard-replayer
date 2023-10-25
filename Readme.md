# Replayer

Replayer can replay recorded timeseries data in realmtime from csv files to simulate advanced iot environments.

Supported backends:
  * Thingsboard

## Usage

Use the following environment variables:

* CSV_FILE: The syntised file to replay
* CSV_TIMESTAMP_COLUMN: Column with the timestamp (default: Time)
* CSV_TIMESTAMP_FORMAT: Format of the timestamp 'TS' or 'ISO' (default: TS)
* CSV_IGNORE_COLUMNS: Ignore certain columns (default: Unix)
* MQTT_URL: Url of the MQTT broker (e.g. mqtt://localhost:1883)
* MQTT_USERNAME (optional): Username if authentication is needed
* MQTT_PASSWORD (optional): Password if authentication is needed
* MAX_WAIT_TIME: Skip rows that exceed the maximum time (default: 60000)

## Authors

Sebastian Alberternst <sebastian.alberternst@dfki.de>

## License

MIT 
