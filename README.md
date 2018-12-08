# JMeter-Listener

[![Maven Central](https://img.shields.io/maven-central/v/cloud.testload/jmeter.pack-listener.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22cloud.testload%22%20AND%20a:%22jmeter.pack-listener%22)

This project is a partial fork of https://github.com/NovaTecConsulting/JMeter-InfluxDB-Writer and https://github.com/delirius325/jmeter-elasticsearch-backend-listener projects

This JMeter Plugin allows to write load test data on-the-fly to InfluxDB, ElasticSearch, ClickHouse (on testing currently)

Additional feature: aggregation of Samplers

Explanations and usage examples on [project wiki](https://gitlab.com/testload/jmeter-listener/wikis/1.-Main). ClickHouse usage example will be updated soon.

Strongly recommended use [chproxy](https://github.com/Vertamedia/chproxy) ([docker image](https://hub.docker.com/r/tacyuuhon/clickhouse-chproxy/)) for ClickHouse
