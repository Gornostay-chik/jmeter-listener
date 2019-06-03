# JMeter-Listener

[![Maven Central](https://img.shields.io/maven-central/v/cloud.testload/jmeter.pack-listener.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22cloud.testload%22%20AND%20a:%22jmeter.pack-listener%22)
[![Javadocs](https://www.javadoc.io/badge/cloud.testload/jmeter.pack-listener.svg)](https://www.javadoc.io/doc/cloud.testload/jmeter.pack-listener)
![pipeline](https://gitlab.com/testload/jmeter-listener/badges/master/build.svg?job=build)

This JMeter Plugin allows to write load test data on-the-fly to ClickHouse, InfluxDB, ElasticSearch (This project is a partial fork of https://github.com/NovaTecConsulting/JMeter-InfluxDB-Writer and https://github.com/delirius325/jmeter-elasticsearch-backend-listener projects)

Additional feature: aggregation of Samplers

Explanations and usage examples on [project wiki](https://gitlab.com/testload/jmeter-listener/wikis/1.-Main). 

Strongly recommended use [chproxy](https://github.com/Vertamedia/chproxy) ([docker image](https://hub.docker.com/r/tacyuuhon/clickhouse-chproxy/)) for ClickHouse

UPD (03 JUNE 2019): New ClickHouseDB scheme used - jmresults(buffer)->jmresults_data(ops storage with extended data request/response)->jmresults_statistic(mat view as archive storage).