# JMeter-Listener

[![Maven Central](https://img.shields.io/maven-central/v/cloud.testload/jmeter.pack-listener.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22cloud.testload%22%20AND%20a:%22jmeter.pack-listener%22)
[![Javadocs](https://www.javadoc.io/badge/cloud.testload/jmeter.pack-listener.svg)](https://www.javadoc.io/doc/cloud.testload/jmeter.pack-listener)
![pipeline](https://gitlab.com/testload/jmeter-listener/badges/master/build.svg?job=build)

This JMeter Plugin allows to write load test data on-the-fly to ClickHouse, InfluxDB, ElasticSearch (This project is a partial fork of https://github.com/NovaTecConsulting/JMeter-InfluxDB-Writer and https://github.com/delirius325/jmeter-elasticsearch-backend-listener projects)

Additional feature: aggregation of Samplers

Explanations and usage examples on [project wiki](https://gitlab.com/testload/jmeter-listener/wikis/1.-Main). 

Strongly recommended use [clickhouse_bulk](https://github.com/nikepan/clickhouse-bulk) ([docker image](https://hub.docker.com/r/nikepan/clickhouse-bulk/)) - brilliant INSERT bulkizator for ClickHouse

UPD (03 JUNE 2019): New ClickHouseDB scheme used - jmresults(ops storage with extended data request/response)->jmresults_statistic(mat view as archive storage).

UPD (29 NOV 2019): V2 class!:
1. New scheme - 
    jmresults(memory buffer) -> 
    jmresults_data(temp ops storage with extended data request/response and TTL 7 days) ->
    jmresults_statistic(mat view as archive storage)
   +++ res_code field in database
   
2. Minus group_by .... Plus new "aggregate" log level
3. New error log level - like "info" but if error add Req/Resp
4. few memory usage optimizations
5. new Grafana dashboards (soon...)

Many thanks for support from:

[JetBrains](https://www.jetbrains.com/opensource/)

[Atlassian](https://www.atlassian.com/software/views/open-source-license-request)

