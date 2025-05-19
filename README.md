# JMeter-Listener

Fork of https://gitlab.com/testload/jmeter-listener

This JMeter Plugin allows to write load test data on-the-fly to ClickHouse, InfluxDB, ElasticSearch (This project is a partial fork of https://github.com/NovaTecConsulting/JMeter-InfluxDB-Writer and https://github.com/delirius325/jmeter-elasticsearch-backend-listener projects)

Additional feature: aggregation of Samplers

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
 

UPD (19 MAY 2025): V3 class!:
1. Fix problem with Gorilla-LZ4
2. New JDBC from CH - from 0.2.4 to 0.8.6

