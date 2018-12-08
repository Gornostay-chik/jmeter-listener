package cloud.testload.jmeter;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import cloud.testload.jmeter.config.influxdb.TestStartEndMeasurement;
import main.java.cloud.testload.jmeter.config.clickhouse.ClickHouseConfig;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterContextService.ThreadCounts;
import org.apache.jmeter.visualizers.backend.AbstractBackendListenerClient;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;

import ru.yandex.clickhouse.ClickHouseArray;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import cloud.testload.jmeter.JMPoint;



/**
 * Backend listener that writes JMeter metrics to ClickHouse directly.
 *
 * @author Alexander Babaev (minor changes and improvements)
 *
 */
public class ClickHouseBackendListenerClient extends AbstractBackendListenerClient implements Runnable {
    /**
     * Logger.
     */
    private static final Logger LOGGER = LoggingManager.getLoggerForClass();

    /**
     * Buffer.
     */
    private static final List<SampleResult> allSampleResults = new ArrayList<SampleResult>();

    /**
     * Parameter Keys.
     */
    private static final String KEY_USE_REGEX_FOR_SAMPLER_LIST = "useRegexForSamplerList";
    private static final String KEY_PROFILE_NAME = "profileName";
    private static final String KEY_RUN_ID = "runId";
    private static final String KEY_SAMPLERS_LIST = "samplersList";
    private static final String KEY_RECORD_SUB_SAMPLES = "recordSubSamples";
    private static final String KEY_RECORD_GROUP_BY = "groupBy";
    private static final String KEY_RECORD_GROUP_BY_COUNT = "groupByCountOrBatchSize";
    private static final String KEY_RECORD_LEVEL = "recordAdditionalDataLevel";


    /**
     * Constants.
     */
    private static final String SEPARATOR = ";";
    private static final int ONE_MS_IN_NANOSECONDS = 1000000;

    /**
     * Scheduler for periodic metric aggregation.
     */
    private ScheduledExecutorService scheduler;

    /**
     * Name of the test.
     */
    private String profileName;

    /**
     * A unique identifier for a single execution (aka 'run') of a load test.
     * In a CI/CD automated performance test, a Jenkins or Bamboo build id would be a good value for this.
     */
    private String runId;

    /**
     * List of samplers to record.
     */
    private String samplersList = "";

    /**
     * Regex if samplers are defined through regular expression.
     */
    private String regexForSamplerList;

    /**
     * Set of samplers to record.
     */
    private Set<String> samplersToFilter;

    /**
     * ClickHouse configuration.
     */
    ClickHouseConfig clickhouseConfig;

    /**
     * clickHouse DS.
     */
    private ClickHouseDataSource clickHouse;
    private Connection connection;

    /**
     * Random number generator
     */
    private Random randomNumberGenerator;

    /**
     * Indicates whether to record Subsamples
     */
    private boolean recordSubSamples;

    /**
     * Indicates whether to aggregate and level statistic
     */
    private boolean groupBy;
    private int groupByCount;
    private String recordLevel;

    /**
     * Processes sampler results.
     */
    public void handleSampleResults(List<SampleResult> sampleResults, BackendListenerContext context) {
        // Gather only regex results to array
        sampleResults.forEach(it -> {
            //write every filtered result to array
            if (checkFilter(it)) {
                getUserMetrics().add(it);
                allSampleResults.add(it);
            }
            if (recordSubSamples) {
                //write every filtered sub_result to array
                for (SampleResult subResult : it.getSubResults()) {
                    if (checkFilter(subResult)) {
                        getUserMetrics().add(subResult);
                        allSampleResults.add(subResult);
                    }
                }
            }
        });

        //Flush point(s) every group by Count
        if (allSampleResults.size() >= groupByCount) {
            if (groupBy) {
                flushAggregatedBatchPoints();
            }else flushBatchPoints();
        }
    };

    private boolean checkFilter(SampleResult sample){
        return 	((null != regexForSamplerList && sample.getSampleLabel().matches(regexForSamplerList)) || samplersToFilter.contains(sample.getSampleLabel()));
    }

    //Save one-item-array to DB
    private void flushBatchPoints() {
        try {
            PreparedStatement point = connection.prepareStatement("insert into jmresults (timestamp_sec, timestamp_millis, profile_name, run_id, thread_name, sample_label, points_count, errors_count, average_time, request, response)" +
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?)");
            allSampleResults.forEach(it -> {
                try {
                    point.setTimestamp(1,new Timestamp(it.getTimeStamp()));
                    point.setLong(2, it.getTimeStamp());
                    point.setString(3, profileName);
                    point.setString(4, runId);
                    point.setString(5, it.getThreadName());
                    point.setString(6, it.getSampleLabel());
                    point.setInt(7, 1);
                    point.setInt(8, it.getErrorCount());
                    point.setDouble(9, it.getTime());
                    switch (recordLevel){
                        case "debug":
                            point.setString(10, it.getSamplerData().toString());
                            point.setString(11, it.getResponseDataAsString());
                            break;
                        default:
                            point.setString(10, "");
                            point.setString(11, "");
                            break;
                    }
                    point.addBatch();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });

            point.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        allSampleResults.clear();
    }
    //Aggregate and Save array to DB
    private void flushAggregatedBatchPoints()
    {
        //group aggregation
        final Map<String,JMPoint> samplesTst = allSampleResults.stream().collect(Collectors.groupingBy(SampleResult::getSampleLabel,
                Collectors.collectingAndThen(Collectors.toList(), list -> {
                            long errorsCount = list.stream().collect(Collectors.summingLong(SampleResult::getErrorCount));
                            long count = list.stream().collect(Collectors.counting());
                            double average = list.stream().collect(Collectors.averagingDouble(SampleResult::getTime));
                            return new JMPoint("aggregate",errorsCount,count, average);
                        }
                )));
        //save aggregates to DB
        try {
            PreparedStatement point = connection.prepareStatement("insert into jmresults (timestamp_sec, timestamp_millis, profile_name, run_id, thread_name, sample_label, points_count, errors_count, average_time, request, response)" +
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?)");
            samplesTst.forEach((pointName,pointData)-> {
                try {
                    point.setTimestamp(1,new Timestamp(System.currentTimeMillis()));
                    point.setLong(2,  System.currentTimeMillis());
                    point.setString(3, profileName);
                    point.setString(4, runId);
                    point.setString(5, pointData.getThreadName());
                    point.setString(6, pointName);
                    point.setLong(7, pointData.getPointsCount());
                    point.setLong(8, pointData.getErrorCount());
                    point.setDouble(9, pointData.getAverageTimeInt());
                    point.setString(10, "");
                    point.setString(11, "");
                    point.addBatch();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });

            point.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        allSampleResults.clear();
    }

    /**
     * Abstract imp
     */
    public void run() {
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments arguments = new Arguments();
        arguments.addArgument(KEY_PROFILE_NAME, "TEST");
        arguments.addArgument(KEY_RUN_ID, "R001");
        arguments.addArgument(ClickHouseConfig.KEY_CLICKHOUSE_URL, ClickHouseConfig.DEFAULT_CLICKHOUSE_URL);
        arguments.addArgument(ClickHouseConfig.KEY_CLICKHOUSE_USER, "default");
        arguments.addArgument(ClickHouseConfig.KEY_CLICKHOUSE_PASSWORD, "");
        arguments.addArgument(ClickHouseConfig.KEY_CLICKHOUSE_DATABASE, ClickHouseConfig.DEFAULT_DATABASE);
        arguments.addArgument(KEY_SAMPLERS_LIST, ".*");
        arguments.addArgument(KEY_USE_REGEX_FOR_SAMPLER_LIST, "true");
        arguments.addArgument(KEY_RECORD_SUB_SAMPLES, "true");
        arguments.addArgument(KEY_RECORD_GROUP_BY, "false");
        arguments.addArgument(KEY_RECORD_GROUP_BY_COUNT, "100");
        arguments.addArgument(KEY_RECORD_LEVEL, "info");

        return arguments;
    }

    @Override
    public void setupTest(BackendListenerContext context) throws Exception {
        profileName = context.getParameter(KEY_PROFILE_NAME, "TEST");
        runId = context.getParameter(KEY_RUN_ID,"R001");
        groupBy = context.getBooleanParameter(KEY_RECORD_GROUP_BY, false);
        groupByCount = context.getIntParameter(KEY_RECORD_GROUP_BY_COUNT, 100);
        recordLevel=context.getParameter(KEY_RECORD_LEVEL,"info");

        setupClickHouseClient(context);
        createDatabaseIfNotExistent();
        parseSamplers(context);
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this, 1, 1, TimeUnit.SECONDS);
        // Indicates whether to write sub sample records to the database
        recordSubSamples = Boolean.parseBoolean(context.getParameter(KEY_RECORD_SUB_SAMPLES, "false"));
    }

    @Override
    public void teardownTest(BackendListenerContext context) throws Exception {
        LOGGER.info("Shutting down clickHouse scheduler...");
        if (groupBy) {
            flushAggregatedBatchPoints();
        }else flushBatchPoints();
        super.teardownTest(context);
    }

    /**
     * Setup clickHouse client.
     *
     * @param context
     *            {@link BackendListenerContext}.
     */
    private void setupClickHouseClient(BackendListenerContext context) {
        clickhouseConfig= new ClickHouseConfig(context);
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setCompress(true);
        clickHouse = new ClickHouseDataSource("jdbc:clickhouse://"+clickhouseConfig.getClickhouseURL(), properties);
        try {
            connection = clickHouse.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Creates the configured database in clickhouse if it does not exist yet.
     */
    private void createDatabaseIfNotExistent() {
        String dbtemplate="create table IF NOT EXISTS jmresults\n" +
                "(\n" +
                "\ttimestamp_sec DateTime,\n" +
                "\ttimestamp_millis UInt64,\n" +
                "\tprofile_name String,\n" +
                "\trun_id String,\n" +
                "\tthread_name String,\n" +
                "\tsample_label String,\n" +
                "\tpoints_count UInt64,\n" +
                "\terrors_count UInt64,\n" +
                "\taverage_time Float64,\n" +
                "\trequest String,\n" +
                "\tresponse String\n" +
                ")\n" +
                "engine = MergeTree()\n" +
                "ORDER BY (timestamp_sec,timestamp_millis,profile_name,run_id,sample_label)\n" +
                "PARTITION BY toYYYYMM(timestamp)";
        try {
            connection.createStatement().execute(dbtemplate);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //TODO: debug autocreation script
        /*
        * create table jmresults
(
	timestamp_sec DateTime,
	timestamp_millis UInt64,
	profile_name String,
	run_id String,
	thread_name String,
	sample_label String,
	points_count UInt64,
	errors_count UInt64,
	average_time Float64,
	request String,
	response String
)
engine = MergeTree()
ORDER BY (timestamp_sec,timestamp_millis,profile_name,run_id,sample_label)
PARTITION BY toYYYYMM(timestamp)
        * */


    }

    /**
     * Parses list of samplers.
     *
     * @param context
     *            {@link BackendListenerContext}.
     */
    private void parseSamplers(BackendListenerContext context) {
        samplersList = context.getParameter(KEY_SAMPLERS_LIST, "");
        samplersToFilter = new HashSet<String>();
        if (context.getBooleanParameter(KEY_USE_REGEX_FOR_SAMPLER_LIST, false)) {
            regexForSamplerList = samplersList;
        } else {
            regexForSamplerList = null;
            String[] samplers = samplersList.split(SEPARATOR);
            samplersToFilter = new HashSet<String>();
            for (String samplerName : samplers) {
                samplersToFilter.add(samplerName);
            }
        }
    }
}
