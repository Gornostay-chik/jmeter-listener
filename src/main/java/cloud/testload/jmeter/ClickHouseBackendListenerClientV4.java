package cloud.testload.jmeter;

import main.java.cloud.testload.jmeter.config.clickhouse.ClickHouseConfig;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.visualizers.backend.AbstractBackendListenerClient;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.clickhouse.jdbc.ClickHouseDriver;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Backend listener that writes JMeter metrics to ClickHouse directly (Version 4).
 *
 * <p>
 *     This implementation is a reworked English version of V3. It persists {@link SampleResult}
 *     data produced by JMeter during a test run to ClickHouse using JDBC. The listener can:
 * </p>
 *
 * <ul>
 *   <li>Filter samplers by explicit names or a regular expression</li>
 *   <li>Optionally record sub-samples (e.g., HTTP redirects, embedded resources)</li>
 *   <li>Control the verbosity of stored data via record level: "debug", "info", "error", or "aggregate"</li>
 *   <li>Batch and/or aggregate samples before writing to ClickHouse for efficiency</li>
 *   <li>Optionally auto-create the database and tables on startup</li>
 * </ul>
 *
 * <h3>Record levels</h3>
 * <ul>
 *   <li><b>debug</b>: Persist full request and response payloads. Highest storage cost.</li>
 *   <li><b>info</b>: Strip request/response payloads for all samples.</li>
 *   <li><b>error</b>: Keep request/response only for samples with errors; strip for successful ones.</li>
 *   <li><b>aggregate</b>: Do not store individual samples. Group by sampler label and store
 *       aggregated counters (count, errors, average) instead.</li>
 * </ul>
 *
 * <h3>ClickHouse schema</h3>
 * <p>
 *     On startup, if {@code createDefinitions=true}, the listener will ensure the following objects exist:
 * </p>
 * <ul>
 *   <li><b>jmresults_data</b> (MergeTree): operational store with a TTL of 1 week</li>
 *   <li><b>jmresults</b> (Buffer): write buffer targeting {@code jmresults_data}</li>
 *   <li><b>jmresults_statistic</b> (Materialized View): aggregated archive over {@code jmresults_data}</li>
 * </ul>
 *
 * <h3>Batching and aggregation</h3>
 * <ul>
 *   <li>Non-aggregate: flush when buffer size ≥ {@code batchSize}</li>
 *   <li>Aggregate: flush when buffer size ≥ {@code groupByCount * batchSize}</li>
 * </ul>
 *
 * <h3>Thread-safety</h3>
 * <p>
 *     The internal buffer is a static {@link ArrayList}. JMeter typically invokes backend listeners on a single
 *     worker thread; if custom wiring introduces concurrency, guard access accordingly.
 * </p>
 *
 * <h3>Configuration in JMeter</h3>
 * <pre>
 *   Backend Listener → Backend Listener implementation:
 *     cloud.testload.jmeter.ClickHouseBackendListenerClientV4
 *
 *   Parameters:
 *     profileName=TEST
 *     runId=R001
 *     chUrl=localhost:8123
 *     chUser=default
 *     chPassword=
 *     chDatabase=default
 *     samplersList=.*
 *     useRegexForSamplerList=true
 *     recordSubSamples=true
 *     groupByCount=100
 *     batchSize=1000
 *     recordDataLevel=info   (debug|info|error|aggregate)
 *     createDefinitions=true
 * </pre>
 *
 * @author Generated from V3
 * @since 4.0
 */
public class ClickHouseBackendListenerClientV4 extends AbstractBackendListenerClient implements Runnable {
    /**
     * Logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseBackendListenerClientV4.class);

    /**
     * In-memory buffer of samples awaiting flush to ClickHouse.
     * For non-aggregate mode, each element corresponds to a single {@link SampleResult}.
     * For aggregate mode, raw samples are buffered and aggregated on flush.
     */
    private static final List<SampleResult> allSampleResults = new ArrayList<SampleResult>();

    /**
     * Parameter keys accepted via JMeter Backend Listener parameters.
     */
    private static final String KEY_USE_REGEX_FOR_SAMPLER_LIST = "useRegexForSamplerList";
    private static final String KEY_PROFILE_NAME = "profileName";
    private static final String KEY_RUN_ID = "runId";
    private static final String KEY_SAMPLERS_LIST = "samplersList";
    private static final String KEY_RECORD_SUB_SAMPLES = "recordSubSamples";
    private static final String KEY_RECORD_GROUP_BY_COUNT = "groupByCount";
    private static final String KEY_RECORD_BATCH_SIZE= "batchSize";
    private static final String KEY_RECORD_LEVEL = "recordDataLevel";
    private static final String KEY_CREATE_DEF = "createDefinitions";

    /**
     * Constants
     */
    private static final String SEPARATOR = ";";
    private static final int ONE_MS_IN_NANOSECONDS = 1000000;

    /**
     * Optional scheduler reserved for periodic tasks (e.g., time-based flushing).
     * Currently scheduled but the {@link #run()} method is a no-op placeholder.
     */
    private ScheduledExecutorService scheduler;

    /**
     * Logical test profile name (e.g., suite identifier). Stored as {@code profile_name}.
     */
    private String profileName;

    /**
     * Hostname of the JMeter engine executing the test. Stored as {@code hostname}.
     */
    private String hostname;

    /**
     * Unique identifier of the test execution (CI build id, deployment id, etc.). Stored as {@code run_id}.
     */
    private String runId;

    /**
     * Sampler list configured via JMeter parameter. Either a regex or a semicolon-separated list.
     */
    private String samplersList = "";

    /**
     * Regex pattern used when {@link #KEY_USE_REGEX_FOR_SAMPLER_LIST} is {@code true}.
     */
    private String regexForSamplerList;

    /**
     * Explicit sampler labels to include when regex is disabled.
     */
    private Set<String> samplersToFilter;

    /**
     * ClickHouse connection configuration constructed from JMeter parameters.
     */
    ClickHouseConfig clickhouseConfig;

    /**
     * JDBC connection used for all writes. Created during {@link #setupTest(BackendListenerContext)}.
     */
    private Connection connection;

    /**
     * Random number generator (reserved for future use: sampling, jitter, etc.).
     */
    private Random randomNumberGenerator;

    /**
     * If true, include {@link SampleResult#getSubResults()} in processing in addition to parent samples.
     */
    private boolean recordSubSamples;

    /**
     * If true, attempt to create the ClickHouse database and tables on startup if they do not exist.
     */
    private boolean createDefinitions;

    /**
     * Aggregation and batching thresholds. See class-level documentation for details.
     */
    private int groupByCount;
    private int batchSize;
    private String recordLevel;

    /**
     * Process a batch of {@link SampleResult}s provided by JMeter.
     *
     * <p>Steps:</p>
     * <ol>
     *   <li>Apply sampler filter (regex or explicit set)</li>
     *   <li>Apply record level (strip or keep payloads)</li>
     *   <li>Optionally include sub-samples</li>
     *   <li>Append to in-memory buffer</li>
     *   <li>Flush to ClickHouse if buffer threshold is reached</li>
     * </ol>
     */
    public void handleSampleResults(List<SampleResult> sampleResults, BackendListenerContext context) {
        // Add only filtered results to the buffer
        sampleResults.forEach(it -> {
            if (checkFilter(it)) {
                switch (recordLevel){
                    case "info":
                        it.setSamplerData("");
                        it.setResponseData("");
                        break;
                    case "error":
                        if (it.getErrorCount()==0) {
                            it.setSamplerData("");
                            it.setResponseData("");
                        }
                        break;
                    default:
                        break;
                }
                allSampleResults.add(it);
            }
            if (recordSubSamples) {
                // Add filtered sub-samples
                for (SampleResult subResult : it.getSubResults()) {
                    if (checkFilter(subResult)) {
                        switch (recordLevel){
                            case "info":
                                subResult.setSamplerData("");
                                subResult.setResponseData("");
                                break;
                            case "error":
                                if (it.getErrorCount()==0) {
                                    subResult.setSamplerData("");
                                    subResult.setResponseData("");
                                }
                                break;
                            default:
                                break;
                        }
                        allSampleResults.add(subResult);
                    } else {
                        if ((recordLevel.equals("error")&&(it.getErrorCount()>0)&&(subResult.getErrorCount()>0))){
                            allSampleResults.add(subResult);
                        }
                    }
                }
            }
        });

        // Flush on thresholds. In aggregate mode we require a larger buffer to produce meaningful groups.
        if (recordLevel.equals("aggregate")) {
            if (allSampleResults.size() >= groupByCount*batchSize) {
                flushAggregatedBatchPoints();
            }
        } else {
            if (allSampleResults.size() >= batchSize) {
                flushBatchPoints();
            }
        }
    };

    private boolean checkFilter(SampleResult sample){
        return     ((null != regexForSamplerList && sample.getSampleLabel().matches(regexForSamplerList)) || samplersToFilter.contains(sample.getSampleLabel()));
    }

    /**
     * Flush non-aggregated samples to ClickHouse.
     *
     * <p>Each buffered sample becomes a single row in {@code jmresults} (buffer → {@code jmresults_data}).
     * The following mapping is used:</p>
     * <ul>
     *   <li>timestamp_sec: {@link SampleResult#getTimeStamp()} converted to SQL {@link Timestamp}</li>
     *   <li>timestamp_millis: raw {@link SampleResult#getTimeStamp()}</li>
     *   <li>profile_name, run_id, hostname: from listener configuration</li>
     *   <li>thread_name, sample_label: from sample</li>
     *   <li>points_count: 1 (one row per sample)</li>
     *   <li>errors_count: {@link SampleResult#getErrorCount()}</li>
     *   <li>average_time: {@link SampleResult#getTime()} (elapsed time)</li>
     *   <li>request/response: kept or stripped per record level</li>
     *   <li>res_code: {@link SampleResult#getResponseCode()}</li>
     * </ul>
     */
    private void flushBatchPoints() {
        try {
            PreparedStatement point = connection.prepareStatement("INSERT INTO jmresults (timestamp_sec, timestamp_millis, profile_name, run_id, hostname, thread_name, sample_label, points_count, errors_count, average_time, request, response, res_code)" +
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)");
            allSampleResults.forEach(it -> {
                try {
                    point.setTimestamp(1,new Timestamp(it.getTimeStamp()));
                    point.setLong(2, it.getTimeStamp());
                    point.setString(3, profileName);
                    point.setString(4, runId);
                    point.setString(5, hostname);
                    point.setString(6, it.getThreadName());
                    point.setString(7, it.getSampleLabel());
                    point.setInt(8, 1);
                    point.setInt(9, it.getErrorCount());
                    point.setDouble(10, it.getTime());
                    switch (recordLevel){
                        case "debug":
                            point.setString(11, it.getSamplerData());
                            point.setString(12, it.getResponseDataAsString());
                            break;
                        case "error":
                            if (it.getErrorCount()>0) {
                                point.setString(11, it.getSamplerData());
                                point.setString(12, it.getResponseDataAsString());
                            }
                            else {
                                point.setString(11, "");
                                point.setString(12, "");
                            }
                            break;
                        default:
                            point.setString(11, "");
                            point.setString(12, "");
                            break;
                    }
                    point.setString(13,it.getResponseCode());
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
     * Aggregate buffered samples by {@code sample_label} and flush aggregates to ClickHouse.
     *
     * <p>For each group the following metrics are computed:</p>
     * <ul>
     *   <li>points_count: total number of samples</li>
     *   <li>errors_count: sum of {@link SampleResult#getErrorCount()}</li>
     *   <li>average_time: arithmetic average of {@link SampleResult#getTime()}</li>
     * </ul>
     *
     * <p>Payload fields (request/response) are intentionally left empty in aggregate mode.</p>
     */
    private void flushAggregatedBatchPoints()
    {
        // Group aggregation
        final Map<String, JMPoint> samplesTst = allSampleResults.stream().collect(Collectors.groupingBy(SampleResult::getSampleLabel,
                Collectors.collectingAndThen(Collectors.toList(), list -> {
                            long errorsCount = list.stream().collect(Collectors.summingLong(SampleResult::getErrorCount));
                            long count = list.stream().collect(Collectors.counting());
                            double average = list.stream().collect(Collectors.averagingDouble(SampleResult::getTime));
                            return new JMPoint("aggregate",errorsCount,count, average);
                        }
                )));
        // Save aggregates to DB
        try {
            PreparedStatement point = connection.prepareStatement("INSERT INTO jmresults (timestamp_sec, timestamp_millis, profile_name, run_id, hostname, thread_name, sample_label, points_count, errors_count, average_time, request, response)" +
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)");
            samplesTst.forEach((pointName,pointData)-> {
                try {
                    point.setTimestamp(1,new Timestamp(System.currentTimeMillis()));
                    point.setLong(2,  System.currentTimeMillis());
                    point.setString(3, profileName);
                    point.setString(4, runId);
                    point.setString(5, hostname);
                    point.setString(6, pointData.getThreadName());
                    point.setString(7, pointName);
                    point.setLong(8, pointData.getPointsCount());
                    point.setLong(9, pointData.getErrorCount());
                    point.setDouble(10, pointData.getAverageTimeInt());
                    point.setString(11, "");
                    point.setString(12, "");
                    point.setString(13, "");
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
     * Runnable hook reserved for future time-based flushing or maintenance.
     * Currently a no-op; flush is purely size/level driven in this version.
     */
    public void run() {
    }

    @Override
    public Arguments getDefaultParameters() {
        // Default values chosen to be safe for local development and broad test plans.
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
        arguments.addArgument(KEY_RECORD_GROUP_BY_COUNT, "100");
        arguments.addArgument(KEY_RECORD_BATCH_SIZE, "1000");
        arguments.addArgument(KEY_RECORD_LEVEL, "info");
        arguments.addArgument(KEY_CREATE_DEF, "true");
        return arguments;
    }

    @Override
    public void setupTest(BackendListenerContext context) throws Exception {
        profileName = context.getParameter(KEY_PROFILE_NAME, "TEST");
        runId = context.getParameter(KEY_RUN_ID,"R001");
        groupByCount = context.getIntParameter(KEY_RECORD_GROUP_BY_COUNT, 100);
        batchSize = context.getIntParameter(KEY_RECORD_BATCH_SIZE, 1000);
        recordLevel=context.getParameter(KEY_RECORD_LEVEL,"info");
        createDefinitions=Boolean.parseBoolean(context.getParameter(KEY_CREATE_DEF,"true"));
        hostname=getHostname();

        setupClickHouseClientWithoutDatabase(context);
        createDatabaseIfNotExistent();
        setupClickHouseClient(context);
        parseSamplers(context);
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this, 1, 1, TimeUnit.SECONDS);
        // Whether to write sub-sample records to the database
        recordSubSamples = Boolean.parseBoolean(context.getParameter(KEY_RECORD_SUB_SAMPLES, "false"));
    }

    @Override
    public void teardownTest(BackendListenerContext context) throws Exception {
        LOGGER.info("Shutting down ClickHouse scheduler...");
        if (recordLevel.equals("aggregate")) {
            flushAggregatedBatchPoints();
        } else {
            flushBatchPoints();
        }
        super.teardownTest(context);
    }

    /**
     * Setup ClickHouse client.
     *
     * <p>Builds a JDBC URL in the form {@code jdbc:clickhouse://<host:port>/<database>} and opens
     * a single {@link Connection} for subsequent prepared statements. Compression is enabled.</p>
     *
     * @param context {@link BackendListenerContext} providing connection parameters.
     */
    private void setupClickHouseClient(BackendListenerContext context) {
        // Initialize configuration from context
        clickhouseConfig = new ClickHouseConfig(context);

        // Prepare connection properties
        Properties properties = new Properties();
        properties.setProperty("compress", "true");
        properties.setProperty("user", clickhouseConfig.getClickhouseUser());
        properties.setProperty("password", clickhouseConfig.getClickhousePassword());

        // Build URL with database in the path
        String url = "jdbc:clickhouse://" + clickhouseConfig.getClickhouseURL() + "/" + clickhouseConfig.getClickhouseDatabase();

        try {
            // Explicit driver registration (if required)
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");

            // Get connection via DriverManager
            connection = DriverManager.getConnection(url, properties);
        } catch (ClassNotFoundException e) {
            System.err.println("ClickHouse driver not found. Ensure the dependency is present.");
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Error establishing ClickHouse connection.");
            e.printStackTrace();
        }
    }

    /**
     * Setup a ClickHouse connection without specifying the database in the URL.
     *
     * <p>Used for initial DDL to create the database itself. A separate connection with a
     * database-qualified URL is opened right after for normal writes.</p>
     */
    private void setupClickHouseClientWithoutDatabase(BackendListenerContext context) {
        // Initialize configuration from context
        clickhouseConfig = new ClickHouseConfig(context);

        // Prepare connection properties
        Properties properties = new Properties();
        properties.setProperty("compress", "true");
        properties.setProperty("user", clickhouseConfig.getClickhouseUser());
        properties.setProperty("password", clickhouseConfig.getClickhousePassword());

        // Build URL without database
        String url = "jdbc:clickhouse://" + clickhouseConfig.getClickhouseURL();

        try {
            // Register ClickHouse driver explicitly (if not auto-loaded)
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");

            // Get connection via DriverManager with given properties
            connection = DriverManager.getConnection(url, properties);
        } catch (ClassNotFoundException e) {
            System.err.println("ClickHouse driver not found. Check dependencies.");
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Error establishing ClickHouse connection.");
            e.printStackTrace();
        }
    }

    /**
     * Create database and tables in ClickHouse on demand.
     *
     * <p>Objects created:</p>
     * <ul>
     *   <li>{@code <db>.jmresults_data}: MergeTree, partitioned by day, TTL 1 week</li>
     *   <li>{@code <db>.jmresults}: Buffer engine writing to {@code jmresults_data}</li>
     *   <li>{@code <db>.jmresults_statistic}: Materialized view aggregating the operational data</li>
     * </ul>
     */
    private void createDatabaseIfNotExistent() {

        String dbtemplate_database="create database IF NOT EXISTS " + clickhouseConfig.getClickhouseDatabase();

        String dbtemplate_data="create table IF NOT EXISTS " +
                clickhouseConfig.getClickhouseDatabase()+".jmresults_data\n" +
                "(\n" +
                "\ttimestamp_sec DateTime,\n" +
                "\ttimestamp_millis UInt64,\n" +
                "\tprofile_name LowCardinality(String),\n" +
                "\trun_id LowCardinality(String),\n" +
                "\thostname LowCardinality(String),\n" +
                "\tthread_name LowCardinality(String),\n" +
                "\tsample_label LowCardinality(String),\n" +
                "\tpoints_count UInt64,\n" +
                "\terrors_count UInt64,\n" +
                "\taverage_time Float64,\n" +
                "\trequest String,\n" +
                "\tresponse String,\n" +
                "\tres_code LowCardinality(String)\n" +
                ")\n" +
                "engine = MergeTree\n" +
                "PARTITION BY toYYYYMMDD(timestamp_sec)\n" +
                "ORDER BY (timestamp_sec)\n" +
                "TTL timestamp_sec + INTERVAL 1 WEEK\n" +
                "SETTINGS index_granularity = 8192";

        String dbtemplate_stats="CREATE MATERIALIZED VIEW IF NOT EXISTS " +
                clickhouseConfig.getClickhouseDatabase()+".jmresults_statistic (\n" +
                "`timestamp_sec` DateTime Codec(LZ4HC),\n" +
                "`timestamp_millis` UInt64 Codec(LZ4HC),\n" +
                "`profile_name` LowCardinality(String),\n" +
                "`run_id` LowCardinality(String),\n" +
                "`thread_name` LowCardinality(String),\n" +
                "`hostname` String Codec(LZ4HC),\n" +
                "`sample_label` LowCardinality(String),\n" +
                "`points_count` UInt64 Codec(LZ4HC),\n" +
                "`errors_count` UInt64 Codec(LZ4HC),\n" +
                "`average_time` Float64 Codec(LZ4HC),\n" +
                "`res_code` LowCardinality(String))\n" +
                "    ENGINE = MergeTree\n" +
                "        PARTITION BY toYYYYMM(timestamp_sec)\n" +
                "        ORDER BY (timestamp_sec,\n" +
                "                  profile_name,\n" +
                "                  run_id,\n" +
                "                  hostname,\n" +
                "                  sample_label,\n" +
                "                  thread_name,\n" +
                "                  res_code)\n" +
                "        SETTINGS index_granularity = 8192\n" +
                "AS\n" +
                "SELECT timestamp_sec,\n" +
                "       timestamp_millis,\n" +
                "       profile_name,\n" +
                "       run_id,\n" +
                "       thread_name,\n" +
                "       hostname,\n" +
                "       sample_label,\n" +
                "       points_count,\n" +
                "       errors_count,\n" +
                "       average_time,\n" +
                "       res_code\n" +
                "FROM " +
                clickhouseConfig.getClickhouseDatabase()+".jmresults_data";

        String dbtemplate_buff="create table IF NOT EXISTS " +
                clickhouseConfig.getClickhouseDatabase()+".jmresults\n" +
                "(\n" +
                "\ttimestamp_sec DateTime,\n" +
                "\ttimestamp_millis UInt64,\n" +
                "\tprofile_name LowCardinality(String),\n" +
                "\trun_id LowCardinality(String),\n" +
                "\thostname LowCardinality(String),\n" +
                "\tthread_name LowCardinality(String),\n" +
                "\tsample_label LowCardinality(String),\n" +
                "\tpoints_count UInt64,\n" +
                "\terrors_count UInt64,\n" +
                "\taverage_time Float64,\n" +
                "\trequest String,\n" +
                "\tresponse String,\n" +
                "\tres_code LowCardinality(String)\n" +
                ")\n" +
                "engine = Buffer("+clickhouseConfig.getClickhouseDatabase()+", jmresults_data, 16, 10, 60, 10000, 100000, 1000000, 10000000)";
        try {
            if (createDefinitions) {
                connection.createStatement().execute(dbtemplate_database);
                connection.createStatement().execute(dbtemplate_data);
                connection.createStatement().execute(dbtemplate_buff);
                connection.createStatement().execute(dbtemplate_stats);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Parse sampler list configuration.
     *
     * <p>When {@code useRegexForSamplerList=true}, {@code samplersList} is interpreted as a Java regex.
     * Otherwise it is parsed as a semicolon-separated list of exact sampler labels.</p>
     *
     * @param context {@link BackendListenerContext}.
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

    /**
     * Resolve and return the local host name used for the {@code hostname} column.
     */
    private static String getHostname() throws UnknownHostException {
        InetAddress iAddress = InetAddress.getLocalHost();
        String hostName = iAddress.getHostName();
        return hostName;
    }
}


