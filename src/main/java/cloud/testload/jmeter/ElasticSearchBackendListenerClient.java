package cloud.testload.jmeter;

import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jmeter.visualizers.backend.AbstractBackendListenerClient;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class ElasticSearchBackendListenerClient extends AbstractBackendListenerClient {
    private static final String BUILD_NUMBER        = "BuildNumber";
    private static final String ES_SCHEME           = "es.scheme";
    private static final String ES_HOST             = "es.host";
    private static final String ES_PORT             = "es.port";
    private static final String ES_INDEX            = "es.index";
    private static final String ES_TIMESTAMP        = "es.timestamp";
    private static final String ES_BULK_SIZE        = "es.bulk.size";
    private static final String ES_TIMEOUT_MS       = "es.timout.ms";
    private static final String ES_SAMPLE_FILTER    = "es.sample.filter";
    private static final String ES_TEST_MODE        = "es.test.mode";
    private static final String ES_AUTH_USER        = "es.xpack.user";
    private static final String ES_AUTH_PWD         = "es.xpack.password";
    private static final String ES_PARSE_REQ_HEADERS    = "es.parse.all.req.headers";
    private static final String ES_PARSE_RES_HEADERS    = "es.parse.all.res.headers";
    private static final String ES_RECORD_GROUP_BY = "es.groupby";
    private static final String ES_RECORD_GROUP_BY_COUNT = "es.groupby.count";
    private static final String ES_RECORD_SUBSAMPLES = "es.record.subsamples";
    private static final long DEFAULT_TIMEOUT_MS = 200L;
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchBackendListenerClient.class);

    private ElasticSearchMetricSender sender;
    private Set<String> modes;
    private Set<String> filters;
    private int buildNumber;
    private int bulkSize;
    private long timeoutMs;
    private boolean recordSubSamples=true;

    /**
     * Indicates whether to aggregate statistic
     */
    private boolean groupBy;
    private int groupByCount;

    /**
     * Buffer for aggregates
     */
    private static final List<SampleResult> allSampleResults = new ArrayList<SampleResult>();

    @Override
    public Arguments getDefaultParameters() {
        Arguments parameters = new Arguments();
        parameters.addArgument(ES_SCHEME, "http");
        parameters.addArgument(ES_HOST, null);
        parameters.addArgument(ES_PORT, "9200");
        parameters.addArgument(ES_INDEX, null);
        parameters.addArgument(ES_TIMESTAMP, "yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
        parameters.addArgument(ES_BULK_SIZE, "100");
        parameters.addArgument(ES_TIMEOUT_MS, Long.toString(DEFAULT_TIMEOUT_MS));
        parameters.addArgument(ES_SAMPLE_FILTER, null);
        parameters.addArgument(ES_TEST_MODE, "info");
        parameters.addArgument(ES_AUTH_USER, "");
        parameters.addArgument(ES_AUTH_PWD, "");
        parameters.addArgument(ES_PARSE_REQ_HEADERS, "false");
        parameters.addArgument(ES_PARSE_RES_HEADERS, "false");
        parameters.addArgument(ES_RECORD_SUBSAMPLES, "true");
        parameters.addArgument(ES_RECORD_GROUP_BY, "false");
        parameters.addArgument(ES_RECORD_GROUP_BY_COUNT, "200");
        return parameters;
    }

    @Override
    public void setupTest(BackendListenerContext context) throws Exception {
        try {
            this.groupBy = context.getBooleanParameter(ES_RECORD_GROUP_BY, false);
            this.groupByCount = context.getIntParameter(ES_RECORD_GROUP_BY_COUNT, 100);
            this.recordSubSamples = context.getBooleanParameter(ES_RECORD_SUBSAMPLES, true);
            this.filters = new HashSet<>();
            this.modes = new HashSet<>(Arrays.asList("info","debug","error","quiet"));
            this.bulkSize = Integer.parseInt(context.getParameter(ES_BULK_SIZE));
            this.timeoutMs = JMeterUtils.getPropDefault(ES_TIMEOUT_MS, DEFAULT_TIMEOUT_MS);
            this.buildNumber = (JMeterUtils.getProperty(ElasticSearchBackendListenerClient.BUILD_NUMBER) != null && !JMeterUtils.getProperty(ElasticSearchBackendListenerClient.BUILD_NUMBER).trim().equals("")) ? Integer.parseInt(JMeterUtils.getProperty(ElasticSearchBackendListenerClient.BUILD_NUMBER)) : 0;
            RestClient client = RestClient.builder(new HttpHost(context.getParameter(ES_HOST), Integer.parseInt(context.getParameter(ES_PORT)), context.getParameter(ES_SCHEME)))
                    .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(5000)
                    .setSocketTimeout((int) timeoutMs))
                    .setFailureListener(new RestClient.FailureListener() {
                        @Override
                        public void onFailure(Node node) {
                            throw new IllegalStateException();
                        }
                    })
                    .setMaxRetryTimeoutMillis(60000)
                    .build();

            this.sender = new ElasticSearchMetricSender(client, context.getParameter(ES_INDEX).toLowerCase() ,context.getParameter(ES_AUTH_USER), context.getParameter(ES_AUTH_PWD));
            this.sender.createIndex();

            checkTestMode(context.getParameter(ES_TEST_MODE));
            
            String[] filterArray = (context.getParameter(ES_SAMPLE_FILTER).contains(";")) ? context.getParameter(ES_SAMPLE_FILTER).split(";") : new String[] {context.getParameter(ES_SAMPLE_FILTER)};
            if(filterArray.length >= 1 && !filterArray[0].trim().equals("")) {
                for (String filter : filterArray) {
                    this.filters.add(filter.toLowerCase().trim());
                }
            }

            super.setupTest(context);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to connect to the ElasticSearch engine", e);
        }
    }

    @Override
    public void handleSampleResults(List<SampleResult> results, BackendListenerContext context) {

        results.forEach(it->{
            //write every filtered result to array
            if (validateSample(context, it)) {
                allSampleResults.add(it);
            }
            if (recordSubSamples) {
                //write every filtered sub_result to array
                for (SampleResult subResult : it.getSubResults()) {
                    if (validateSample(context, subResult)) {
                        allSampleResults.add(subResult);
                    }
                }
            }
        });

        //Flush point(s) every group by Count
        if (groupBy) {
            if (allSampleResults.size() >= groupByCount) {
                logger.info("SAVE ITEM TO AGGREGATE!");
                flushPoints(context);
            }
        } else flushPoint(context); //or every point
    }

    //Save one-item-array to bulk
    private void flushPoint(BackendListenerContext context)
    {
        allSampleResults.forEach(it -> {
            ElasticSearchMetric metric = new ElasticSearchMetric(it, context.getParameter(ES_TEST_MODE), context.getParameter(ES_TIMESTAMP), this.buildNumber, context.getBooleanParameter(ES_PARSE_REQ_HEADERS, false), context.getBooleanParameter(ES_PARSE_RES_HEADERS, false));
            try {
                this.sender.addToList(new Gson().toJson(metric.getMetric(context)));
            } catch (Exception e) {
                logger.error("The ElasticSearch Backend Listener was unable to a sampler to the list of samplers to send... More info in JMeter's console.");
                e.printStackTrace();
            }

            if(this.sender.getListSize() >= this.bulkSize) {
                try {
                    this.sender.sendRequest();
                } catch (Exception e) {
                    logger.error("Error occured while sending bulk request.", e);
                } finally {
                    this.sender.clearList();
                }
            }
        });
        allSampleResults.clear();
    }

    //Aggregate and Save array to DB
    private void flushPoints(BackendListenerContext context)
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
        //save aggregates to bulk
        samplesTst.forEach((pointName,pointData)-> {
            ElasticSearchMetric metric = new ElasticSearchMetric(pointName,pointData, context.getParameter(ES_TEST_MODE), context.getParameter(ES_TIMESTAMP), this.buildNumber, context.getBooleanParameter(ES_PARSE_REQ_HEADERS, false), context.getBooleanParameter(ES_PARSE_RES_HEADERS, false));
            try {
                this.sender.addToList(new Gson().toJson(metric.getAggregatedMetric(context)));
            } catch (Exception e) {
                logger.error("The ElasticSearch Backend Listener was unable to a sampler to the list of samplers to send... More info in JMeter's console.");
                e.printStackTrace();
            }
        });
        //so now we can clean array
        allSampleResults.clear();
        //and we can clear bulk
        if(this.sender.getListSize() >= this.bulkSize) {
            logger.info("SENDING!!!");
            try {
                this.sender.sendRequest();
            } catch (Exception e) {
                logger.error("Error occured while sending bulk request.", e);
            } finally {
                this.sender.clearList();
            }
        }
    }

    @Override
    public void teardownTest(BackendListenerContext context) throws Exception {
        //Flush point(s) every group
        if (groupBy) flushPoints(context);
        logger.info("FLUSH UNSAVED!");
        if(this.sender.getListSize() > 0) {
            this.sender.sendRequest();
        }
        this.sender.closeConnection();
        super.teardownTest(context);
    }

    /**
     * This method checks if the test mode is valid
     * @param mode The test mode as String
     */
    private void checkTestMode(String mode) {
        if(!this.modes.contains(mode)) {
            logger.warn("The parameter \"es.test.mode\" isn't set properly. Three modes are allowed: debug ,info, and quiet.");
            logger.warn(" -- \"debug\": sends request and response details to ElasticSearch. Info only sends the details if the response has an error.");
            logger.warn(" -- \"info\": should be used in production");
            logger.warn(" -- \"error\": should be used if you.");
            logger.warn(" -- \"quiet\": should be used if you don't care to have the details.");
        }
    }

    /**
     * This method will validate the current sample to see if it is part of the filters or not.
     * @param context The Backend Listener's context
     * @param sr The current SampleResult
     * @return true or false depending on whether or not the sample is valid
     */
    private boolean validateSample(BackendListenerContext context, SampleResult sr) {
        boolean validSample = false;
        String sampleLabel = sr.getSampleLabel().toLowerCase().trim();

        if(this.filters.size() == 0 || this.filters.contains(sampleLabel)) {
            validSample = (context.getParameter(ES_TEST_MODE).trim().equals("error") && sr.isSuccessful()) ? false : true;
        }

        return validSample;
    }
}
