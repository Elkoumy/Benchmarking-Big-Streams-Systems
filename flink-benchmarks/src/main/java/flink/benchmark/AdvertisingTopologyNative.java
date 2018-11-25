package flink.benchmark; /**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */


import ee.ut.cs.dsg.efficientSWAG.Enumerators;
import org.apache.flink.api.common.functions.*;
//import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * To Run:  flink run target/flink-benchmarks-0.1.0-AdvertisingTopologyNative.jar  --confPath "../conf/benchmarkConf.yaml"
 */
public class AdvertisingTopologyNative {

    private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopologyNative.class);

    private static Random rand = new Random();
    public static void main(final String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        Map conf = Utils.findAndReadConfigFile(parameterTool.getRequired("confPath"), true);
        int kafkaPartitions = ((Number)conf.get("kafka.partitions")).intValue();
        int hosts = ((Number)conf.get("process.hosts")).intValue();
        int cores = ((Number)conf.get("process.cores")).intValue();

        ParameterTool flinkBenchmarkParams = ParameterTool.fromMap(getFlinkConfs(conf));

        LOG.info("conf: {}", conf);
        LOG.info("Parameters used: {}", flinkBenchmarkParams.toMap());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("172.17.77.47", 6123, "C:\\Gamal Elkoumy\\PhD\\OneDrive - Tartu Ülikool\\Stream Processing\\Source Code and Example\\Benchmarking-gamal-version\\redisTest\\out\\artifacts\\redisTest_jar\\redisTest.jar");
        env.getConfig().setGlobalJobParameters(flinkBenchmarkParams);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // Set the buffer timeout (default 100)
        // Lowering the timeout will lead to lower latencies, but will eventually reduce throughput.
        env.setBufferTimeout(flinkBenchmarkParams.getLong("flink.buffer-timeout", 100));

        if(flinkBenchmarkParams.has("flink.checkpoint-interval")) {
            // enable checkpointing for fault tolerance
            env.enableCheckpointing(flinkBenchmarkParams.getLong("flink.checkpoint-interval", 1000));
        }
        // set default parallelism for all operators (recommended value: number of available worker CPU cores in the cluster (hosts * cores))
        env.setParallelism(hosts * cores);

        DataStream<String> messageStream = env
                .addSource(new FlinkKafkaConsumer011<String>(
                        flinkBenchmarkParams.getRequired("topic"),
                        new SimpleStringSchema(),
                        flinkBenchmarkParams.getProperties())).setParallelism(Math.min(hosts * cores, kafkaPartitions));



        /*****************************
         adding metrics for the log
         *****************************/
//
//        messageStream= messageStream.map(new MyMapper());
//        messageStream= messageStream.map(new ThroughputRecorder());


//messageStream
//                .rebalance()
//                // Parse the String as JSON
//                .flatMap(new DeserializeBolt())
//                //Filter the records if event type is "view"
//                .filter(new EventFilterBolt())
//                // project the event
//                .<Tuple2<String, String>>project(2, 5)
//                // perform join with redis data
//                .flatMap(new RedisJoinBolt())
//                .flatMap(new FormatConvert())
//        .assignTimestampsAndWatermarks(
//                new AscendingTimestampExtractor<Tuple5<String, String, String, Double,Long>>() {
//
//                    public long extractAscendingTimestamp(Tuple5<String, String, String, Double,Long> element) {
//                        return Long.parseLong(element.f2);
//                    }
//
//                }
//        )
//                .keyBy(0)
//        .timeWindow(Time.of(1, SECONDS), Time.of(1, SECONDS),1, Enumerators.Operator.MEDIAN_VEB)
//        .sum(3)
//        .flatMap(new FormatRestore())
//                .flatMap(new CampaignProcessor())
// ;


/*************************************************************************
 * Window calculations
 */
//        config = conf;

        args[0]="/root/stream-benchmarking/conf/benchmarkConf.yaml";
        BenchmarkConfig config = BenchmarkConfig.fromArgs(args);

        DataStream<String> rawMessageStream = streamSource(config, env);

        // log performance
        rawMessageStream.flatMap(new ThroughputLogger<String>(240, 1000000));

//        DataStream<Tuple2<String, String>> joinedAdImpressions = rawMessageStream
//                .flatMap(new DeserializeBolt())
//                .filter(new EventFilterBolt())
//                .<Tuple2<String, String>>project(2, 5) //ad_id, event_time
//                .flatMap(new RedisJoinBolt()) // campaign_id, event_time
//                .assignTimestampsAndWatermarks(new AdTimestampExtractorNewer()); // extract timestamps and generate watermarks from event_time


//        WindowedStream<Tuple3<String, String, Long>, Tuple, TimeWindow> windowStream = rawMessageStream
//                .flatMap(new DeserializeBolt())
//                .filter(new EventFilterBolt())
//                .<Tuple2<String, String>>project(2, 5) //ad_id, event_time
//                .flatMap(new RedisJoinBolt()) // campaign_id, event_time
//                .assignTimestampsAndWatermarks(new AdTimestampExtractorNewer())
//                .map( new MapToImpressionCount())
//                .keyBy(0) // campaign_id
//                .timeWindow(Time.of(config.windowSize, TimeUnit.MILLISECONDS));
//
//        // set a custom trigger
//        windowStream.trigger(new EventAndProcessingTimeTrigger());
//
//        // campaign_id, window end time, count
//        DataStream<Tuple3<String, String, Long>> result =
//                windowStream.apply(sumReduceFunction(), sumWindowFunction());

        DataStream<Tuple3<String, String, Long>> result=
                rawMessageStream
                        .flatMap(new DeserializeBolt())
                        .filter(new EventFilterBolt())
                        .<Tuple2<String, String>>project(2, 5) //ad_id, event_time
                        .flatMap(new RedisJoinBolt()) // campaign_id, ad_id, event_time

                        .assignTimestampsAndWatermarks(new AdTimestampExtractorNewer())
                        .flatMap(new FormatConvert())
//////                .assignTimestampsAndWatermarks(
//////                        new AscendingTimestampExtractor<Tuple5<String,String,String,Double, Long>>() {
//////
//////                            //																		 @Override
//////                            public long extractAscendingTimestamp(Tuple5<String, String, String, Double,Long> element) {
//////                                return Long.parseLong(element.f2);
//////                            }
//////                        }
//////                )
                        .keyBy(0)
//                .timeWindow(Time.of(2500, MILLISECONDS), Time.of(500, MILLISECONDS),3, Enumerators.Operator.STANDARD_DEVIATION)
                        .timeWindow( Time.of(config.windowSize, TimeUnit.MILLISECONDS),Time.of(config.windowSize, TimeUnit.MILLISECONDS),3, Enumerators.Operator.STANDARD_DEVIATION)
                        .sum(3)
                        .flatMap(new FormatRestore())

                ;
//
//
//        // write result to redis
//        if (config.getParameters().has("add.result.sink.optimized")) {
//            result.addSink(new RedisResultSinkOptimized(config));
//        } else {
//            result.addSink(new RedisResultSink(config));
//        }
//
///**
// * ***********************************************************************
// */

        result.print();

        env.execute();
    }


    public static class FormatConvert implements FlatMapFunction<Tuple3<String, String, String>, Tuple5<String, String, String, Double,Long>> {

        Random rand = new Random();

        public void flatMap(Tuple3<String, String, String> input, Collector<Tuple5<String, String, String, Double,Long>> collector) throws Exception {
            collector.collect(new Tuple5(input.f0,input.f2,input.f1, rand.nextDouble(), 1L));
        }
    }

    public static class FormatRestore implements FlatMapFunction<Tuple5<String, String, String, Double,Long>, Tuple3<String, String, Long>> {

        public void flatMap(Tuple5<String, String, String, Double,Long> input, Collector<Tuple3<String, String, Long>> collector) throws Exception {
            collector.collect(new Tuple3(input.f0, input.f1, input.f4));
        }
    }


    public static class DeserializeBolt implements
            FlatMapFunction<String, Tuple7<String, String, String, String, String, String, String>> {

        //        @Override
        public void flatMap(String input, Collector<Tuple7<String, String, String, String, String, String, String>> out)
                throws Exception {
            JSONObject obj = new JSONObject(input);
            Tuple7<String, String, String, String, String, String, String> tuple =
                    new Tuple7<String, String, String, String, String, String, String>(
                            obj.getString("user_id"),
                            obj.getString("page_id"),
                            obj.getString("ad_id"),
                            obj.getString("ad_type"),
                            obj.getString("event_type"),
                            obj.getString("event_time"),
                            obj.getString("ip_address"));
            out.collect(tuple);
        }
    }


    /********************
     * Adding metric class
     ********************/

    public static class MyMapper extends RichMapFunction<String, String> {
        private transient Counter counter;

        @Override
        public void open(Configuration config) {
            this.counter = getRuntimeContext()
                    .getMetricGroup()
                    .counter("myCounter");
        }

        @Override
        public String map(String value) throws Exception {
            this.counter.inc();
            return value;
        }
    }
    public static class ThroughputRecorder  extends RichMapFunction<String, String> {



        private transient Meter meter;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.meter = getRuntimeContext()
                    .getMetricGroup()
                    .meter("throughput", new MeterView(5));
//                    .meter("throughput", new MeterV(new com.codahale.metrics.Meter()));
        }

        @Override
        public String map(String value) throws Exception {
            this.meter.markEvent();
            return value;
        }
    }


    public static class EventFilterBolt implements
            FilterFunction<Tuple7<String, String, String, String, String, String, String>> {
        //        @Override
        public boolean filter(Tuple7<String, String, String, String, String, String, String> tuple) throws Exception {
            return tuple.getField(4).equals("view");
        }
    }

    public static class EventFilterBoltGamal implements
            FilterFunction<Tuple3<Long, String, Double>> {
        //        @Override
        public boolean filter(Tuple3<Long, String, Double> tuple) throws Exception {
            return tuple.getField(4).equals("view");
        }
    }



    public static final class RedisJoinBoltGamal extends RichFlatMapFunction<Tuple3<Long, String, Double>, Tuple3<Long, String, Double>> {

        RedisAdCampaignCache redisAdCampaignCache;

        @Override
        public void open(Configuration parameters) {
            //initialize jedis
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            parameterTool.getRequired("jedis_server");
            LOG.info("Opening connection with Jedis to {}", parameterTool.getRequired("jedis_server"));
            this.redisAdCampaignCache = new RedisAdCampaignCache(parameterTool.getRequired("jedis_server"));
            this.redisAdCampaignCache.prepare();
        }

        @Override
        public void flatMap(Tuple3<Long, String, Double> input,
                            Collector<Tuple3<Long, String, Double>> out) throws Exception {
            String ad_id = input.getField(1);
            String campaign_id = this.redisAdCampaignCache.execute(ad_id);
            if(campaign_id == null) {
                return;
            }

            Tuple3<Long, String, Double> tuple = new Tuple3<Long, String, Double>(
                    (Long)    input.getField(0),
                    (String)  campaign_id,
                    (Double) input.getField(2));
            out.collect(tuple);
        }
    }
    public static final class RedisJoinBolt extends RichFlatMapFunction<Tuple2<String, String>, Tuple3<String, String, String>> {

        RedisAdCampaignCache redisAdCampaignCache;

        @Override
        public void open(Configuration parameters) {
            //initialize jedis
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            parameterTool.getRequired("jedis_server");
            LOG.info("Opening connection with Jedis to {}", parameterTool.getRequired("jedis_server"));
            this.redisAdCampaignCache = new RedisAdCampaignCache(parameterTool.getRequired("jedis_server"));
            this.redisAdCampaignCache.prepare();
        }

        @Override
        public void flatMap(Tuple2<String, String> input,
                            Collector<Tuple3<String, String, String>> out) throws Exception {
            String ad_id = input.getField(0);
            String campaign_id = this.redisAdCampaignCache.execute(ad_id);
            if(campaign_id == null) {
                return;
            }

            Tuple3<String, String, String> tuple = new Tuple3<String, String, String>(
                    campaign_id,
                    (String) input.getField(0),
                    (String) input.getField(1));
            out.collect(tuple);
        }
    }



    public static class DeserializeBoltGamal implements
            FlatMapFunction<String, Tuple3<Long, String, Double>> {

        //        @Override
        public void flatMap(String input, Collector<Tuple3<Long, String, Double>> out)
                throws Exception {
            JSONObject obj = new JSONObject(input);

            Tuple3<Long, String, Double> tuple =
                    new Tuple3<Long, String, Double>(
                            Long.parseLong(obj.getString("event_time")),
                            obj.getString("ad_id"),
                            rand.nextDouble()

                    );
            out.collect(tuple);
        }
    }



    public static final class MyFlatMap extends RichFlatMapFunction<Tuple2<String, String>, Tuple3<String, String, String>> {



        @Override
        public void open(Configuration parameters) {

        }

        @Override
        public void flatMap(Tuple2<String, String> input,
                            Collector<Tuple3<String, String, String>> out) throws Exception {
//            String ad_id = input.getField(0);


            Random rand = new Random();

            int  n = rand.nextInt(1000000000) + 1;
            String s_n = n+"";
            Tuple3<String, String, String> tuple = new Tuple3<String, String, String>(
                    s_n,
                    (String) input.getField(0),
                    (String) input.getField(1));
            out.collect(tuple);
        }
    }



    public static class CampaignProcessorGamal extends RichFlatMapFunction<Tuple3<Long, String, Double>, String> {

        CampaignProcessorCommon campaignProcessorCommon;

        @Override
        public void open(Configuration parameters) {
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            parameterTool.getRequired("jedis_server");
            LOG.info("Opening connection with Jedis to {}", parameterTool.getRequired("jedis_server"));

            this.campaignProcessorCommon = new CampaignProcessorCommon(parameterTool.getRequired("jedis_server"),Long.valueOf(parameterTool.get("time.divisor")));
            this.campaignProcessorCommon.prepare();
        }

        @Override
        public void flatMap(Tuple3<Long, String, Double> tuple, Collector<String> out) throws Exception {

            String campaign_id = tuple.getField(1);
            String event_time =  tuple.getField(0)+"";
            this.campaignProcessorCommon.execute(campaign_id, event_time);
        }
    }

    public static class CampaignProcessor extends RichFlatMapFunction<Tuple3<String, String, String>, String> {

        CampaignProcessorCommon campaignProcessorCommon;

        @Override
        public void open(Configuration parameters) {
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            parameterTool.getRequired("jedis_server");
            LOG.info("Opening connection with Jedis to {}", parameterTool.getRequired("jedis_server"));

            this.campaignProcessorCommon = new CampaignProcessorCommon(parameterTool.getRequired("jedis_server"),Long.valueOf(parameterTool.get("time.divisor")));
            this.campaignProcessorCommon.prepare();
        }

        @Override
        public void flatMap(Tuple3<String, String, String> tuple, Collector<String> out) throws Exception {

            String campaign_id = tuple.getField(0);
            String event_time =  tuple.getField(2);
            this.campaignProcessorCommon.execute(campaign_id, event_time);
        }
    }









    private static Map<String, String> getFlinkConfs(Map conf) {
        String kafkaBrokers = getKafkaBrokers(conf);
        String zookeeperServers = getZookeeperServers(conf);

        Map<String, String> flinkConfs = new HashMap<String, String>();
        flinkConfs.put("topic", getKafkaTopic(conf));
        flinkConfs.put("bootstrap.servers", kafkaBrokers);
        flinkConfs.put("zookeeper.connect", zookeeperServers);
        flinkConfs.put("jedis_server", getRedisHost(conf));
        flinkConfs.put("time.divisor", getTimeDivisor(conf));
        flinkConfs.put("group.id", "myGroup");

        return flinkConfs;
    }

//    private static String getTimeDivisor(Map conf) {
//        if(!conf.containsKey("time.divisor")) {
//            throw new IllegalArgumentException("Not time divisor found!");
//        }
//        return String.valueOf(conf.get("time.divisor"));
//    }

    private static String getZookeeperServers(Map conf) {
        if(!conf.containsKey("zookeeper.servers")) {
            throw new IllegalArgumentException("Not zookeeper servers found!");
        }
        return listOfStringToString((List<String>) conf.get("zookeeper.servers"), String.valueOf(conf.get("zookeeper.port")));
    }

    private static String getKafkaBrokers(Map conf) {
        if(!conf.containsKey("kafka.brokers")) {
            throw new IllegalArgumentException("No kafka brokers found!");
        }
        if(!conf.containsKey("kafka.port")) {
            throw new IllegalArgumentException("No kafka port found!");
        }
        return listOfStringToString((List<String>) conf.get("kafka.brokers"), String.valueOf(conf.get("kafka.port")));
    }

    private static String getKafkaTopic(Map conf) {
        if(!conf.containsKey("kafka.topic")) {
            throw new IllegalArgumentException("No kafka topic found!");
        }
        return (String)conf.get("kafka.topic");
    }

    private static String getRedisHost(Map conf) {
        if(!conf.containsKey("redis.host")) {
            throw new IllegalArgumentException("No redis host found!");
        }
        return (String)conf.get("redis.host");
    }

    public static String listOfStringToString(List<String> list, String port) {
        String val = "";
        for(int i=0; i<list.size(); i++) {
            val += list.get(i) + ":" + port;
            if(i < list.size()-1) {
                val += ",";
            }
        }
        return val;
    }


    /**
     * Choose source - either Kafka or data generator
     */

//    private static Map<String, String> getFlinkConfs(Map conf) {
//        String kafkaBrokers = getKafkaBrokers(conf);
//        String zookeeperServers = getZookeeperServers(conf);
//
//        Map<String, String> flinkConfs = new HashMap<String, String>();
//        flinkConfs.put("topic", getKafkaTopic(conf));
//        flinkConfs.put("bootstrap.servers", kafkaBrokers);
//        flinkConfs.put("zookeeper.connect", zookeeperServers);
//        flinkConfs.put("jedis_server", getRedisHost(conf));
//        flinkConfs.put("time.divisor", getTimeDivisor(conf));
//        flinkConfs.put("group.id", "myGroup");
//
//        return flinkConfs;
//    }

    private static String getTimeDivisor(Map conf) {
        if(!conf.containsKey("time.divisor")) {
            throw new IllegalArgumentException("Not time divisor found!");
        }
        return String.valueOf(conf.get("time.divisor"));
    }

//    private static String getZookeeperServers(Map conf) {
//        if(!conf.containsKey("zookeeper.servers")) {
//            throw new IllegalArgumentException("Not zookeeper servers found!");
//        }
//        return listOfStringToString((List<String>) conf.get("zookeeper.servers"), String.valueOf(conf.get("zookeeper.port")));
//    }

//    private static String getKafkaBrokers(Map conf) {
//        if(!conf.containsKey("kafka.brokers")) {
//            throw new IllegalArgumentException("No kafka brokers found!");
//        }
//        if(!conf.containsKey("kafka.port")) {
//            throw new IllegalArgumentException("No kafka port found!");
//        }
//        return listOfStringToString((List<String>) conf.get("kafka.brokers"), String.valueOf(conf.get("kafka.port")));
//    }

//    private static String getKafkaTopic(Map conf) {
//        if(!conf.containsKey("kafka.topic")) {
//            throw new IllegalArgumentException("No kafka topic found!");
//        }
//        return (String)conf.get("kafka.topic");
//    }
//
//    private static String getRedisHost(Map conf) {
//        if(!conf.containsKey("redis.host")) {
//            throw new IllegalArgumentException("No redis host found!");
//        }
//        return (String)conf.get("redis.host");
//    }
//
//    public static String listOfStringToString(List<String> list, String port) {
//        String val = "";
//        for(int i=0; i<list.size(); i++) {
//            val += list.get(i) + ":" + port;
//            if(i < list.size()-1) {
//                val += ",";
//            }
//        }
//        return val;
//    }


    private static DataStream<String> streamSource(BenchmarkConfig config, StreamExecutionEnvironment env) {
        // Choose a source -- Either local generator or Kafka
        RichParallelSourceFunction<String> source;
        String sourceName;
        if (config.useLocalEventGenerator) {
            EventGeneratorSource eventGenerator = new EventGeneratorSource(config);
            source = eventGenerator;
            sourceName = "EventGenerator";

            Map<String, List<String>> campaigns = eventGenerator.getCampaigns();
            RedisHelper redisHelper = new RedisHelper(config);
            redisHelper.prepareRedis(campaigns);
            redisHelper.writeCampaignFile(campaigns);
        } else {
            source = kafkaSource(config);
            sourceName = "Kafka";
        }

        return env.addSource(source, sourceName);
    }

    /**
     * Setup Flink environment
     */
    private static StreamExecutionEnvironment setupEnvironment(BenchmarkConfig config) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(config.getParameters());

        if (config.checkpointsEnabled) {
            env.enableCheckpointing(config.checkpointInterval);
        }

        // use event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return env;
    }

    /**
     * Sum - window reduce function
     */
    private static ReduceFunction<Tuple3<String, String, Long>> sumReduceFunction() {
        return new ReduceFunction<Tuple3<String, String, Long>>() {
            //            @Override
            public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> t0, Tuple3<String, String, Long> t1) throws Exception {
                t0.f2 += t1.f2;
                return t0;
            }
        };
    }

    /**
     * Sum - Window function, summing already happened in reduce function
     */
    private static WindowFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple, TimeWindow> sumWindowFunction() {
        return new WindowFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple, TimeWindow>() {
            //            @Override
            public void apply(Tuple keyTuple, TimeWindow window, Iterable<Tuple3<String, String, Long>> values, Collector<Tuple3<String, String, Long>> out) throws Exception {
                Iterator<Tuple3<String, String, Long>> valIter = values.iterator();
                Tuple3<String, String, Long> tuple = valIter.next();
                if (valIter.hasNext()) {
                    throw new IllegalStateException("Unexpected");
                }
                tuple.f1 = Long.toString(window.getEnd());
                out.collect(tuple); // collect end time here
            }
        };
    }

    /**
     * Configure Kafka source
     */
//    private static FlinkKafkaConsumer082<String> kafkaSource(BenchmarkConfig config) {
//        return new FlinkKafkaConsumer082(
//                config.kafkaTopic,
//                new SimpleStringSchema(),
//                config.getParameters().getProperties());
//    }

    private static FlinkKafkaConsumer011<String> kafkaSource(BenchmarkConfig config) {
        return new FlinkKafkaConsumer011(
                config.kafkaTopic,
                new SimpleStringSchema(),
                config.getParameters().getProperties());
    }

    /**
     * Custom trigger - Fire and purge window when window closes, also fire every 1000 ms.
     */
    private static class EventAndProcessingTimeTrigger extends Trigger<Object, TimeWindow> {

        @Override
        public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            // register system timer only for the first time
            ValueState<Boolean> firstTimerSet = ctx.getKeyValueState("firstTimerSet", Boolean.class, false);
//            firstTimerSet= ctx.
            if (!firstTimerSet.value()) {
                ctx.registerProcessingTimeTimer(System.currentTimeMillis() + 1000L);
                firstTimerSet.update(true);
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
            return TriggerResult.FIRE_AND_PURGE;
        }

        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            // schedule next timer
            ctx.registerProcessingTimeTimer(System.currentTimeMillis() + 1000L);
            return TriggerResult.FIRE;
        }
    }

    /**
     * Parse JSON
     */
//    private static class DeserializeBolt implements
//            FlatMapFunction<String, Tuple7<String, String, String, String, String, String, String>> {
//
//        transient JSONParser parser = null;
//
//        @Override
//        public void flatMap(String input, Collector<Tuple7<String, String, String, String, String, String, String>> out)
//                throws Exception {
//            if (parser == null) {
//                parser = new JSONParser();
//            }
//            JSONObject obj = (JSONObject) parser.parse(input);
//
//            Tuple7<String, String, String, String, String, String, String> tuple =
//                    new Tuple7<>(
//                            obj.getAsString("user_id"),
//                            obj.getAsString("page_id"),
//                            obj.getAsString("ad_id"),
//                            obj.getAsString("ad_type"),
//                            obj.getAsString("event_type"),
//                            obj.getAsString("event_time"),
//                            obj.getAsString("ip_address"));
//            out.collect(tuple);
//        }
//    }

    /**
     * Filter out all but "view" events
     */
//    public static class EventFilterBolt implements
//            FilterFunction<Tuple7<String, String, String, String, String, String, String>> {
//        @Override
//        public boolean filter(Tuple7<String, String, String, String, String, String, String> tuple) throws Exception {
//            return tuple.getField(4).equals("view");
//        }
//    }

    /**
     * Map ad ids to campaigns using cached data from Redis
     */
//    private static final class RedisJoinBolt extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, String>> {
//
//        private RedisAdCampaignCache redisAdCampaignCache;
//        private BenchmarkConfig config;
//
//        public RedisJoinBolt(BenchmarkConfig config) {
//            this.config = config;
//        }
//
//        @Override
//        public void open(Configuration parameters) {
//            //initialize jedis
//            String redis_host = config.redisHost;
//            LOG.info("Opening connection with Jedis to {}", redis_host);
//            this.redisAdCampaignCache = new RedisAdCampaignCache(redis_host);
//            this.redisAdCampaignCache.prepare();
//        }
//
//        @Override
//        public void flatMap(Tuple2<String, String> input, Collector<Tuple2<String, String>> out) throws Exception {
//            String ad_id = input.getField(0);
//            String campaign_id = this.redisAdCampaignCache.execute(ad_id);
//            if (campaign_id == null) {
//                return;
//            }
//
//            Tuple2<String, String> tuple = new Tuple2<>(campaign_id, (String) input.getField(1)); // event_time
//            out.collect(tuple);
//        }
//    }


    /**
     * Generate timestamp and watermarks for data stream
     */
    private static class AdTimestampExtractor implements TimestampExtractor<Tuple2<String, String>> {

        long maxTimestampSeen = 0;

        //        @Override
        public long extractTimestamp(Tuple2<String, String> element, long currentTimestamp) {
            long timestamp = Long.parseLong(element.f1);
            maxTimestampSeen = Math.max(timestamp, maxTimestampSeen);
            return timestamp;
        }

        //        @Override
        public long extractWatermark(Tuple2<String, String> element, long currentTimestamp) {
            return Long.MIN_VALUE;
        }

        //        @Override
        public long getCurrentWatermark() {
            long watermark = maxTimestampSeen - 1L;
            return watermark;
        }
    }

    /**
     *
     */


    /**
     * Sink computed windows to Redis
     */
    private static class RedisResultSink extends RichSinkFunction<Tuple3<String, String, Long>> {
        private Jedis flushJedis;

        private BenchmarkConfig config;

        public RedisResultSink(BenchmarkConfig config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            flushJedis = new Jedis(config.redisHost);
        }

        //        @Override
        public void invoke(Tuple3<String, String, Long> result) throws Exception {
            // set (campaign, count)
            //    flushJedis.hset("campaign-counts", result.f0, Long.toString(result.f2));

            String campaign = result.f0;
            String timestamp = result.f1;
            String windowUUID = getOrCreateWindow(campaign, timestamp);

            flushJedis.hset(windowUUID, "seen_count", Long.toString(result.f2));
            flushJedis.hset(windowUUID, "time_updated", Long.toString(System.currentTimeMillis()));
            flushJedis.lpush("time_updated", Long.toString(System.currentTimeMillis()));
        }

        private String getOrCreateWindow(String campaign, String timestamp) {
            String windowUUID = flushJedis.hmget(campaign, timestamp).get(0);
            if (windowUUID == null) {
                windowUUID = UUID.randomUUID().toString();
                flushJedis.hset(campaign, timestamp, windowUUID);
                getOrCreateWindowList(campaign, timestamp);
            }
            return windowUUID;
        }

        private void getOrCreateWindowList(String campaign, String timestamp) {
            String windowListUUID = flushJedis.hmget(campaign, "windows").get(0);
            if (windowListUUID == null) {
                windowListUUID = UUID.randomUUID().toString();
                flushJedis.hset(campaign, "windows", windowListUUID);
            }
            flushJedis.lpush(windowListUUID, timestamp);
        }

        @Override
        public void close() throws Exception {
            super.close();
            flushJedis.close();
        }
    }

    /**
     * Simplified version of Redis data structure
     */
    private static class RedisResultSinkOptimized extends RichSinkFunction<Tuple3<String, String, Long>> {
        private final BenchmarkConfig config;
        private Jedis flushJedis;

        public RedisResultSinkOptimized(BenchmarkConfig config){
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            flushJedis = new Jedis(config.redisHost);
            flushJedis.select(1); // select db 1
        }

        //        @Override
        public void invoke(Tuple3<String, String, Long> result) throws Exception {
            // set campaign id -> (window-timestamp, count)
            flushJedis.hset(result.f0, result.f1, Long.toString(result.f2));
        }

        @Override
        public void close() throws Exception {
            super.close();
            flushJedis.close();
        }
    }



    public static class EventGeneratorSource extends LoadGeneratorSource<String> {

        private int adsIdx = 0;
        private int eventsIdx = 0;
        private StringBuilder sb = new StringBuilder();
        private String pageID = UUID.randomUUID().toString();
        private String userID = UUID.randomUUID().toString();
        private final String[] eventTypes = new String[]{"view", "click", "purchase"};

        private List<String> ads;
        private final Map<String, List<String>> campaigns;

        public EventGeneratorSource(BenchmarkConfig config) {
            super(config.loadTargetHz, config.timeSliceLengthMs);
            this.campaigns = generateCampaigns();
            this.ads = flattenCampaigns();
        }

        public Map<String, List<String>> getCampaigns() {
            return campaigns;
        }

        /**
         * Generate a single element
         */
        @Override
        public String generateElement() {
            if (adsIdx == ads.size()) {
                adsIdx = 0;
            }
            if (eventsIdx == eventTypes.length) {
                eventsIdx = 0;
            }
            sb.setLength(0);
            sb.append("{\"user_id\":\"");
            sb.append(pageID);
            sb.append("\",\"page_id\":\"");
            sb.append(userID);
            sb.append("\",\"ad_id\":\"");
            sb.append(ads.get(adsIdx++));
            sb.append("\",\"ad_type\":\"");
            sb.append("banner78"); // value is immediately discarded. The original generator would put a string with 38/5 = 7.6 chars. We put 8.
            sb.append("\",\"event_type\":\"");
            sb.append(eventTypes[eventsIdx++]);
            sb.append("\",\"event_time\":\"");
            sb.append(System.currentTimeMillis());
            sb.append("\",\"ip_address\":\"1.2.3.4\"}");

            return sb.toString();
        }

        /**
         * Generate a random list of ads and campaigns
         */
        private Map<String, List<String>> generateCampaigns() {
            int numCampaigns = 100;
            int numAdsPerCampaign = 10;
            Map<String, List<String>> adsByCampaign = new LinkedHashMap();
            for (int i = 0; i < numCampaigns; i++) {
                String campaign = UUID.randomUUID().toString();
                ArrayList<String> ads = new ArrayList();
                adsByCampaign.put(campaign, ads);
                for (int j = 0; j < numAdsPerCampaign; j++) {
                    ads.add(UUID.randomUUID().toString());
                }
            }
            return adsByCampaign;
        }

        /**
         * Flatten into just ads
         */
        private List<String> flattenCampaigns() {
            // Flatten campaigns into simple list of ads
            List<String> ads = new ArrayList();
            for (Map.Entry<String, List<String>> entry : campaigns.entrySet()) {
                for (String ad : entry.getValue()) {
                    ads.add(ad);
                }
            }
            return ads;
        }
    }



    public static class RedisHelper {
        private static final Logger LOG = LoggerFactory.getLogger(RedisHelper.class);

        private final BenchmarkConfig config;

        public RedisHelper(BenchmarkConfig config){
            this.config = config;
        }

        public void prepareRedis(Map<String, List<String>> campaigns) {
            Jedis redis = new Jedis(config.redisHost);
            redis.select(config.redisDb);
            if (config.redisFlush) {
                LOG.info("Flushing Redis DB.");
                redis.flushDB();
            }

            LOG.info("Preparing Redis with campaign data.");
            for (Map.Entry<String, List<String>> entry : campaigns.entrySet()) {
                String campaign = entry.getKey();
                redis.sadd("campaigns", campaign);
                for (String ad : entry.getValue()) {
                    redis.set(ad, campaign);
                }
            }
            redis.close();
        }

        public void writeCampaignFile(Map<String, List<String>> campaigns) {
            try {
                PrintWriter adToCampaignFile = new PrintWriter("ad-to-campaign-ids.txt");
                for (Map.Entry<String, List<String>> entry : campaigns.entrySet()) {
                    String campaign = entry.getKey();
                    for (String ad : entry.getValue()) {
                        adToCampaignFile.println("{\"" + ad + "\":\"" + campaign + "\"}");
                    }
                }
                adToCampaignFile.close();
            } catch (Throwable t) {
                throw new RuntimeException("Error opening ads file", t);
            }
        }
    }




    public static class ThroughputLogger<T> implements FlatMapFunction<T, Integer> {

        private static final Logger LOG = LoggerFactory.getLogger(ThroughputLogger.class);

        private long totalReceived = 0;
        private long lastTotalReceived = 0;
        private long lastLogTimeMs = -1;
        private int elementSize;
        private long logfreq;

        public ThroughputLogger(int elementSize, long logfreq) {
            this.elementSize = elementSize;
            this.logfreq = logfreq;
        }

        //        @Override
        public void flatMap(T element, Collector<Integer> collector) throws Exception {
            totalReceived++;
            if (totalReceived % logfreq == 0) {
                // throughput over entire time
                long now = System.currentTimeMillis();

                // throughput for the last "logfreq" elements
                if(lastLogTimeMs == -1) {
                    // init (the first)
                    lastLogTimeMs = now;
                    lastTotalReceived = totalReceived;
                } else {
                    long timeDiff = now - lastLogTimeMs;
                    long elementDiff = totalReceived - lastTotalReceived;
                    double ex = (1000/(double)timeDiff);
                    LOG.info("During the last {} ms, we received {} elements. That's {} elements/second/core. {} MB/sec/core. GB received {}",
                            timeDiff, elementDiff, elementDiff*ex, elementDiff*ex*elementSize / 1024 / 1024, (totalReceived * elementSize) / 1024 / 1024 / 1024);
                    // reinit
                    lastLogTimeMs = now;
                    lastTotalReceived = totalReceived;
                }
            }
        }
    }

    public static class AdTimestampExtractorNewer implements AssignerWithPeriodicWatermarks<Tuple3<String, String, String>> {
        @Nullable
        public Watermark getCurrentWatermark() {
            return null;
        }

        @Nullable
        public Watermark checkAndGetNextWatermark(Tuple3<String, String, String> stringStringStringTuple3, long l) {
            return null;
        }

        public long extractTimestamp(Tuple3<String, String, String> element, long l) {
            return Long.parseLong( element.f2);
        }
    }


    private static class MapToImpressionCount implements MapFunction<Tuple3<String, String, String>, Tuple3<String, String, Long> > {
        public Tuple3<String, String, Long> map(Tuple3<String, String, String> t3) throws Exception {
            return new Tuple3(t3.f0, t3.f2, 1L);
        }
    }
}


abstract class LoadGeneratorSource<T> extends RichParallelSourceFunction<T> {

    private boolean running = true;

    private final int loadTargetHz;
    private final int timeSliceLengthMs;

    public LoadGeneratorSource(int loadTargetHz, int timeSliceLengthMs) {
        this.loadTargetHz = loadTargetHz;
        this.timeSliceLengthMs = timeSliceLengthMs;
    }

    /**
     * Subclasses must override this to generate a data element
     */
    public abstract T generateElement();


    /**
     * The main loop
     */
//    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        int elements = loadPerTimeslice();

        while (running) {
            long emitStartTime = System.currentTimeMillis();
            for (int i = 0; i < elements; i++) {
                sourceContext.collect(generateElement());
            }
            // Sleep for the rest of timeslice if needed
            long emitTime = System.currentTimeMillis() - emitStartTime;
            if (emitTime < timeSliceLengthMs) {
                Thread.sleep(timeSliceLengthMs - emitTime);
            }
        }
        sourceContext.close();
    }

    //    @Override
    public void cancel() {
        running = false;
    }


    /**
     * Given a desired load figure out how many elements to generate in each timeslice
     * before yielding for the rest of that timeslice
     */
    private int loadPerTimeslice() {
        int messagesPerOperator = loadTargetHz / getRuntimeContext().getNumberOfParallelSubtasks();
        return messagesPerOperator / (1000 / timeSliceLengthMs);
    }





}




/**
 * To Run:  flink run target/flink-benchmarks-0.1.0-AdvertisingTopologyNative.jar  --confPath "../conf/benchmarkConf.yaml"
 */


