/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark;



import ee.ut.cs.dsg.efficientSWAG.Enumerators;
import benchmark.common.Utils;
import benchmark.common.advertising.CampaignProcessorCommon;
import benchmark.common.advertising.RedisAdCampaignCache;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

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

        messageStream= messageStream.map(new MyMapper());
        messageStream= messageStream.map(new ThroughputRecorder());

//        messageStream
//                .rebalance()
//                // Parse the String as JSON
//                .flatMap(new DeserializeBoltGamal())
//                //Filter the records if event type is "view"
////                .filter(new EventFilterBoltGamal())
//                // project the event
//                .<Tuple2<String, String>>project(2, 5)
//                // perform join with redis data
////                .flatMap(new RedisJoinBolt())
//                // process campaign
//                .flatMap(new MyFlatMap())
//                .keyBy(0)
//                .flatMap(new CampaignProcessor());

        DataStream result= messageStream
                .rebalance()
                // Parse the String as JSON
                .flatMap(new DeserializeBoltGamal())
                // perform join with redis data
                .flatMap(new RedisJoinBoltGamal())
                .keyBy(1)
                .timeWindow(Time.of(1, SECONDS), Time.of(1, SECONDS),1, Enumerators.Operator.MEDIAN_SKIP_LIST)
                .sum(2)
                .flatMap(new CampaignProcessorGamal())
                ;
        ;

        result.print();

        env.execute();
    }


    public static class DeserializeBolt implements
            FlatMapFunction<String, Tuple7<String, String, String, String, String, String, String>> {

        @Override
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
        @Override
        public boolean filter(Tuple7<String, String, String, String, String, String, String> tuple) throws Exception {
            return tuple.getField(4).equals("view");
        }
    }

    public static class EventFilterBoltGamal implements
            FilterFunction<Tuple3<Long, String, Double>> {
        @Override
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

                     input.getField(0),
                    campaign_id,
                     input.getField(2));
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

        @Override
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

    private static String getTimeDivisor(Map conf) {
        if(!conf.containsKey("time.divisor")) {
            throw new IllegalArgumentException("Not time divisor found!");
        }
        return String.valueOf(conf.get("time.divisor"));
    }

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
}

/**
 * To Run:  flink run target/flink-benchmarks-0.1.0-AdvertisingTopologyNative.jar  --confPath "../conf/benchmarkConf.yaml"
 */


