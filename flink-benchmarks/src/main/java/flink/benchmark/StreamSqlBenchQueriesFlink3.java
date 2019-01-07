package flink.benchmark;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;
import org.json.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.state.CheckpointStreamWithResultProvider.LOG;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class StreamSqlBenchQueriesFlink3 {
    public static Long throughputCounterBefore=new Long("0");
    public static Long throughputCounterAfter=new Long("0");
    public static Long throughputAdsCounterAfter=new Long("0");
    public static Long throughputAccomulationcount=new Long("0");
    public static Long initialTime=System.currentTimeMillis();
    public static Jedis flush_jedis;
    public static Pipeline p;
    public static HashMap<String, String> elementsBatchBefore=new HashMap<>();
    public static HashMap<String, String> elementsBatch=new HashMap<>();
    public static void main(String[] args) {
        //ParameterTool params = ParameterTool.fromArgs(args);
        //String ip = params.getRequired("ip");
        int k_partitions = 1;
        //int port=Integer.parseInt(params.getRequired("port"));
        //String ip="localhost";
        // port=6666;
        //////
 /*       ParameterTool parameterTool = ParameterTool.fromArgs(args);

        Map conf = Utils.findAndReadConfigFile(parameterTool.getRequired("confPath"), true);
        int kafkaPartitions = ((Number)conf.get("kafka.partitions")).intValue();
        int hosts = ((Number)conf.get("process.hosts")).intValue();
        int cores = ((Number)conf.get("process.cores")).intValue();

        ParameterTool flinkBenchmarkParams = ParameterTool.fromMap(getFlinkConfs(conf));

        LOG.info("conf: {}", conf);
        LOG.info("Parameters used: {}", flinkBenchmarkParams.toMap());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(flinkBenchmarkParams);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // Set the buffer timeout (default 100)
        // Lowering the timeout will lead to lower latencies, but will eventually reduce throughput.
        env.setBufferTimeout(flinkBenchmarkParams.getLong("flink.buffer-timeout", 100));
        if(flinkBenchmarkParams.has("flink.checkpoint-interval")) {
            // enable checkpointing for fault tolerance
            env.enableCheckpointing(flinkBenchmarkParams.getLong("flink.checkpoint-interval", 1000));
        }*/
        // set default parallelism for all operators (recommended value: number of available worker CPU cores in the cluster (hosts * cores))
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(5 );


        /////

        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(160);
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);


       /* DataStreamSource<Tuple4<Integer, Integer, Integer, Long>> purchaseInputTuple=env.addSource(new YetAnotherSourceSocketPurchase(ip,6666));
        DataStreamSource<Tuple3<Integer, Integer, Long>> adsInputTuple=env.addSource(new YetAnotherSourceSocketAd(ip,7777));
*/
        Properties props = new Properties();
        props.setProperty("zookeeper.connect", "zookeeper-node-01:2181");
        props.setProperty("bootstrap.servers", "kafka-node-01:9092,kafka-node-02:9092,kafka-node-03:9092");
        // not to be shared with another job consuming the same topic
        props.setProperty("group.id", "flink-group");
        props.setProperty("enable.auto.commit","false");
        FlinkKafkaConsumer011<String> purchasesConsumer=new FlinkKafkaConsumer011<String>("purchases",
                new SimpleStringSchema(),
                props);
        purchasesConsumer.setStartFromEarliest();

        FlinkKafkaConsumer011<String> adsConsumer=new FlinkKafkaConsumer011<String>("ads",
                new SimpleStringSchema(),
                props);
        //adsConsumer.setStartFromEarliest();

        DataStream<String> purchasesStream = env
                .addSource(purchasesConsumer)
                .setParallelism(5);


        DataStream<String> adsStream = env
                .addSource(adsConsumer)
                .setParallelism(5);

        /*****************************
         *  adding metrics for the log (I need to know what are these actually)
         *****************************/

/*        purchasesStream= purchasesStream.map(new MyMapper());
        purchasesStream= purchasesStream.map(new ThroughputRecorder());
        adsStream= adsStream.map(new MyMapper());
        adsStream= adsStream.map(new ThroughputRecorder());*/
        /************************************************************/

        DataStream<Tuple5<Integer, Integer, Integer, Long,String>> purchaseWithTimestampsAndWatermarks =
                purchasesStream
                        .map(new PurchasesParser())
                        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple4<Integer, Integer, Integer, Long>>(Time.seconds(10)) {

                            @Override
                            public long extractTimestamp(Tuple4<Integer, Integer, Integer, Long> element) {
                                return element.getField(3);
                            }
                        }).map(new AddPurchaseLatencyId());

        DataStream<Tuple4<Integer, Integer, Long,String>> adsWithTimestampsAndWatermarks =
                adsStream
                        .map( new AdsParser())
                        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Integer, Integer, Long>>(Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(Tuple3<Integer, Integer, Long> element) {
                                return element.getField(2);
                            }
                        }).map(new AddAdLatencyId());




        //mapper to write key and value of each element ot redis
        // purchaseWithTimestampsAndWatermarks.flatMap(new WriteToRedis());
        purchaseWithTimestampsAndWatermarks=  purchaseWithTimestampsAndWatermarks.map(new WriteToRedisBeforeQuery()).name("Write to Redis");
        purchaseWithTimestampsAndWatermarks.map(new MapFunction<Tuple5<Integer, Integer, Integer, Long, String>, Tuple4<Integer, Integer, Integer, String>>() {

            @Override
            public Tuple4<Integer, Integer,Integer, String> map(Tuple5<Integer, Integer, Integer, Long, String> input) throws Exception {
                return new Tuple4<>(input.f0,input.f1, input.f2, input.f4);
            }
        }).map(new WriteToRedisAfterQueryForMapper());
/*
        Table purchasesTable = tEnv.fromDataStream(purchaseWithTimestampsAndWatermarks, "userID, gemPackID,price, rowtime.rowtime, ltcID");
        Table adsTable = tEnv.fromDataStream(adsWithTimestampsAndWatermarks, "userID, gemPackID, rowtime.rowtime,ltcID");
        tEnv.registerTable("purchasesTable", purchasesTable);
        tEnv.registerTable("adsTable", adsTable);
*/




        //Workloads
        //================================General======================

        /**************************************************************
         * 1- Projection//Get all purchased gem pack
         * TODO> return value of writeToRedisAfter is not correct
         * ************************************************************/

      /*  Table result = tEnv.sqlQuery("SELECT  userID, gemPackID, rowtime,ltcID from purchasesTable");
        DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);
        queryResultAsDataStream.map(new WriteToRedisAfterQuery());*/
        //queryResultAsDataStream.print();
//        queryResultAsDataStream.writeAsCsv("/root/stream-benchmarking/data/testSink").setParallelism(1);


        //queryResultAsDataStream.process(new WriteToRedisAfterQueryProcessFn());
//        queryResultAsDataStream.keyBy().timeWindow(1).aggregate().flatMap(new wrtetoredis)
        //queryResultAsDataStream.timeWindowAll(TimeUnit.SECONDS(1)).aggregate()

        /**************************************************************
         * 2- Filtering// Get the purchases of specific user//
         * TODO> I think in this kind of queries we should not calculate throughput. because we will not be able to count the filtered out tuples
         * ************************************************************/
/*        purchaseWithTimestampsAndWatermarks.flatMap(new WriteToRedisBeforeQuery());
        Table result = tEnv.sqlQuery("SELECT  userID, gemPackID, rowtime from purchasesTable WHERE price>20");
        DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);
        queryResultAsDataStream.flatMap(new WriteToRedisAfterQuery());*/

        /**************************************************************
         * 3- Group by // Getting revenue from gempack when it exceeds specified amount
         * TODO> I think in this kind of queries we should not calculate throughput. because we will not be able to count the filtered out tuples
         * ************************************************************/

        // register function
/*        purchaseWithTimestampsAndWatermarks.flatMap(new WriteToRedisBeforeQuery());
        tEnv.registerFunction("getKeyAndValue", new KeyValueGetter());
        Table result = tEnv.sqlQuery("SELECT  gemPackID,sum(price)as revenue,getKeyAndValue(userID, rowtime),count(*)   from purchasesTable GROUP BY TUMBLE(rowtime, INTERVAL '2' SECOND),gemPackID");
        //for the metrics calculation after
        DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);
        queryResultAsDataStream.flatMap(new WriteToRedisAfterQuery());*/
/*        purchaseWithTimestampsAndWatermarks.flatMap(new WriteToRedisBeforeQuery());
        tEnv.registerFunction("getKeyAndValue", new KeyValueGetter());
        Table result = tEnv.sqlQuery("SELECT  sum(price)as revenue,getKeyAndValue(userID, rowtime),count(*)   from purchasesTable GROUP BY TUMBLE(rowtime, INTERVAL '2' SECOND)");
        //for the metrics calculation after
        DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);
        queryResultAsDataStream.flatMap(new WriteToRedisAfterQuery());*/

        /**************************************************************
         * 3- Group by and having // Getting revenue from gempack when it exceeds specified amount
         * TODO> I think in this kind of queries we should not calculate throughput. because we will not be able to count the filtered out tuples
         * ************************************************************/

        // register function
/*        purchaseWithTimestampsAndWatermarks.flatMap(new WriteToRedisBeforeQuery());
        tEnv.registerFunction("getKeyAndValue", new KeyValueGetter());
        Table result = tEnv.sqlQuery("SELECT  gemPackID,sum(price)as revenue,getKeyAndValue(userID, rowtime),count(*)   from purchasesTable GROUP BY TUMBLE(rowtime, INTERVAL '2' SECOND),gemPackID HAVING sum(price)>20 ");
        //for the metrics calculation after
        DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);
        queryResultAsDataStream.flatMap(new WriteToRedisAfterQuery());*/

        //================================WINDOWING======================
        /**************************************************************
         * 4- Tumbling Window// Getting revenue obtained  from each gem pack over fixed period of time
         * ************************************************************/
/*        purchaseWithTimestampsAndWatermarks.flatMap(new WriteToRedisBeforeQuery());
        // register function
        tEnv.registerFunction("getKeyAndValue", new KeyValueGetter());
        Table result = tEnv.sqlQuery("SELECT  gemPackID,sum(price)as revenue,getKeyAndValue(userID, rowtime),count(*)   from purchasesTable GROUP BY TUMBLE(rowtime, INTERVAL '2' SECOND),gemPackID");
        //for the metrics calculation after
        DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);
        queryResultAsDataStream.flatMap(new WriteToRedisAfterQuery());*/

        /**************************************************************
         * 5- Sliding Window //Getting revenue obtained from each gem pack over fixed overlapped period of time
         * ************************************************************/
/*        purchaseWithTimestampsAndWatermarks.flatMap(new WriteToRedisBeforeQuery());
        // register function
        tEnv.registerFunction("getKeyAndValue", new KeyValueGetter());
        Table result = tEnv.sqlQuery("SELECT  gemPackID,sum(price)as revenue,getKeyAndValue(userID, rowtime),count(*)   from purchasesTable GROUP BY HOP(rowtime, INTERVAL '1' SECOND, INTERVAL '2' SECOND),gemPackID");
        //for the metrics calculation after
        DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);
        queryResultAsDataStream.flatMap(new WriteToRedisAfterQuery());*/

        /**************************************************************
         * 6- Session window //Getting Revenue obtained from each gem pack after each specific period of inactivity
         * ************************************************************/
/*        purchaseWithTimestampsAndWatermarks.flatMap(new WriteToRedisBeforeQuery());
        // register function
        tEnv.registerFunction("getKeyAndValue", new KeyValueGetter());
        Table result = tEnv.sqlQuery("SELECT  gemPackID,sum(price)as revenue,getKeyAndValue(userID, rowtime),count(*)   from purchasesTable GROUP BY SESSION(rowtime, INTERVAL '2' SECOND),gemPackID");
        //for the metrics calculation after
        DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);
        queryResultAsDataStream.flatMap(new WriteToRedisAfterQuery());*/


        //================================JOINS======================
        /**************************************************************
         * 7- Inner join // Getting revenue from each ad (which ad triggered purchase)
         * TODO>Throughput in joins is not representative (look at previous papers amd discuss with the geeks)
         * ************************************************************/

        // register function
/*        purchaseWithTimestampsAndWatermarks.flatMap(new WriteToRedisBeforeQuery());
        tEnv.registerFunction("getKeyAndValue", new KeyValueGetter());

        Table result = tEnv.sqlQuery("SELECT  p.userID,p.gemPackID,p.price, p.rowtime  " +
                "from purchasesTable p inner join adsTable a " +
                "on p.userID = a.userID " +
                "and p.gemPackID = a.gemPackID " +
                "and p.rowtime  BETWEEN a.rowtime - INTERVAL '1' SECOND AND a.rowtime+INTERVAL '9' SECOND");

        //for the metrics calculation after
        DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);
        queryResultAsDataStream.flatMap(new WriteToRedisAfterQuery());*/

        /**************************************************************
         * 8- Full outer // Getting revenue from each ad (which ad triggered purchase)
         * TODO>Throughput in joins is not representative (look at previous papers amd discuss with the geeks)
         * ************************************************************/
/*        DataStream<Tuple2<Boolean, Row>> PurchaseDataStreamTable = tEnv.toRetractStream(purchasesTable, Row.class);
        DataStream<Tuple2<String,String>> writeToRedisBefore = PurchaseDataStreamTable.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("before "+"Key> p"+inputTuple.f1.getField(0)+""+new Instant(inputTuple.f1.getField(3)).getMillis()+" value> "+System.currentTimeMillis());
                System.out.println( throughputCounterBefore++);//for throughput
                return new Tuple2<>(inputTuple.f1.getField(0)+"",System.currentTimeMillis()+"");//for latency

            }
        });

        // register function
        tEnv.registerFunction("getKeyAndValue", new KeyValueGetter());

        Table result = tEnv.sqlQuery("SELECT  p.userID,p.gemPackID,p.price, p.rowtime  " +
                "from purchasesTable p FULL OUTER JOIN adsTable a " +
                "on p.userID = a.userID " +
                "and p.gemPackID = a.gemPackID " +
                "and p.rowtime  BETWEEN a.rowtime - INTERVAL '1' SECOND AND a.rowtime+INTERVAL '1' SECOND");

        //for the metrics calculation after
        DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);

        DataStream<Tuple2<String,String>> writeToRedisAfter = queryResultAsDataStream.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("after "+"Key> p"+inputTuple.f1.getField(0)+""+new Instant(inputTuple.f1.getField(3)).getMillis()+" value> "+System.currentTimeMillis());
                System.out.println( "throughput> " +throughputCounterAfter++); //for throughput
                return new Tuple2<>(inputTuple.f1.getField(0)+"",System.currentTimeMillis()+""); //for latency

            }
        });*/
        /**************************************************************
         * 9- Left outer// Getting revenue from each ad (which ad triggered purchase)
         * TODO>Throughput in joins is not representative (look at previous papers amd discuss with the geeks)
         * ************************************************************/
 /*       DataStream<Tuple2<Boolean, Row>> PurchaseDataStreamTable = tEnv.toRetractStream(purchasesTable, Row.class);
        DataStream<Tuple2<String,String>> writeToRedisBefore = PurchaseDataStreamTable.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("before "+"Key> p"+inputTuple.f1.getField(0)+""+new Instant(inputTuple.f1.getField(3)).getMillis()+" value> "+System.currentTimeMillis());
                System.out.println( throughputCounterBefore++);//for throughput
                return new Tuple2<>(inputTuple.f1.getField(0)+"",System.currentTimeMillis()+"");//for latency

            }
        });

        // register function
        tEnv.registerFunction("getKeyAndValue", new KeyValueGetter());

        Table result = tEnv.sqlQuery("SELECT  p.userID,p.gemPackID,p.price, p.rowtime  " +
                "from purchasesTable p LEFT OUTER JOIN adsTable a " +
                "on p.userID = a.userID " +
                "and p.gemPackID = a.gemPackID " +
                "and p.rowtime  BETWEEN a.rowtime - INTERVAL '1' SECOND AND a.rowtime+INTERVAL '1' SECOND");

        //for the metrics calculation after
        DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);

        DataStream<Tuple2<String,String>> writeToRedisAfter = queryResultAsDataStream.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("after "+"Key> p"+inputTuple.f1.getField(0)+""+new Instant(inputTuple.f1.getField(3)).getMillis()+" value> "+System.currentTimeMillis());
                System.out.println( "throughput> " +throughputCounterAfter++); //for throughput
                return new Tuple2<>(inputTuple.f1.getField(0)+"",System.currentTimeMillis()+""); //for latency

            }
        });*/
        /**************************************************************
         * 10- right outer// Getting revenue from each ad (which ad triggered purchase)
         * TODO>Throughput in joins is not representative (look at previous papers amd discuss with the geeks)
         * ************************************************************/
/*        DataStream<Tuple2<Boolean, Row>> PurchaseDataStreamTable = tEnv.toRetractStream(purchasesTable, Row.class);
        DataStream<Tuple2<String,String>> writeToRedisBefore = PurchaseDataStreamTable.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("before "+"Key> p"+inputTuple.f1.getField(0)+""+new Instant(inputTuple.f1.getField(3)).getMillis()+" value> "+System.currentTimeMillis());
                System.out.println( throughputCounterBefore++);//for throughput
                return new Tuple2<>(inputTuple.f1.getField(0)+"",System.currentTimeMillis()+"");//for latency

            }
        });

        // register function
        tEnv.registerFunction("getKeyAndValue", new KeyValueGetter());

        Table result = tEnv.sqlQuery("SELECT  p.userID,p.gemPackID,p.price, p.rowtime  " +
                "from purchasesTable p RIGHT OUTER JOIN adsTable a " +
                "on p.userID = a.userID " +
                "and p.gemPackID = a.gemPackID " +
                "and p.rowtime  BETWEEN a.rowtime - INTERVAL '1' SECOND AND a.rowtime+INTERVAL '1' SECOND");

        //for the metrics calculation after
        DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);

        DataStream<Tuple2<String,String>> writeToRedisAfter = queryResultAsDataStream.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("after "+"Key> p"+inputTuple.f1.getField(0)+""+new Instant(inputTuple.f1.getField(3)).getMillis()+" value> "+System.currentTimeMillis());
                System.out.println( "throughput> " +throughputCounterAfter++); //for throughput
                return new Tuple2<>(inputTuple.f1.getField(0)+"",System.currentTimeMillis()+""); //for latency

            }
        });*/
        //================================Set operations======================
        /**************************************************************
         * 11- UNION //Get all gem packs either purchased or shown as ad
         * ************************************************************/
 /*       DataStream<Tuple2<Boolean, Row>> PurchaseDataStreamTable = tEnv.toRetractStream(purchasesTable, Row.class);
        DataStream<Tuple2<String,String>> writeToRedisBefore = PurchaseDataStreamTable.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("before "+"Key> p"+inputTuple.f1.getField(0)+""+new Instant(inputTuple.f1.getField(3)).getMillis()+" value> "+System.currentTimeMillis());
                System.out.println( throughputCounterBefore++);//for throughput
                return new Tuple2<>(inputTuple.f1.getField(0)+"",System.currentTimeMillis()+"");//for latency

            }
        });
        DataStream<Tuple2<Boolean, Row>> adDataStreamTable = tEnv.toRetractStream(adsTable, Row.class);
        DataStream<Tuple2<String,String>> writeAdToRedisBefore = adDataStreamTable.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("before "+"Key> a"+inputTuple.f1.getField(0)+""+new Instant(inputTuple.f1.getField(2)).getMillis()+" value> "+System.currentTimeMillis());
                System.out.println( throughputCounterBefore++);//for throughput
                return new Tuple2<>(inputTuple.f1.getField(0)+"",System.currentTimeMillis()+"");//for latency

            }
        });
        tEnv.registerFunction("addPChar", new AddCharToUserID ("p"));
        tEnv.registerFunction("addAChar", new AddCharToUserID ("a"));

        Table result = tEnv.sqlQuery("SELECT  gemPackID,addPChar(userID), rowtime from purchasesTable " +
                "UNION SELECT  gemPackID,addAChar(userID), rowtime from adsTable");

        DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);

        DataStream<Tuple2<String,String>> writeToRedisAfter = queryResultAsDataStream.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("after "+"Key> "+inputTuple.f1.getField(1)+""+new Instant(inputTuple.f1.getField(2)).getMillis()+" value> "+System.currentTimeMillis());
                //System.out.println("check this"+inputTuple.f1.getField(1));
                System.out.println( "throughput> " +throughputCounterAfter++); //for throughput
                return new Tuple2<>(inputTuple.f1.getField(0)+"",System.currentTimeMillis()+""); //for latency

            }
        });*/
        /**************************************************************
         * 12- Intersect // not yet supported in flink
         * ************************************************************/
        //================================Nested Queries======================
        /**************************************************************
         * 13-Nested Queries //Get purchased gem pack with price exceeds average price for the purchased items in some time frame.
         * TODO> still not working.
         * ************************************************************/
/*        DataStream<Tuple2<Boolean, Row>> PurchaseDataStreamTable = tEnv.toRetractStream(purchasesTable, Row.class);
        DataStream<Tuple2<String,String>> writeToRedisBefore = PurchaseDataStreamTable.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("before "+"Key> p"+inputTuple.f1.getField(0)+""+new Instant(inputTuple.f1.getField(3)).getMillis()+" value> "+System.currentTimeMillis());
                System.out.println( throughputCounterBefore++);//for throughput
                return new Tuple2<>(inputTuple.f1.getField(0)+"",System.currentTimeMillis()+"");//for latency

            }
        });

        // register function
        tEnv.registerFunction("getKeyAndValue", new KeyValueGetter());
        tEnv.registerFunction("concatUIDAndTime", new ConcatTowCulomnTo ());

        //Table result = tEnv.sqlQuery("with SubQ As (select  gemPackID,getKeyAndValue(userID, rowtime) as userIDWithTime,count(*) as throughput,avg(price) as avgPrice from purchasesTable GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND), gemPackID)  select userIDWithTime,gemPackID,throughput from SubQ where ");
        //Table result = tEnv.sqlQuery("with SubQ As (select  gemPackID,getKeyAndValue(userID, rowtime) as userIDWithTime,count(*) as throughput,avg(price) as avgPrice from purchasesTable GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND), gemPackID)  select gemPackID, price from purchasesTable where price> select avgPrice from subQ");
        Table result = tEnv.sqlQuery("select concatUIDAndTime(userID,rowtime) price from purchasesTable where price in (select avg(price) from purchasesTable GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND)) ");

        //for the metrics calculation after
       DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);

        DataStream<Tuple2<String,String>> writeToRedisAfter = queryResultAsDataStream.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("after "+"Key> p"+inputTuple.f1.getField(0) +new Instant(inputTuple.f1.getField(2)).getMillis()+" value> "+System.currentTimeMillis()); //for latency
                System.out.println( throughputCounterAfter++);//for throughput
                return new Tuple2<>(inputTuple.f1.getField(0)+"",System.currentTimeMillis()+""); //for latency

            }
        });*/

        //================================User-defined functions======================
        /**************************************************************
         * 13- Scalar UDF //Convert price to some other currency
         * ************************************************************/
 /*       DataStream<Tuple2<Boolean, Row>> PurchaseDataStreamTable = tEnv.toRetractStream(purchasesTable, Row.class);
        DataStream<Tuple2<String,String>> writeToRedisBefore = PurchaseDataStreamTable.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("before "+"Key> p"+inputTuple.f1.getField(0)+""+new Instant(inputTuple.f1.getField(3)).getMillis()+" value> "+System.currentTimeMillis());
                System.out.println( throughputCounterBefore++);//for throughput
                return new Tuple2<>(inputTuple.f1.getField(0)+"",System.currentTimeMillis()+"");//for latency

            }
        });

        // register function
        tEnv.registerFunction("getKeyAndValue", new KeyValueGetter());
        tEnv.registerFunction("convertPriceCurrency", new CurrencyCoverter (0.7));

        Table result = tEnv.sqlQuery("select userID, convertPriceCurrency(price),rowtime from purchasesTable ");

        //for the metrics calculation after
       DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);

        DataStream<Tuple2<String,String>> writeToRedisAfter = queryResultAsDataStream.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("after "+"Key> p"+inputTuple.f1.getField(0) +new Instant(inputTuple.f1.getField(2)).getMillis()+" value> "+System.currentTimeMillis()); //for latency
                System.out.println( throughputCounterAfter++);//for throughput
                return new Tuple2<>(inputTuple.f1.getField(0)+"",System.currentTimeMillis()+""); //for latency

            }
        });*/

        /**************************************************************
         * 14- Aggregate UDF
         * ************************************************************/
  /*      DataStream<Tuple2<Boolean, Row>> PurchaseDataStreamTable = tEnv.toRetractStream(purchasesTable, Row.class);
        DataStream<Tuple2<String,String>> writeToRedisBefore = PurchaseDataStreamTable.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("before "+"Key> p"+inputTuple.f1.getField(0)+""+new Instant(inputTuple.f1.getField(3)).getMillis()+" value> "+System.currentTimeMillis());
                //System.out.println( throughputCounterBefore++);//for throughput
                return new Tuple2<>(inputTuple.f1.getField(0)+"",System.currentTimeMillis()+"");//for latency

            }
        });

        // register function
        tEnv.registerFunction("wAvg", new WeightedAvg());
        tEnv.registerFunction("getKeyAndValue", new KeyValueGetter());

        Table result = tEnv.sqlQuery("select  wAvg(price, price),getKeyAndValue(userID,rowtime),count(*) from purchasesTable GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND)");

        //for the metrics calculation after
        DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);

        DataStream<Tuple2<String,String>> writeToRedisAfter = queryResultAsDataStream.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("after "+"Key> p"+inputTuple.f1.getField(1) +" value> "+System.currentTimeMillis()); //for latency
                throughputAccomulationcount+=Integer.parseInt(inputTuple.f1.getField(2).toString());
                System.out.println("throughput > " + throughputAccomulationcount); //for throughput
                return new Tuple2<>(inputTuple.f1.getField(1)+"",System.currentTimeMillis()+""); //for latency

            }
        });*/





        //tEnv.toRetractStream(result, Row.class).print();

        try {
           JobExecutionResult jobResult = env.execute("flink SQL Streaming Benchmarking");
           Integer throughput = jobResult.getAccumulatorResult("throughput");
            System.out.println(throughput+"=================================================");
           flush_jedis=new Jedis("redis",6379);
            flush_jedis.hset("throughput","throughput",throughput.toString());


        } catch (Exception e) {
            e.printStackTrace();
        }

    }



    // *************************************************************************
    //     User-defined Table function
    // *************************************************************************.
    // The generic type "Tuple2<String, Integer>" determines the schema of the returned table as (String, Integer).
    public static class Split extends TableFunction<Tuple2<String, Integer>> {
        private String separator = " ";

        public Split(String separator) {
            this.separator = separator;
        }

        public void eval(String str) {
            for (String s : str.split(separator)) {
                // use collect(...) to emit a row
                collect(new Tuple2<String, Integer>(s, s.length()));
            }
        }
    }


    // *************************************************************************
    //     User-defined scalar function
    // *************************************************************************.
    public static class AddCharToUserID extends ScalarFunction {
        private String charToAdd = "p";

        public AddCharToUserID(String userID) {
            this.charToAdd = userID;
        }

        public String eval(int stringToModify) {
            return this.charToAdd+""+stringToModify;
        }
    }

    // *************************************************************************
    //     User-defined scalar function
    // *************************************************************************.
    public static class ConcatTowCulomnTo extends ScalarFunction {


        public ConcatTowCulomnTo() {

        }

        public String eval(int Column1ToConcat, Timestamp Column2ToConcat) {
            return Column1ToConcat+""+Column2ToConcat;
        }
    }

    // *************************************************************************
    //     User-defined scalar function //currency converter
    // *************************************************************************.
    public static class CurrencyCoverter extends ScalarFunction {
        private double conversionRate=0;


        public CurrencyCoverter(double conversionRate) {
            this.conversionRate=conversionRate;

        }

        public double eval(int ammountToConvert) {
            return ammountToConvert*conversionRate;
        }
    }
    // *************************************************************************
    //     User-defined Aggregate function
    // *************************************************************************.
    /**
     * Accumulator for WeightedAvg.
     */
    public static class KeyValueContainer {
        public Timestamp timestmp;
        public int userID = 0;
    }

    /**
     *
     */
    public static class KeyValueGetter extends AggregateFunction<String, KeyValueContainer> {

        @Override
        public KeyValueContainer createAccumulator() {
            return new KeyValueContainer();
        }

        @Override
        public String getValue(KeyValueContainer kv) {
            if (kv.userID == 0) {
                return "0000000000000";
            } else {
                //System.out.println("in getValue "+"  "+kv.timestmp+""+kv.userID);
                //System.out.println(new Instant(kv.timestmp).getMillis()+"===============================");
                return kv.userID+":"+new Instant(kv.timestmp).getMillis();
            }
        }
        public void accumulate(KeyValueContainer kv, int iKey, Timestamp iValue) {
            kv.userID=iKey;
            kv.timestmp=iValue;
        }
        public void merge(KeyValueContainer kv, Iterable<KeyValueContainer> it) {
            Iterator<KeyValueContainer> iter = it.iterator();
            while (iter.hasNext()) {
                KeyValueContainer a = iter.next();
                kv.userID= a.userID;
                kv.timestmp = a.timestmp;
            }
        }

    }

    /**
     * Accumulator for WeightedAvg.
     */
    public static class WeightedAvgAccum {
        public long sum = 0;
        public int count = 0;
    }

    /**
     * Weighted Average user-defined aggregate function.
     */
    public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccum> {

        @Override
        public WeightedAvgAccum createAccumulator() {
            return new WeightedAvgAccum();
        }

        @Override
        public Long getValue(WeightedAvgAccum acc) {
            if (acc.count == 0) {
                return null;
            } else {
                return acc.sum / acc.count;
            }
        }

        public void accumulate(WeightedAvgAccum acc, int iValue, int iWeight) {
            acc.sum += iValue * iWeight;
            acc.count += iWeight;
        }

        public void retract(WeightedAvgAccum acc, int iValue, int iWeight) {
            acc.sum -= iValue * iWeight;
            acc.count -= iWeight;
        }

        public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
            Iterator<WeightedAvgAccum> iter = it.iterator();
            while (iter.hasNext()) {
                WeightedAvgAccum a = iter.next();
                acc.count += a.count;
                acc.sum += a.sum;
            }
        }

        public void resetAccumulator(WeightedAvgAccum acc) {
            acc.count = 0;
            acc.sum = 0L;
        }
    }




    // *************************************************************************
    //
    // *************************************************************************.


    private static class PurchasesParser  implements MapFunction<String,Tuple4<Integer, Integer, Integer, Long>> {
        @Override
        public Tuple4<Integer, Integer, Integer, Long> map(String input) throws Exception {
            JSONObject obj = new JSONObject(input);
            Tuple4<Integer,Integer,Integer,Long> tuple =
                    new Tuple4<Integer,Integer,Integer,Long> (
                            obj.getInt("userID"),
                            obj.getInt("gemPackID"),
                            obj.getInt("price"),
                            obj.getLong("timeStamp")
                    );

            return tuple;
        }

    }

    private static class AdsParser implements  MapFunction<String,Tuple3<Integer, Integer,  Long>>{

        @Override
        public Tuple3<Integer, Integer, Long> map(String input) throws Exception {
            JSONObject obj = new JSONObject(input);
            Tuple3<Integer,Integer,Long> tuple =
                    new Tuple3<Integer,Integer,Long> (
                            obj.getInt("userID"),
                            obj.getInt("gemPackID"),
                            obj.getLong("timeStamp")
                    );

            return tuple;
        }
    }

    private static class AddPurchaseLatencyId  implements MapFunction<Tuple4<Integer, Integer, Integer, Long>,Tuple5<Integer, Integer, Integer, Long,String>> {
        @Override
        public Tuple5<Integer, Integer, Integer, Long,String> map(Tuple4<Integer, Integer, Integer, Long> input) throws Exception {
            return  new Tuple5<>(input.f0,input.f1,input.f2,input.f3,input.f0+":"+input.f3);
        }

    }
    private static class AddAdLatencyId  implements MapFunction<Tuple3<Integer, Integer,  Long>,Tuple4<Integer, Integer,  Long,String>> {
        @Override
        public Tuple4<Integer, Integer,  Long,String> map(Tuple3<Integer, Integer,Long> input) throws Exception {
           return new Tuple4<>(input.f0,input.f1,input.f2,input.f0+":"+input.f2);
        }

    }
    /**
     * write to redis before query
     */
    public static class WriteToRedis extends RichFlatMapFunction<Tuple4<Integer, Integer, Integer, Long>, String> {
        RedisReadAndWrite redisReadAndWrite;

        @Override
        public void open(Configuration parameters) {
            //ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            //parameterTool.getRequired("jedis_server");
//            LOG.info("Opening connection with Jedis to {}", parameterTool.getRequired("jedis_server"));
            LOG.info("Opening connection with Jedis to {}", "redis");
            //this.redisReadAndWrite=new RedisReadAndWrite("redis",6379);
            this.redisReadAndWrite = new RedisReadAndWrite("redis",6379);
            //this.redisReadAndWrite.prepare();

        }

        @Override
        public void flatMap(Tuple4<Integer, Integer, Integer, Long> input, Collector<String> out) throws Exception {

            this.redisReadAndWrite.write(input.f0+":"+input.f3+"","time_seen", TimeUnit.NANOSECONDS.toMillis(System.nanoTime())+"");
            this.redisReadAndWrite.write("JnTPBef","Throughput", (throughputCounterBefore++)+"");
            //this.redisReadAndWrite.execute(input.f0+":"+input.f3+"", TimeUnit.NANOSECONDS.toMillis(System.nanoTime())+"");
        }
    }
    /**
     * write to redis after query
     */
    public static class WriteToRedisBeforeQuery extends RichMapFunction<Tuple5<Integer, Integer, Integer, Long,String>, Tuple5<Integer, Integer, Integer, Long,String>> {
        //RedisReadAndWrite redisReadAndWrite;
        RedisReadAndWriteBefore redisReadAndWriteBefore;
        @Override
        public String toString() {
            return "";
        }
        @Override
        public void open(Configuration parameters) {
            // this.redisReadAndWrite=new RedisReadAndWrite("redis",6379);
            this.redisReadAndWriteBefore=new RedisReadAndWriteBefore("redis",6379);
            this.redisReadAndWriteBefore.prepare_before();
        }
       /* @Override
        public void imap(Tuple5<Integer, Integer, Integer, Long,String> input, Collector<Tuple5<Integer, Integer, Integer, Long,String>> out) throws Exception {

            //this.redisReadAndWrite.write(input.f1.getField(0)+":"+new Instant(input.f1.getField(2)).getMillis()+"","time_updated", TimeUnit.NANOSECONDS.toMillis(System.nanoTime())+"");
            //this.redisReadAndWrite.write("JnTPAft","Throughput", (throughputCounterAfter++)+"");
            //this.redisReadAndWriteAfter.execute(input.f1.getField(0)+":"+new Instant(input.f1.getField(2)).getMillis()+"","time_updated:"+TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
*//*            synchronized (elementsBatchBefore){
                elementsBatchBefore.put(input.f0+":"+new Instant(input.f3).getMillis(),"time_seen:"+System.currentTimeMillis());
                if(elementsBatchBefore.size()>500){
                    this.redisReadAndWriteAfter.execute(elementsBatchBefore);
                    elementsBatchBefore.clear();
                }
            }*//*
            //System.out.println("Before   "+input.f4);

            this.redisReadAndWriteAfter.execute_before(input.f4,"time_seen:"+TimeUnit.NANOSECONDS.toMillis(System.nanoTime())+"");
            out.collect(input);


        }*/

        @Override
        public Tuple5<Integer, Integer, Integer, Long, String> map(Tuple5<Integer, Integer, Integer, Long, String> input) throws Exception {
//            this.redisReadAndWriteBefore.execute_before(input.f4,TimeUnit.NANOSECONDS.toMillis(System.nanoTime())+"");
            this.redisReadAndWriteBefore.execute_before(input.f4,TimeUnit.NANOSECONDS.toMillis(System.nanoTime())+"",input.f0+"*");
            return input;
        }
    }
    /**
     * write to redis after query
     */
    public static class WriteToRedisAfterQuery extends RichMapFunction<Tuple2<Boolean, Row>, String> {
        //RedisReadAndWrite redisReadAndWrite;
        RedisReadAndWriteAfter redisReadAndWriteAfter;
        @Override
        public String toString() {
            return "";
        }
        private IntCounter num_elements = new IntCounter();
        private long totElements = 0;
        @Override
        public void open(Configuration parameters) {
            // this.redisReadAndWrite=new RedisReadAndWrite("redis",6379);
            this.redisReadAndWriteAfter=new RedisReadAndWriteAfter("redis",6379);
            this.redisReadAndWriteAfter.prepare();
//            this.redisReadAndWriteAfter.prepare_throuphput();
            getRuntimeContext().addAccumulator("throughput",
                    this.num_elements);
        }

        @Override
        public String map(Tuple2<Boolean, Row> input) throws Exception {

            //this.redisReadAndWrite.write(input.f1.getField(0)+":"+new Instant(input.f1.getField(2)).getMillis()+"","time_updated", TimeUnit.NANOSECONDS.toMillis(System.nanoTime())+"");
            //this.redisReadAndWrite.write("JnTPAft","Throughput", (throughputCounterAfter++)+"");
            //this.redisReadAndWriteAfter.execute(input.f1.getField(0)+":"+new Instant(input.f1.getField(2)).getMillis()+"","time_updated:"+TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));


            //throughputCounterAfter++; // open this line for non aggregate queries
 /*           synchronized (elementsBatch){
                elementsBatch.put(input.f1.getField(0)+":"+new Instant(input.f1.getField(3)).getMillis(),"time_updated:"+System.currentTimeMillis()); // open this line for nin aggregate queries
                elementsBatch.put("tpt:"+System.currentTimeMillis(),"throughput:"+throughputCounterAfter); // open this line for nin aggregate queries

//                elementsBatch.put(input.f1.getField(2)+"","time_updated:"+System.currentTimeMillis()); // open this line for  aggregate queries
//                elementsBatch.put("tpt:"+System.currentTimeMillis(),"throughput:"+input.f1.getField(3)+""); // open this line for aggregate queries

                if(elementsBatch.size()>500){
                    this.redisReadAndWriteAfter.execute(elementsBatch);
                    elementsBatch.clear();
                    throughputCounterAfter=0L;
                }
            }*/
            // System.out.println("after   "+input.f1.getField(3));
            totElements++;
            this.redisReadAndWriteAfter.execute1(input.f1.getField(3).toString(),TimeUnit.NANOSECONDS.toMillis(System.nanoTime())+"",input.f1.getField(0).toString()); //for non aggregate
//            this.redisReadAndWriteAfter.executeForAgregate(input.f1.getField(1)+"","time_updated:"+System.currentTimeMillis(),input.f1.getField(2)+"");
            this.num_elements.add(1);

            return input.f1.toString();
        }
    }
    /**
     * write to redis after query
     */
    public static class WriteToRedisAfterQueryForMapper extends RichMapFunction<Tuple4<Integer, Integer,Integer, String>, String> {
        //RedisReadAndWrite redisReadAndWrite;
        RedisReadAndWriteAfter redisReadAndWriteAfter;
        @Override
        public String toString() {
            return "";
        }
        private IntCounter num_elements = new IntCounter();
        private long totElements = 0;
        @Override
        public void open(Configuration parameters) {
            // this.redisReadAndWrite=new RedisReadAndWrite("redis",6379);
            this.redisReadAndWriteAfter=new RedisReadAndWriteAfter("redis",6379);
            this.redisReadAndWriteAfter.prepare();
//            this.redisReadAndWriteAfter.prepare_throuphput();
            getRuntimeContext().addAccumulator("throughput",
                    this.num_elements);
        }

        @Override
        public String map(Tuple4<Integer, Integer,Integer, String> input) throws Exception {

            //this.redisReadAndWrite.write(input.f1.getField(0)+":"+new Instant(input.f1.getField(2)).getMillis()+"","time_updated", TimeUnit.NANOSECONDS.toMillis(System.nanoTime())+"");
            //this.redisReadAndWrite.write("JnTPAft","Throughput", (throughputCounterAfter++)+"");
            //this.redisReadAndWriteAfter.execute(input.f1.getField(0)+":"+new Instant(input.f1.getField(2)).getMillis()+"","time_updated:"+TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));


            //throughputCounterAfter++; // open this line for non aggregate queries
 /*           synchronized (elementsBatch){
                elementsBatch.put(input.f1.getField(0)+":"+new Instant(input.f1.getField(3)).getMillis(),"time_updated:"+System.currentTimeMillis()); // open this line for nin aggregate queries
                elementsBatch.put("tpt:"+System.currentTimeMillis(),"throughput:"+throughputCounterAfter); // open this line for nin aggregate queries

//                elementsBatch.put(input.f1.getField(2)+"","time_updated:"+System.currentTimeMillis()); // open this line for  aggregate queries
//                elementsBatch.put("tpt:"+System.currentTimeMillis(),"throughput:"+input.f1.getField(3)+""); // open this line for aggregate queries

                if(elementsBatch.size()>500){
                    this.redisReadAndWriteAfter.execute(elementsBatch);
                    elementsBatch.clear();
                    throughputCounterAfter=0L;
                }
            }*/
            // System.out.println("after   "+input.f1.getField(3));
            totElements++;
            this.redisReadAndWriteAfter.execute1(input.f3,TimeUnit.NANOSECONDS.toMillis(System.nanoTime())+"",input.f0.toString()); //for non aggregate
//            this.redisReadAndWriteAfter.executeForAgregate(input.f1.getField(1)+"","time_updated:"+System.currentTimeMillis(),input.f1.getField(2)+"");
            this.num_elements.add(1);

            return input.f1.toString();
        }
    }
    /**
     * write to redis after query using process function
     */
    public static class WriteToRedisAfterQueryProcessFn extends ProcessFunction<Tuple2<Boolean, Row>, String> {
        //RedisReadAndWrite redisReadAndWrite;
        // RedisReadAndWriteAfter redisReadAndWriteAfter;
        Long timetoFlush;

        @Override
        public void open(Configuration parameters) {
            flush_jedis=new Jedis("redis",6379);
            p = flush_jedis.pipelined();
            this.timetoFlush=System.currentTimeMillis()-initialTime;

        }

        @Override
        public void processElement(Tuple2<Boolean, Row> input, Context context, Collector<String> collector) throws Exception {
            //this.redisReadAndWrite.write(input.f1.getField(0)+":"+new Instant(input.f1.getField(2)).getMillis()+
            // "","time_updated", TimeUnit.NANOSECONDS.toMillis(System.nanoTime())+"");
            p.hset(input.f1.getField(0)+":"+new Instant(input.f1.getField(2)).getMillis()+"","time_updated",TimeUnit.NANOSECONDS.toMillis(System.nanoTime())+"");
            if(((this.timetoFlush%30000)>25000)&& ((this.timetoFlush%60000)<=29000)){
                p.sync();
            }



        }
    }
    /**
     * write ads to redis before query
     */
    public static class WriteAdsToRedis extends RichFlatMapFunction<Tuple3<Integer, Integer, Long>, String> {
        RedisReadAndWrite redisReadAndWrite;

        @Override
        public void open(Configuration parameters) {
            //ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            //parameterTool.getRequired("jedis_server");
//            LOG.info("Opening connection with Jedis to {}", parameterTool.getRequired("jedis_server"));
            LOG.info("Opening connection with Jedis to {}", "redis");
            //this.redisReadAndWrite=new RedisReadAndWrite("redis",6379);
            this.redisReadAndWrite = new RedisReadAndWrite("redis",6379);
            //this.redisReadAndWrite.prepare();

        }

        @Override
        public void flatMap(Tuple3<Integer, Integer, Long> input, Collector<String> out) throws Exception {

            this.redisReadAndWrite.write(input.f0+":"+input.f2+"","time_seen", TimeUnit.NANOSECONDS.toMillis(System.nanoTime())+"");
            //this.redisReadAndWrite.write("JnTPBef","Throughput", (throughputCounterBefore++)+"");
            //this.redisReadAndWrite.execute(input.f0+":"+input.f3+"", TimeUnit.NANOSECONDS.toMillis(System.nanoTime())+"");
        }
    }

    /**
     * write to redis after query
     */
    public static class WriteAdsToRedisAfterQuery extends RichFlatMapFunction<Tuple2<Boolean, Row>, String> {
        RedisReadAndWrite redisReadAndWrite;

        @Override
        public String toString() {
            return "";
        }
        @Override
        public void open(Configuration parameters) {
            this.redisReadAndWrite=new RedisReadAndWrite("redis",6379);

        }

        @Override
        public void flatMap(Tuple2<Boolean, Row> input, Collector<String> out) throws Exception {

            this.redisReadAndWrite.write(input.f1.getField(0)+":"+new Instant(input.f1.getField(2)).getMillis()+"","time_updated", TimeUnit.NANOSECONDS.toMillis(System.nanoTime())+"");
            this.redisReadAndWrite.write("JnTPAft","Throughput", (throughputAdsCounterAfter++)+"");


        }
    }


    private static Map<String, String> getFlinkConfs(Map conf) {
        String kafkaBrokers = getKafkaBrokers(conf);
        String zookeeperServers = getZookeeperServers(conf);

        Map<String, String> flinkConfs = new HashMap<String, String>();
        //flinkConfs.put("topic", getKafkaTopic(conf));
        flinkConfs.put("bootstrap.servers", kafkaBrokers);
        flinkConfs.put("zookeeper.connect", zookeeperServers);
        flinkConfs.put("jedis_server", "redis");
//        flinkConfs.put("jedis_server", getRedisHost(conf));
        // flinkConfs.put("time.divisor", getTimeDivisor(conf));
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
}
