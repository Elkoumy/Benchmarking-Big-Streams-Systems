package flink.benchmark;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
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

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Properties;


public class StreamSqlBenchQueriesFlink3 {
    public static int throughputCounterBefore=0;
    public static int throughputCounterAfter=0;
    public static int throughputAccomulationcount=0;
    public static void main(String[] args) {
        //ParameterTool params = ParameterTool.fromArgs(args);
        //String ip = params.getRequired("ip");
        //int port=Integer.parseInt(params.getRequired("port"));
        String ip="localhost";
        // port=6666;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

       /* DataStreamSource<Tuple4<Integer, Integer, Integer, Long>> purchaseInputTuple=env.addSource(new YetAnotherSourceSocketPurchase(ip,6666));
        DataStreamSource<Tuple3<Integer, Integer, Long>> adsInputTuple=env.addSource(new YetAnotherSourceSocketAd(ip,7777));
*/
        Properties props = new Properties();
        props.setProperty("zookeeper.connect", "zookeeper-node-01:2181");
        props.setProperty("bootstrap.servers", "kafka-node-01:9092,kafka-node-02:9092,kafka-node-03:9092");
        // not to be shared with another job consuming the same topic
        props.setProperty("group.id", "flink-group");



        DataStream<String> purchasesStream = env
                .addSource(new FlinkKafkaConsumer011<String>(
                        "purchases",
                        new SimpleStringSchema(),
                        props)).
                        setParallelism(1)
                ;

        DataStream<String> adsStream = env
                .addSource(new FlinkKafkaConsumer011<String>(
                        "ads",
                        new SimpleStringSchema(),
                        props)).
                        setParallelism(1)
                ;


        DataStream<Tuple4<Integer, Integer, Integer, Long>> purchaseWithTimestampsAndWatermarks =
                purchasesStream
                        .flatMap(new PurchasesParser())
                        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple4<Integer, Integer, Integer, Long>>(Time.seconds(10)) {

                            @Override
                            public long extractTimestamp(Tuple4<Integer, Integer, Integer, Long> element) {
                                //System.out.println("p "+element.f0+"  "+element.f1+"  "+element.f2+"  "+element.f3);
                                return element.getField(3);
                            }
                        });

        DataStream<Tuple3<Integer, Integer, Long>> adsWithTimestampsAndWatermarks =
                adsStream
                        .flatMap( new AdsParser())
                        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Integer, Integer, Long>>(Time.seconds(1)) {
                            @Override
                            public long extractTimestamp(Tuple3<Integer, Integer, Long> element) {
                                //System.out.println("a "+element.f0+"  "+element.f1+"  "+element.f2);
                                return element.getField(2);
                            }
                        });
        adsWithTimestampsAndWatermarks.print();


        Table purchasesTable = tEnv.fromDataStream(purchaseWithTimestampsAndWatermarks, "userID, gemPackID,price, rowtime.rowtime");
        Table adsTable = tEnv.fromDataStream(adsWithTimestampsAndWatermarks, "userID, gemPackID, rowtime.rowtime");
        tEnv.registerTable("purchasesTable", purchasesTable);
        tEnv.registerTable("adsTable", adsTable);

        //mapper to write key and value of each element ot redis
/*          //if i used this the event time as a key not written the same before and after
            DataStream<Tuple2<String,String>> writeToRedisBefore = purchaseWithTimestampsAndWatermarks.map(new MapFunction<Tuple4<Integer, Integer, Integer, Long>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple4<Integer, Integer, Integer, Long> inputTuple) {
                System.out.println("before "+"Key>"+inputTuple.f0+" "+inputTuple.f3+" "+"value> "+new Instant(System.currentTimeMillis()) );
                return new Tuple2<>(inputTuple.f0+"",inputTuple.f3+"");
            }
        });*/



        //Workloads
        //================================General======================
        /**************************************************************
         * 1- Projection//Get all purchased gem pack
         * TODO> return value of writeToRedisAfter is not correct
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

        Table result = tEnv.sqlQuery("SELECT  userID, gemPackID, rowtime from purchasesTable");

        DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);

        DataStream<Tuple2<String,String>> writeToRedisAfter = queryResultAsDataStream.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("after "+"Key> p"+inputTuple.f1.getField(0)+""+new Instant(inputTuple.f1.getField(2)).getMillis()+" value> "+System.currentTimeMillis());
                System.out.println( "throughput> " +throughputCounterAfter++); //for throughput
                return new Tuple2<>(inputTuple.f1.getField(0)+"",System.currentTimeMillis()+""); //for latency

            }
        });*/

        /**************************************************************
         * 2- Filtering// Get the purchases of specific user//
         * TODO> I think in this kind of queries we should not calculate throughput. because we will not be able to count the filtered out tuples
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

        Table result = tEnv.sqlQuery("SELECT  userID, gemPackID, rowtime from purchasesTable WHERE price>20");

        DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);

        DataStream<Tuple2<String,String>> writeToRedisAfter = queryResultAsDataStream.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("after "+"Key> p"+inputTuple.f1.getField(0)+""+new Instant(inputTuple.f1.getField(2)).getMillis()+" value> "+System.currentTimeMillis());
                System.out.println( "throughput> " +throughputCounterAfter++); //for throughput
                return new Tuple2<>(inputTuple.f1.getField(0)+"",System.currentTimeMillis()+""); //for latency

            }
        });*/
        /**************************************************************
         * 3- Group by & having // Getting revenue from gempack when it exceeds specified amount
         * TODO> I think in this kind of queries we should not calculate throughput. because we will not be able to count the filtered out tuples
         * ************************************************************/
    /*    DataStream<Tuple2<Boolean, Row>> PurchaseDataStreamTable = tEnv.toRetractStream(purchasesTable, Row.class);
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
        Table result = tEnv.sqlQuery("SELECT  gemPackID,sum(price)as revenue,getKeyAndValue(userID, rowtime),count(*)   from purchasesTable GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND),gemPackID HAVING sum(price)>400 ");

        //for the metrics calculation after
        DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);

        DataStream<Tuple2<String,String>> writeToRedisAfter = queryResultAsDataStream.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("after "+"Key> p"+inputTuple.f1.getField(2) +" value> "+System.currentTimeMillis()); //for latency
                throughputAccomulationcount+=Integer.parseInt(inputTuple.f1.getField(3).toString());
                System.out.println("throughput > " + throughputAccomulationcount); //for throughput
                return new Tuple2<>(inputTuple.f1.getField(2)+"",System.currentTimeMillis()+""); //for latency

            }
        });
        */
        //================================WINDOWING======================
        /**************************************************************
         * 4- Tumbling Window// Getting revenue obtained  from each gem pack over fixed period of time
         * ************************************************************/
/*    DataStream<Tuple2<Boolean, Row>> PurchaseDataStreamTable = tEnv.toRetractStream(purchasesTable, Row.class);
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
        Table result = tEnv.sqlQuery("SELECT  gemPackID,sum(price)as revenue,getKeyAndValue(userID, rowtime),count(*)   from purchasesTable GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND),gemPackID");

        //for the metrics calculation after
        DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);

        DataStream<Tuple2<String,String>> writeToRedisAfter = queryResultAsDataStream.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("after "+"Key> p"+inputTuple.f1.getField(2) +" value> "+System.currentTimeMillis()); //for latency
                throughputAccomulationcount+=Integer.parseInt(inputTuple.f1.getField(3).toString());
                System.out.println("throughput > " + throughputAccomulationcount); //for throughput
                return new Tuple2<>(inputTuple.f1.getField(2)+"",System.currentTimeMillis()+""); //for latency

            }
        });*/
        /**************************************************************
         * 5- Sliding Window //Getting revenue obtained from each gem pack over fixed overlapped period of time
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
        Table result = tEnv.sqlQuery("SELECT  gemPackID,sum(price)as revenue,getKeyAndValue(userID, rowtime),count(*)   from purchasesTable GROUP BY HOP(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND),gemPackID");

        //for the metrics calculation after
        DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);

        DataStream<Tuple2<String,String>> writeToRedisAfter = queryResultAsDataStream.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("after "+"Key> p"+inputTuple.f1.getField(2) +" value> "+System.currentTimeMillis()); //for latency
                throughputAccomulationcount+=Integer.parseInt(inputTuple.f1.getField(3).toString());
                System.out.println("throughput > " + throughputAccomulationcount); //for throughput
                return new Tuple2<>(inputTuple.f1.getField(2)+"",System.currentTimeMillis()+""); //for latency

            }
        });*/
        /**************************************************************
         * 6- Session window //Getting Revenue obtained from each gem pack after each specific period of inactivity
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
        Table result = tEnv.sqlQuery("SELECT  gemPackID,sum(price)as revenue,getKeyAndValue(userID, rowtime),count(*)   from purchasesTable GROUP BY SESSION(rowtime, INTERVAL '2' SECOND),gemPackID");

        //for the metrics calculation after
        DataStream<Tuple2<Boolean, Row>> queryResultAsDataStream = tEnv.toRetractStream(result, Row.class);

        DataStream<Tuple2<String,String>> writeToRedisAfter = queryResultAsDataStream.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("after "+"Key> p"+inputTuple.f1.getField(2) +" value> "+System.currentTimeMillis()); //for latency
                throughputAccomulationcount+=Integer.parseInt(inputTuple.f1.getField(3).toString());
                System.out.println("throughput > " + throughputAccomulationcount); //for throughput
                return new Tuple2<>(inputTuple.f1.getField(2)+"",System.currentTimeMillis()+""); //for latency

            }
        });*/
        //================================JOINS======================
        /**************************************************************
         * 7- Inner join // Getting revenue from each ad (which ad triggered purchase)
         * TODO>Throughput in joins is not representative (look at previous papers amd discuss with the geeks)
         * ************************************************************/
        DataStream<Tuple2<Boolean, Row>> PurchaseDataStreamTable = tEnv.toRetractStream(purchasesTable, Row.class);
        DataStream<Tuple2<String,String>> writeToRedisBefore = PurchaseDataStreamTable.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("before "+"Key> p"+inputTuple.f1.getField(0)+""+new Instant(inputTuple.f1.getField(3)).getMillis()+" value> "+System.currentTimeMillis());
                System.out.println( throughputCounterBefore++);//for throughput
                return new Tuple2<>(inputTuple.f1.getField(0)+"",System.currentTimeMillis()+"");//for latency

            }
        });

  // I think no need to insert the other table since we are in the query projecting all from single tale so I hashed this mapper.
/*      DataStream<Tuple2<Boolean, Row>> adDataStreamTable = tEnv.toRetractStream(adsTable, Row.class);
        DataStream<Tuple2<String,String>> writeAdToRedisBefore = adDataStreamTable.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(Tuple2<Boolean, Row> inputTuple) {
                System.out.println("before "+"Key> a"+inputTuple.f1.getField(0)+""+new Instant(inputTuple.f1.getField(2)).getMillis()+" value> "+System.currentTimeMillis());
                System.out.println( throughputCounterBefore++);//for throughput
                return new Tuple2<>(inputTuple.f1.getField(0)+"",System.currentTimeMillis()+"");//for latency

            }
        });*/
        // register function
        tEnv.registerFunction("getKeyAndValue", new KeyValueGetter());

        Table result = tEnv.sqlQuery("SELECT  p.userID,p.gemPackID,p.price, p.rowtime  " +
                "from purchasesTable p inner join adsTable a " +
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
        });

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
            env.execute("flink SQL Streaming Benchmarking");
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
                return kv.userID+""+new Instant(kv.timestmp).getMillis();
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


    private static class PurchasesParser  implements FlatMapFunction<String,Tuple4<Integer, Integer, Integer, Long>> {
        @Override
        public void flatMap(String input, Collector<Tuple4<Integer, Integer, Integer, Long>> out) throws Exception {
            JSONObject obj = new JSONObject(input);
            Tuple4<Integer,Integer,Integer,Long> tuple =
                    new Tuple4<Integer,Integer,Integer,Long> (
                            obj.getInt("userID"),
                            obj.getInt("gemPackID"),
                            obj.getInt("price"),
                            obj.getLong("timeStamp")
                    );

            out.collect(tuple);
        }

    }

    private static class AdsParser implements  FlatMapFunction<String,Tuple3<Integer, Integer,  Long>>{

        @Override
        public void flatMap(String input, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
            JSONObject obj = new JSONObject(input);
            Tuple3<Integer,Integer,Long> tuple =
                    new Tuple3<Integer,Integer,Long> (
                            obj.getInt("userID"),
                            obj.getInt("gemPackID"),
                            obj.getLong("timeStamp")
                    );

            out.collect(tuple);
        }
    }


}
