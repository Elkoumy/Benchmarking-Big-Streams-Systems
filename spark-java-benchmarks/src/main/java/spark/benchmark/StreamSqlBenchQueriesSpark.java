package spark.benchmark;
import org.apache.spark.api.java.JavaRDD;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.functions.*;
import org.json.JSONObject;
import scala.Tuple5;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
//        .{ConsumerStrategies, KafkaUtils, LocationStrategies};


public class StreamSqlBenchQueriesSpark {
    private static void sparkStreamingSqkExample() throws  Exception{
        //        Dataset<Row> tobeSelect=inputAsString.select(functions.from_json(inputAsString.col("value"),purchasesSchema,new HashMap<>()));
          /*      Dataset<Row> calcLatency = Query.groupBy(
                functions.window(Query.col("timestamp"), "1 seconds", "1 seconds")
        ).sum("UDF:getTSDiff(ltcID)");*/

/*        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaDStream<String> customReceiverStream = jssc.receiverStream(new JavaCustomReceiver("localhost", 6666));*/

//        SparkSession spark = SparkSession.builder().master("local[3]")
        SparkSession spark = SparkSession.builder().master("spark://172.17.77.47:7077")
                .appName("sparkStreamingSqlBenchmark")
                .getOrCreate();
//        StructType purchasesSchema = new StructType().add("userID", "integer").add("gemPackID", "Integer").add("price", "integer").add("timeStamp", "long");
        Dataset<Row> purchasesStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-node-01:9092,kafka-node-02:9092,kafka-node-03:9092")
                .option("subscribe", "purchases")
//                .option("startingOffsets", "earliest")
                .load();

        Dataset<Row> adsStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-node-01:9092,kafka-node-02:9092,kafka-node-03:9092")
                .option("subscribe", "ads")
//                .option("startingOffsets", "earliest")
                .load();

        Dataset<Row> purchasesAsStr=purchasesStream.selectExpr( "CAST(value AS STRING)");
        Dataset<Row> adsAsStr=adsStream.selectExpr( "CAST(value AS STRING)");

        Encoder<Users> userEncoder = Encoders.bean(Users.class);
        Encoder<Ads> adsEncoder = Encoders.bean(Ads.class);
        Encoder<UserWithLatencyStamp> userWithLatencyStampEncoder = Encoders.bean(UserWithLatencyStamp.class);


        Dataset<Users> purchasesTupples=purchasesAsStr.map(new GetPurchases(),userEncoder);
        Dataset<Ads> adsTupples=adsAsStr.map(new GetAds(),adsEncoder);
        Dataset<Row> purchasesTupples2=purchasesTupples.toDF();
        Dataset<Row> adsTupples2=adsTupples.toDF();

        Dataset<Row> purchasesTuppleswithLtcStamp=purchasesTupples2.withColumn("ltcID", functions.concat(purchasesTupples2.col("userID"),
                functions.lit(" "+System.currentTimeMillis())));
        Dataset<Row> adsTuppleswithLtcStamp=adsTupples2.withColumn("ltcID", functions.concat(adsTupples2.col("userID"),
                functions.lit(" "+System.currentTimeMillis())));

        spark.udf().register("getTSDiff", new UDF1<String, Long>() {
            @Override
            public Long call(String ltcID) throws Exception {
                String latencyAttr[]=ltcID.split(" ");
                return Math.abs(System.currentTimeMillis()-Long.parseLong(latencyAttr[1]));
            }
        }, DataTypes.LongType);

        purchasesTuppleswithLtcStamp.createOrReplaceTempView("purchases");
        adsTuppleswithLtcStamp.createOrReplaceTempView("ads");

        /**************************************************************
         * 1- Projection //Get all purchased gem packs work
         * ************************************************************/
        Dataset<Row> Query = spark.sql("SELECT  userID, gemPackID, timeStamp,getTSDiff(ltcID) as ltc from ads");
        Query.createOrReplaceTempView("purchasesQuery");
        Dataset<Row> QueryToClacLatecy=spark.sql("SELECT  window.start, window.end, count(userID), sum(ltc) from purchasesQuery GROUP BY window(timeStamp, '1 seconds' , '1 seconds')");
        /**************************************************************
         * 1- 2- Filtering// Get the purchases of specific user
         * ************************************************************/
/*
        Dataset<Row> Query = spark.sql("SELECT  userID, gemPackID, price,timeStamp,getTSDiff(ltcID) as ltcSum from purchases where price >20");
        Query.createOrReplaceTempView("purchasesQuery");
        Dataset<Row> QueryToClacLatecy=spark.sql("SELECT  window.start, window.end, count(userID), sum(ltcSum) from purchasesQuery GROUP BY window(timeStamp, '1 seconds' , '1 seconds')");

*/
        /**************************************************************
         * 3- Group by // Getting revenue from gempack when it exceeds specified amount.
         * ************************************************************/
        //register functions
/*
        spark.udf().register("getDifferences", new MySum());
        spark.udf().register("getTheSpecialValue", new GetSpecialValue());
        Dataset<Row> Query = spark.sql("SELECT  gemPackID,sum(price)as revenue,window.start, window.end,getDifferences(ltcID),count(*),getTheSpecialValue(userID) from purchases GROUP BY window(timeStamp, '2 seconds' , '2 seconds'),gemPackID");
        // in this types of queries, we can print them directly. However, we need to add more map function to check the special value.
*/


        /**************************************************************
         * 7- Inner join // Getting revenue from each ad (which ad triggered purchase)
         * * ************************************************************/
     /*   Dataset<Row> Query = spark.sql("SELECT  purchases.userID,purchases.gemPackID, purchases.price, purchases.timeStamp, getTSDiff(purchases.ltcID) as ltcSum " +
                "from purchases inner join ads " +
                "on purchases.userID = ads.userID " +
                "and purchases.gemPackID = ads.gemPackID " +
                "and purchases.timeStamp  BETWEEN ads.timeStamp - INTERVAL '1' SECOND AND ads.timeStamp + INTERVAL 1 SECOND").withWatermark("timeStamp","0 second");
        Query.createOrReplaceTempView("purchasesQuery");
        Dataset<Row> QueryToClacLatecy=spark.sql("SELECT  window.start, window.end, count(userID), sum(ltcSum) from purchasesQuery GROUP BY window(timeStamp, '1 seconds' , '1 seconds')").withWatermark("start","1 second");
*/



        StreamingQuery query = QueryToClacLatecy.writeStream()
                .outputMode("update")
                .format("console")
//                .trigger(Trigger.Continuous("1 second"))
                .trigger(Trigger.ProcessingTime("0 second"))
                .start();
        query.awaitTermination();

/*        StreamingQuery query2 = QueryToClacLatecy.writeStream()
                .outputMode("update")
                .format("console")
                .start();
        query2.awaitTermination();*/


        StreamingQuery spv= Query.map(new CheckSpecialValue(),Encoders.INT()).writeStream().outputMode("update")
                .format("console")
                .start();
        spv.awaitTermination();

    }

    public static void main(String[] args) throws Exception{
        System.setProperty("hadoop.home.dir", "c:\\winutil\\");
    /*    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        System.out.println(dtf.format(now));*/

        // Setting the log level. you will need to change thi level if you need to trace errors.
        Logger.getLogger("org").setLevel(Level.FATAL);
        Logger.getLogger("akka").setLevel(Level.FATAL);
        sparkStreamingSqkExample();


    }



    public static class GetPurchases implements MapFunction<Row, Users> {
        public Users call(Row input) {
            String inputStr=input.toString().substring(1,(input.toString().length())-1);
            JSONObject obj = new JSONObject(inputStr);
            Timestamp ts=new Timestamp(obj.getLong("timeStamp"));
            return new Users(
                    obj.getInt("userID"),
                    obj.getInt("gemPackID"),
                    obj.getInt("price"),
                    ts
            );
        }
    }


    public static class GetAds implements MapFunction<Row, Ads> {
        public Ads call(Row input) {
            String inputStr=input.toString().substring(1,(input.toString().length())-1);
            JSONObject obj = new JSONObject(inputStr);
            Timestamp ts=new Timestamp(obj.getLong("timeStamp"));
            return new Ads(
                    obj.getInt("userID"),
                    obj.getInt("gemPackID"),
                    ts
            );
        }
    }

    public static class AddPurchaseLatencyId implements MapFunction<Row, UserWithLatencyStamp> {
        public UserWithLatencyStamp call(Row input) {
            return new UserWithLatencyStamp(
                    input.getInt(0),
                    input.getInt(1),
                    input.getInt(2),
                    input.getInt(3),
                    input.getString(4)+""+System.currentTimeMillis()
            );
        }
    }


    public static class CheckSpecialValue implements MapFunction<Row, Integer> {
        @Override
        public Integer call(Row input) throws Exception {
            System.out.println(input.get(0).toString());
            if (input.get(0).toString().equals("-1000000")){
                //System.exit(0);

            }
            return null;
        }
    }

    //sum the differences
    public static class MySum extends UserDefinedAggregateFunction {

        private StructType inputSchema;
        private StructType bufferSchema;

        public MySum() {
            List<StructField> inputFields = new ArrayList<>();
            inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.StringType, true));
            inputSchema = DataTypes.createStructType(inputFields);

            List<StructField> bufferFields = new ArrayList<>();
            bufferFields.add(DataTypes.createStructField("sum", DataTypes.LongType, true));
            bufferSchema = DataTypes.createStructType(bufferFields);
        }
        // Data types of input arguments of this aggregate function
        public StructType inputSchema() {
            return inputSchema;
        }
        // Data types of values in the aggregation buffer
        public StructType bufferSchema() {
            return bufferSchema;
        }
        // The data type of the returned value
        public DataType dataType() {
            return DataTypes.LongType;
        }
        // Whether this function always returns the same output on the identical input
        public boolean deterministic() {
            return true;
        }
        // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
        // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
        // the opportunity to update its values. Note that arrays and maps inside the buffer are still
        // immutable.
        public void initialize(MutableAggregationBuffer buffer) {
            buffer.update(0, 0L);
        }
        // Updates the given aggregation buffer `buffer` with new input data from `input`
        public void update(MutableAggregationBuffer buffer, Row input) {
            if (!input.isNullAt(0)) {
                String latencyAttr[]=input.getString(0).split(" ");
                Long timeDifference=Math.abs(System.currentTimeMillis()-Long.parseLong(latencyAttr[1]));
                long updatedSum = buffer.getLong(0) + timeDifference;
                buffer.update(0, updatedSum);
            }
        }

        // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            long mergedSum = buffer1.getLong(0) + buffer2.getLong(0);
            buffer1.update(0, mergedSum);
        }
        // Calculates the final result
        public Long evaluate(Row buffer) {
            return  buffer.getLong(0) ;
        }
    }


    // get special value
    public static class GetSpecialValue extends UserDefinedAggregateFunction {

        private StructType inputSchema;
        private StructType bufferSchema;

        public GetSpecialValue() {
            List<StructField> inputFields = new ArrayList<>();
            inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.IntegerType, true));
            inputSchema = DataTypes.createStructType(inputFields);

            List<StructField> bufferFields = new ArrayList<>();
            bufferFields.add(DataTypes.createStructField("Spv", DataTypes.IntegerType, true));
            bufferSchema = DataTypes.createStructType(bufferFields);
        }
        // Data types of input arguments of this aggregate function
        public StructType inputSchema() {
            return inputSchema;
        }
        // Data types of values in the aggregation buffer
        public StructType bufferSchema() {
            return bufferSchema;
        }
        // The data type of the returned value
        public DataType dataType() {
            return DataTypes.IntegerType;
        }
        // Whether this function always returns the same output on the identical input
        public boolean deterministic() {
            return true;
        }
        // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
        // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
        // the opportunity to update its values. Note that arrays and maps inside the buffer are still
        // immutable.
        public void initialize(MutableAggregationBuffer buffer) {
            buffer.update(0, 0);
        }
        // Updates the given aggregation buffer `buffer` with new input data from `input`
        public void update(MutableAggregationBuffer buffer, Row input) {
            if (!input.isNullAt(0)) {
                Integer updatedCount = input.getInt(0);
                buffer.update(0, updatedCount);
            }
        }
        // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            Integer mergedCount = buffer2.getInt(0);
            buffer1.update(0, mergedCount);
        }
        // Calculates the final result
        public Integer evaluate(Row buffer) {
            return  buffer.getInt(0) ;
        }
    }
}

