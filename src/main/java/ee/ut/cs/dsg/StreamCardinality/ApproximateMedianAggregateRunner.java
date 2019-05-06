package ee.ut.cs.dsg.StreamCardinality;

import java.io.File;
import java.util.ArrayList;

import ee.ut.cs.dsg.StreamCardinality.ApproximateThroughput.ApproximateThroughput;
import ee.ut.cs.dsg.StreamCardinality.ApproximateAggregateFunction.*;
import ee.ut.cs.dsg.StreamCardinality.ApproximateThroughput.StreamCounter;
import ee.ut.cs.dsg.StreamCardinality.ExactAggregateFunction.MedianAggregateRunner;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import ee.ut.cs.dsg.StreamCardinality.ApproximateAggregateFunction.LogLogAggregationFunction;
import ee.ut.cs.dsg.StreamCardinality.ApproximateThroughput.ApproximateThroughputCounter;


import javax.annotation.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

public class ApproximateMedianAggregateRunner {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final String dir = System.getProperty("user.dir");

        DataStream<Tuple3<Long, String, Integer>> stream2 = env.addSource(new YetAnotherSource(dir+File.separator+"data"+File.separator+"data.csv"));

        final SingleOutputStreamOperator<Tuple3<Long, String, Integer>> result2 =
                stream2
                        .assignTimestampsAndWatermarks(new MedianAggregateRunner.TimestampsAndWatermarks());
        long startTime = System.nanoTime();

        ApproximateThroughput myThroughputMetrics = new ApproximateThroughput();


        AggregateFunction agg;

        switch (args[0]){
            case "MedianCKMSAggregationFunction":
                agg = new MedianCKMSAggregationFunction();
                break;
            case "LogLogAggregationFunction":
                agg = new LogLogAggregationFunction();
                break;
            case "AdaptiveCountingAggregationFunction":
                agg = new AdaptiveCountingAggregationFunction();
                break;
            case "HyperLogLogAggregationFunction":
                agg = new HyperLogLogAggregationFunction();
                break;
            case "LinearCountingAggregationFunction":
                agg = new LinearCountingAggregationFunction();
                break;
            case "FlajoletMartinAggregationFunction":
                agg = new FlajoletMartinAggregationFunction();
                break;
            case "KMinValuesAggregationFunction":
                agg = new KMinValuesAggregationFunction();
                break;
            case "BJKSTAggregationFunction":
                agg = new BJKSTAggregationFunction();
                break;
            case "CountThenEstimateAggregationFunction":
                agg = new CountThenEstimateAggregationFunction();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + args[1]);
        }

        result2
                .keyBy(1)
                .timeWindow( Time.of(200, MILLISECONDS), Time.of(100, MILLISECONDS))
                .aggregate(agg)
                .print();

        long endTime = System.nanoTime();
        long duration = (endTime - startTime);

        env.execute();

        System.out.print("Running duration is: ");
        System.out.println(duration);



        // -> this part for testing only

        ApproximateThroughputCounter myAT = ApproximateThroughputCounter.getInstance();
        ArrayList<Tuple2<String, Integer>> Records = myAT.getResults();

        System.out.println("records in metrics group: ");
        for (Tuple2 pairs: Records)
        {
            System.out.print(pairs.f0);
            System.out.print(" ");
            System.out.print(pairs.f1);
            System.out.println();
        }
        String pointer = Records.get(2).f0;
        int count = 0;
        for (Tuple2 pairs: Records)
        {
            if (pairs.f0 == pointer)
            {
                count += 1;
            }
        }
        System.out.print("same id with first one has this nr: ");
        System.out.println(count);
        System.out.print("merge func call time:");
        StreamCounter scnt = StreamCounter.getInstance();
        System.out.println(scnt.getResults());

        Integer WindowNr = scnt.getOf(1);
        System.out.print("Window created nr: ");
        System.out.println(WindowNr);
        // -> endpoint

    }


    public static class TimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<Tuple3<Long, String, Double>> {
        private final long maxOutOfOrderness = seconds(20).toMilliseconds(); // 5 seconds
        private long currentMaxTimestamp;
        private long startTime = System.currentTimeMillis();

        @Override
        public long extractTimestamp(final Tuple3<Long, String, Double> element, final long previousElementTimestamp) {
            long timestamp = element.f0;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp);
        }

    }
}
