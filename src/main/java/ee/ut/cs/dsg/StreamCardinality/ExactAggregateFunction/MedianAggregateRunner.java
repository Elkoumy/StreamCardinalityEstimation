package ee.ut.cs.dsg.StreamCardinality.ExactAggregateFunction;


//import ee.ut.cs.dsg.SWAGwithSCOTTY.ExactMedian.MedianSkipListAggregationFunction;
import ee.ut.cs.dsg.StreamCardinality.ApproximateAggregateFunction.AdaptiveCountingAggregationFunction;
import ee.ut.cs.dsg.StreamCardinality.ApproximateAggregateFunction.MedianCKMSAggregationFunction;
import ee.ut.cs.dsg.StreamCardinality.YetAnotherSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.io.File;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

public class MedianAggregateRunner {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final String dir = System.getProperty("user.dir");

        DataStream<Tuple3<Long, String, Integer>> stream2 = env.addSource(new YetAnotherSource(dir+File.separator+"data"+File.separator+"data.csv"));

        final SingleOutputStreamOperator<Tuple3<Long, String, Integer>> result2 =
                stream2
                        .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());
        long startTime = System.nanoTime();


        result2
                .keyBy(1)
                .timeWindow( Time.of(2, MILLISECONDS), Time.of(1, MILLISECONDS))
                .aggregate(new AdaptiveCountingAggregationFunction())
//                .print()
        ;

        long endTime = System.nanoTime();

        long duration = (endTime - startTime);

        env.execute();
        System.out.println(duration);
    }


    public static class TimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<Tuple3<Long, String, Integer>> {
        private final long maxOutOfOrderness = seconds(20).toMilliseconds(); // 5 seconds
        private long currentMaxTimestamp;
        private long startTime = System.currentTimeMillis();

        @Override
        public long extractTimestamp(final Tuple3<Long, String, Integer> element, final long previousElementTimestamp) {
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
