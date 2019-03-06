package ee.ut.cs.dsg.SWAGwithSCOTTY;



import de.tub.dima.scotty.core.TimeMeasure;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.flinkconnector.KeyedScottyWindowOperator;
//import ee.ut.cs.dsg.SWAGwithSCOTTY.WindowFunctions.DoubleHeapWindowFunction;
//import ee.ut.cs.dsg.SWAGwithSCOTTY.WindowFunctions.RedBlackWindowFunction;
//import ee.ut.cs.dsg.SWAGwithSCOTTY.WindowFunctions.SkipListWindowFunction;
import ee.ut.cs.dsg.SWAGwithSCOTTY.ApproximateWindowFunctions.*;
import ee.ut.cs.dsg.SWAGwithSCOTTY.ExactWindowFunctions.VEBWindowFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import javax.annotation.Nullable;

import java.util.concurrent.TimeUnit;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

public class ApproximateMedianRunnder {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        final String dir = System.getProperty("user.dir");

        DataStream<Tuple3<Long, String, Double>> stream2 = env.addSource(new YetAnotherSource(dir+"\\data\\data.csv"));


        KeyedScottyWindowOperator<Tuple, Tuple3<Long, String, Double>, Tuple3<Long, String, Double> > windowOperator =
                new KeyedScottyWindowOperator<>( new SimpleQuantileWindowfunction());

        windowOperator.addWindow(new SlidingWindow(WindowMeasure.Time, TimeMeasure.of(5, TimeUnit.MILLISECONDS).toMilliseconds(), TimeMeasure.of(2, TimeUnit.MILLISECONDS).toMilliseconds()));



        final SingleOutputStreamOperator<Tuple3<Long, String, Double>> result2 =
                stream2
                        .assignTimestampsAndWatermarks(new MedianRunner.TimestampsAndWatermarks());

        result2.keyBy(1)
                .process(windowOperator);

        result2.print();

        env.execute();

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
