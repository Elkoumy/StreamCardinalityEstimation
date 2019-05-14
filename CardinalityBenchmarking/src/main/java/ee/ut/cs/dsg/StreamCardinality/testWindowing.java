package ee.ut.cs.dsg.StreamCardinality;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.flinkconnector.KeyedScottyWindowOperator;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityWindowFunctions.HyperLogLogWindowFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import de.tub.dima.scotty.core.windowType.*;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class testWindowing {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

         String dir = System.getProperty("user.dir");
            dir="C:\\Gamal Elkoumy\\PhD\\OneDrive - Tartu Ülikool\\Stream Processing\\SWAG & Scotty\\DataGeneration\\normal_distribution_long.csv";
        DataStream<Tuple3<Long, String, Long>> stream2 = env.addSource(new YetAnotherSource(dir,10*1000*1,10,1000));

        KeyedScottyWindowOperator<Tuple, Tuple3<Long,String,Long>, Tuple4<Long,String,Long,Long>> windowOperator =
                new KeyedScottyWindowOperator<>(new HyperLogLogWindowFunction());
        windowOperator.addWindow(new SlidingWindow(WindowMeasure.Time, 1000, 500));
//        windowOperator.addWindow(new TumblingWindow(WindowMeasure.Time, 10000));
        stream2
                .keyBy(0)
                .process(windowOperator)
                .process(new latencyProcessFunctionScotty())
//                .map(x -> x.getAggValues().get(0))
                .print();


//        stream2
//                .keyBy(1)
//                .timeWindow( Time.of(1000, MILLISECONDS), Time.of(500, MILLISECONDS))
//                //                .timeWindow( Time.of(1000, MILLISECONDS))
////                .trigger(new CustomEventTimeTrigger())
//                .aggregate(new LinearCountingAggregationFunction())
//                //                .aggregate(new MedianDoubleHeapAggregationFunction())
//                .print()
//        ;
        env.execute("demo");


    }


    private static class latencyProcessFunctionScotty extends ProcessFunction<AggregateWindow<Tuple4<Long, String,  Long,Long>>, Tuple3<Long,String,Long>> {

        @Override
        public void processElement(AggregateWindow<Tuple4<Long, String,Long, Long>> tuple4AggregateWindow, Context context, Collector<Tuple3<Long, String, Long>> collector) throws Exception {


//            ExperimentConfiguration.async.hset("w"+tuple4AggregateWindow.getStart()+"|"+tuple4AggregateWindow.getAggValues().get(0).f1, "window_end",tuple4AggregateWindow.getEnd()+"");
            collector.collect(new Tuple3<>(tuple4AggregateWindow.getAggValues().get(0).f0,tuple4AggregateWindow.getAggValues().get(0).f1,tuple4AggregateWindow.getAggValues().get(0).f2));
        }
    }
}