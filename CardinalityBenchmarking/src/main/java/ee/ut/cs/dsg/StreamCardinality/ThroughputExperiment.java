package ee.ut.cs.dsg.StreamCardinality;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;

import ee.ut.cs.dsg.StreamCardinality.ExactAggregateFunction.*;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityWindowFunctions.*;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityAggregateFunction.*;
import ee.ut.cs.dsg.StreamCardinality.utils.KeyedScottyWindowOperatorWithTrigger;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.Random;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ThroughputExperiment {
    public static void main(String[] args) throws Exception {

        /**
         * Setting experiment configurations
         */
        ExperimentConfiguration.initialize();
        ExperimentConfiguration.experimentType= ExperimentConfiguration.ExperimentType.Throughput;


//        KLL scotty "C:\Gamal Elkoumy\PhD\OneDrive - Tartu Ülikool\Stream Processing\SWAG & Scotty\DataGeneration\data" 100000 normal
//        AC scotty "C:\Users\Anders\Desktop\data" 100000 normal
        /**
         * Reading system parameters from args
         */
        String algorithm=args[0];
        String approach=args[1];
        String inDir= args[2];
        String tps=args[3];
        String dist=args[4];
        System.out.println("****** Starting Latency experiment with the following settings *******");
        System.out.println("algorithm: "+algorithm+" , appraoch: "+approach+", input dir: "+inDir+", tps: "+tps+", distribution: "+dist);
        double epsilon=0.1;
        double quantile=0.5;

        /**
         * Creating the system env
         */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(10);
//        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /**
         * Reading input data
         */
        String inputDir=Paths.get(inDir,dist+".csv").toString();
        long tps_long;
        try {
            tps_long = Integer.parseInt(tps);
        }catch(Exception e){
            tps_long=1000l;
        }
        DataStream<Tuple3<Long, String, Long>> stream2 = env.addSource(new YetAnotherSource(inputDir, 60*1000*2,10,tps_long));


        /**
         * Branching over the aggregation functions and scotty slicing
         */


        if (approach.equals("aggregate")){
            AggregateFunction fn ;
            /**
             * aggregation functions
             */

            /**
             * Exact Algorithms
             */
            if (algorithm.equals("LL")){ //LogLog
                fn= new LogLogAggregationFunction();
            }else if(algorithm.equals("AC")){ //AdaptiveCounting
                fn=new AdaptiveCountingAggregationFunction();
            }else if (algorithm.equals("HLL")){ //HyperLogLog
                fn=new HyperLogLogAggregationFunction();
            }else if (algorithm.equals("LC")){//LinearCounting
                fn=new LinearCountingAggregationFunction();
            }else if (algorithm.equals("FM")){//FlajoletMartin
                fn = new FlajoletMartinAggregationFunction();
            }else if (algorithm.equals("CTE")){//CountThenEstimate
                fn = new CountThenEstimateAggregationFunction();
            }else if(algorithm.equals("HLLP")) { //HyperLogLogPlus
                fn = new HyperLogLogPlusAggregationFunction();
            }else if(algorithm.equals("KMV")) { //KMinValues
                fn = new KMinValuesAggregationFunction();
            }else if(algorithm.equals("BJKST")) { //BJKST
                fn = new BJKSTAggregationFunction();
            }else{
                fn= null;
            }
            stream2
                    .keyBy(1)
                    .timeWindow( Time.of(10000, MILLISECONDS), Time.of(500, MILLISECONDS))
                    .trigger(new CustomEventTimeTrigger())
                    .aggregate(fn, new throughputProcessFunction())
                    //                .aggregate(new MedianDoubleHeapAggregationFunction())
                    .print()
            ;
            env.execute(algorithm+":"+approach+":"+dist+":"+tps);

        }else {
            /**
             * Scotty
             */

            KeyedScottyWindowOperatorWithTrigger<Tuple, Tuple3<Long,String,Long>, Tuple4<Long,String,Long, Long>> windowOperator;


            /**
             * Approximate algorithms
             */
            if (algorithm.equals("LL")){ //LogLog
                //fn= new LogLogAggregationFunction();
                windowOperator=new KeyedScottyWindowOperatorWithTrigger<>(new LogLogWindowFunction());
            }else if(algorithm.equals("AC")){ //AdaptiveCounting
                //fn=new AdaptiveCountingAggregationFunction();
                windowOperator=new KeyedScottyWindowOperatorWithTrigger<>(new AdaptiveCountingWindowFunction());
            }else if (algorithm.equals("HLL")){ //HyperLogLog
                //fn=new HyperLogLogAggregationFunction();
                windowOperator=new KeyedScottyWindowOperatorWithTrigger<>(new HyperLogLogWindowFunction());
            }else if (algorithm.equals("LC")){//LinearCounting
                //fn=new LinearCountingAggregationFunction();
                windowOperator=new KeyedScottyWindowOperatorWithTrigger<>(new LinearCountingWindowFunction());
            }else if (algorithm.equals("FM")){//FlajoletMartin
                //fn = new FlajoletMartinAggregationFunction();
                windowOperator=new KeyedScottyWindowOperatorWithTrigger<>(new FlajoletMartinWindowFunction());
            }else if (algorithm.equals("CTE")){//CountThenEstimate
                //fn = new CountThenEstimateAggregationFunction();
                windowOperator=new KeyedScottyWindowOperatorWithTrigger<>(new CountThenEstimateWindowFunction());
            }else if(algorithm.equals("HLLP")) { //HyperLogLogPlus
                //fn = new HyperLogLogPlusAggregationFunction();
                windowOperator=new KeyedScottyWindowOperatorWithTrigger<>(new HyperLogLogPlusWindowFunction());
            }else if(algorithm.equals("KMV")) { //KMinValues
                //fn = new KMinValuesAggregationFunction();
                windowOperator=new KeyedScottyWindowOperatorWithTrigger<>(new KMinValuesWindowFunction());
            }else if(algorithm.equals("BJKST")) { //BJKST
                //fn = new BJKSTAggregationFunction();
                windowOperator=new KeyedScottyWindowOperatorWithTrigger<>(new BJKSTWindowFunction());
            }else{
                windowOperator= null;

            windowOperator.addWindow(new SlidingWindow(WindowMeasure.Time, 10000, 500));

            stream2
                    .keyBy(1)
                    .process(windowOperator)
                    .process(new throughputProcessFunctionScotty())
                    //                .map(x -> x.getAggValues().get(0).f2)
                    .print();

            env.execute(algorithm+":"+approach+":"+dist+":"+tps);

        }

        ExperimentConfiguration.connection.close();
        }
    }



    public static class TimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<Tuple3<Long, String, Long>> {
        //        private final long maxOutOfOrderness = seconds(20).toMilliseconds(); // 5 seconds
        private long currentMaxTimestamp=-1;
//        private long startTime = System.currentTimeMillis();

        @Override
        public long extractTimestamp(final Tuple3<Long, String, Long> element, final long previousElementTimestamp) {
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

    public static class CustomEventTimeTrigger extends Trigger<Object, TimeWindow> {
        private static final long serialVersionUID = 1L;
        private String key;
        private CustomEventTimeTrigger() {
        }

        public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
                key=((Tuple3<Long,String,Long>)element).f1;
                return TriggerResult.FIRE;
            } else {
                ctx.registerEventTimeTimer(window.maxTimestamp());
                return TriggerResult.CONTINUE;
            }
        }

        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
            if(time == window.maxTimestamp()){
                long cur = System.nanoTime();
                String key = ctx.toString().substring(ctx.toString().indexOf("(")+1,ctx.toString().indexOf(")"));

                ExperimentConfiguration.async.hset("w"+window.getStart()+"|"+key, "query_start", Long.toString(System.nanoTime()));
                ExperimentConfiguration.async.hset("w"+window.getStart()+"|"+key, "window_end_time", ""+window.getEnd());

                return TriggerResult.FIRE ;}
            else{return TriggerResult.CONTINUE;}
//            return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
        }

        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.deleteEventTimeTimer(window.maxTimestamp());
        }

        public boolean canMerge() {
            return false;
        }

        public void onMerge(TimeWindow window, OnMergeContext ctx) {
            long windowMaxTimestamp = window.maxTimestamp();
            if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
                ctx.registerEventTimeTimer(windowMaxTimestamp);
            }

        }

        public String toString() {
            return "EventTimeTrigger()";
        }

        public CustomEventTimeTrigger create() {
            return new CustomEventTimeTrigger();
        }
    }

    public static class DemoSource3 extends RichSourceFunction<Tuple3<Long,String,Long>> implements Serializable {

        private Random key;
        private Random value;
        private boolean canceled = false;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.key = new Random(42);
            this.value = new Random(43);
        }

        public long lastWatermark = 0;

        @Override
        public void run(SourceContext<Tuple3<Long,String,Long>> ctx) throws Exception {
            long start_time = System.currentTimeMillis();
//            long duration = 60000*3; //3 minutes
            long duration = 3000;

            while (!canceled) {

                long cur = System.currentTimeMillis();

                for(int i=0; i<5; i++) {
                    ctx.collectWithTimestamp(new Tuple3<>(System.currentTimeMillis(), "h1", value.nextLong()),cur);
//                    ctx.collect(new Tuple3<>(System.currentTimeMillis(), "h"+i%10, value.nextDouble()));
                }
                if (lastWatermark + 1000 < System.currentTimeMillis()) {
                    ctx.emitWatermark(new Watermark(System.currentTimeMillis()));
                    lastWatermark = System.currentTimeMillis();
                }
                Thread.sleep(1);


                if (System.currentTimeMillis()>start_time+duration){
                    cancel();
                }

            }
        }

        @Override
        public void cancel() { canceled = true; }
        }




    private static class throughputProcessFunction extends ProcessWindowFunction<Tuple4<Long,String,Long,Long>,Tuple3<Long,String,Long>,Tuple,TimeWindow >{


        @Override
        public void process(Tuple s, Context context, Iterable<Tuple4<Long, String, Long,Long>> iterable, Collector<Tuple3<Long, String, Long>> collector) throws Exception {

            ExperimentConfiguration.async.hset("w"+context.window().getStart()+"|"+s.getField(0), "window_count", iterable.iterator().next().f3.toString());
            ExperimentConfiguration.async.hset("w"+context.window().getStart()+"|"+s.getField(0), "window_end",context.window().getEnd()+"");
            ExperimentConfiguration.async.hset("w"+context.window().getStart()+"|"+s.getField(0), "out_time",System.nanoTime()+"");
            if (iterable.iterator().hasNext()) {
                Tuple4<Long, String, Long, Long> res = iterable.iterator().next();
                collector.collect(new Tuple3<Long,String,Long>(res.f0,res.f1,res.f2));
            }
        }
    }


    private static class throughputProcessFunctionScotty extends ProcessFunction<AggregateWindow<Tuple4<Long, String, Long, Long>>, Tuple3<Long,String,Long>> {
        @Override
        public void processElement(AggregateWindow<Tuple4<Long, String, Long, Long>> tuple4AggregateWindow, Context context, Collector<Tuple3<Long, String, Long>> collector) throws Exception {
            ExperimentConfiguration.async.hset("w"+tuple4AggregateWindow.getStart()+"|"+tuple4AggregateWindow.getAggValues().get(0).f1, "window_count", tuple4AggregateWindow.getAggValues().get(0).f3+"");
            ExperimentConfiguration.async.hset("w"+tuple4AggregateWindow.getStart()+"|"+tuple4AggregateWindow.getAggValues().get(0).f1, "window_end",tuple4AggregateWindow.getEnd()+"");
            ExperimentConfiguration.async.hset("w"+tuple4AggregateWindow.getStart()+"|"+tuple4AggregateWindow.getAggValues().get(0).f1, "out_time",System.nanoTime()+"");
            collector.collect(new Tuple3<>(tuple4AggregateWindow.getAggValues().get(0).f0,tuple4AggregateWindow.getAggValues().get(0).f1,(long)tuple4AggregateWindow.getAggValues().get(0).f2));
        }
    }

}

