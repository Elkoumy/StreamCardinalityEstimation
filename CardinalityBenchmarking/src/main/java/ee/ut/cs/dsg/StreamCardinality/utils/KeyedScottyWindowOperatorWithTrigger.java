package ee.ut.cs.dsg.StreamCardinality.utils;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.slicing.WindowManager;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import ee.ut.cs.dsg.StreamCardinality.ExperimentConfiguration;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

//import de.tub.dima.scotty.slicing.SlicingWindowOperatorPublic;

public class KeyedScottyWindowOperatorWithTrigger<Key, InputType, FinalAggregateType> extends KeyedProcessFunction<Key, InputType, AggregateWindow<FinalAggregateType>> {
    private MemoryStateFactory stateFactory;
    private HashMap<Key, SlicingWindowOperatorPublic<InputType>> slicingWindowOperatorMap;
    private long lastWatermark;
    private final AggregateFunction<InputType, ?, FinalAggregateType> windowFunction;
    private final List<Window> windows;

    public KeyedScottyWindowOperatorWithTrigger(AggregateFunction<InputType, ?, FinalAggregateType> windowFunction) {
        this.windowFunction = windowFunction;
        this.windows = new ArrayList();
    }

    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.stateFactory = new MemoryStateFactory();
        this.slicingWindowOperatorMap = new HashMap();
    }

    public SlicingWindowOperatorPublic<InputType> initWindowOperator() {
        SlicingWindowOperatorPublic<InputType> slicingWindowOperator = new SlicingWindowOperatorPublic(this.stateFactory);
        Iterator var2 = this.windows.iterator();

        while(var2.hasNext()) {
            Window window = (Window)var2.next();
            slicingWindowOperator.addWindowAssigner(window);
        }

        slicingWindowOperator.addAggregation(this.windowFunction);
        return slicingWindowOperator;
    }

    public Key getKey(Context ctx) {
        return ctx.getCurrentKey();
    }

    public void processElement(InputType value, Context ctx, Collector<AggregateWindow<FinalAggregateType>> out) throws Exception {
        Key currentKey = this.getKey(ctx);
        if (!this.slicingWindowOperatorMap.containsKey(currentKey)) {
            this.slicingWindowOperatorMap.put(currentKey, this.initWindowOperator());
        }

        SlicingWindowOperatorPublic<InputType> slicingWindowOperator = (SlicingWindowOperatorPublic)this.slicingWindowOperatorMap.get(currentKey);
        slicingWindowOperator.processElement(value, ctx.timestamp());
        this.processWatermark(ctx, out);
    }

    private void processWatermark(Context ctx, Collector<AggregateWindow<FinalAggregateType>> out) {
        long currentWaterMark = ctx.timerService().currentWatermark();
        String kk=ctx.getCurrentKey().toString().toString();
        kk=kk.replace("(","");
        kk=kk.replace(")","");

        if (currentWaterMark > this.lastWatermark) {
            Iterator var5 = this.slicingWindowOperatorMap.values().iterator();
            WindowManager ttt = this.slicingWindowOperatorMap.get(ctx.getCurrentKey()).getWindowManager();
            while(var5.hasNext()) {
                SlicingWindowOperatorPublic<InputType> slicingWindowOperator = (SlicingWindowOperatorPublic)var5.next();
                List<AggregateWindow> aggregates = slicingWindowOperator.processWatermark(currentWaterMark);
                Iterator var8 = aggregates.iterator();

                while(var8.hasNext()) {
                    AggregateWindow<FinalAggregateType> aggregateWindow = (AggregateWindow)var8.next();
                            String key=ctx.getCurrentKey().toString().toString();
                            key=key.replace("(","");
                            key=key.replace(")","");
                           String s= ctx.getCurrentKey().toString();

//                    System.out.println("windowStart:"+ aggregateWindow.getStart()+"  end: "+aggregateWindow.getEnd());

                    if (aggregateWindow.hasValue()) {
                        Tuple4<Long, String, Double, Long> x = (Tuple4<Long, String, Double, Long>) aggregateWindow.getAggValues().get(0);

                        ExperimentConfiguration.async.hset("w"+ aggregateWindow.getStart()+"|"+x.f1, "query_start", Long.toString(System.nanoTime()));
                        ExperimentConfiguration.async.hset("w"+ aggregateWindow.getStart()+"|"+x.f1, "window_end_time", ""+aggregateWindow.getEnd());
                        out.collect(aggregateWindow);
                    }
                }
            }

            this.lastWatermark = currentWaterMark;
        }

    }

//    private void processWatermark(KeyedProcessFunction<Key, InputType, AggregateWindow<FinalAggregateType>>.Context ctx, Collector<AggregateWindow<FinalAggregateType>> out) {
//
////        String kk=ctx.getCurrentKey().toString().toString();
////        kk=kk.replace("(","");
////        kk=kk.replace(")","");
//
//        long currentWaterMark = ctx.timerService().currentWatermark();
//        if (currentWaterMark > this.lastWatermark) {
//            Iterator var5 = this.slicingWindowOperatorMap.values().iterator();
////            WindowManager ttt = this.slicingWindowOperatorMap.get(ctx.getCurrentKey()).getWindowManager();
//
////            kk=ctx.getCurrentKey().toString();
////
////            ExperimentConfiguration.async.hset("w_scotty|"+kk+"|"+this.lastWatermark, "query_start", Long.toString(System.nanoTime()));
////            ExperimentConfiguration.async.hset("w_scotty|"+kk+"|"+this.lastWatermark, "watermark_end_time", ""+currentWaterMark);
//            while(var5.hasNext()) {
////                kk=ctx.getCurrentKey().toString();
//                SlicingWindowOperatorPublic<InputType> slicingWindowOperator = (SlicingWindowOperatorPublic)var5.next();
//                List<AggregateWindow> aggregates = slicingWindowOperator.processWatermark(currentWaterMark);
//                Iterator var8 = aggregates.iterator();
//
//                while(var8.hasNext()) {
//                    AggregateWindow<FinalAggregateType> aggregateWindow = (AggregateWindow)var8.next();
//                    out.collect(aggregateWindow);
//                }
//            }
//
//            this.lastWatermark = currentWaterMark;
//        }
//
//    }

    public void addWindow(Window window) {
        this.windows.add(window);
    }
}

