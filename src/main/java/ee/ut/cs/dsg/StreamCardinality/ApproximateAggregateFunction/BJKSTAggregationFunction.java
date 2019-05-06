package ee.ut.cs.dsg.StreamCardinality.ApproximateAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.BJKST;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import java.util.UUID;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Counter;
import ee.ut.cs.dsg.StreamCardinality.ApproximateThroughput.ApproximateThroughputCounter;
import ee.ut.cs.dsg.StreamCardinality.ApproximateThroughput.StreamCounter;


public class BJKSTAggregationFunction implements AggregateFunction<Tuple3<Long, String, Integer>, BJKSTAccumulator, Tuple3<Long,String,Integer>> {

    //private Gauge<Integer> gauge;
    private Integer counter=0;

    @Override
    public BJKSTAccumulator createAccumulator() {
        StreamCounter scnt = StreamCounter.getInstance();
        scnt.pushTo(1);
        return new BJKSTAccumulator();
    }

    @Override
    public BJKSTAccumulator merge(BJKSTAccumulator a, BJKSTAccumulator b) {
        StreamCounter scnt = StreamCounter.getInstance();
        scnt.push();
        try {
            a.acc = (BJKST) a.acc.merge(b.acc);
        } catch (BJKST.BJKSTException e) {
            e.printStackTrace();
        }

        return a;
    }

    @Override
    public BJKSTAccumulator add(Tuple3<Long, String, Integer> value, BJKSTAccumulator acc) {
        this.counter++;

        acc.f0 = value.f0;
        acc.f1 = value.f1;
        long val = Math.round(value.f2);
        acc.acc.offer(val);
        return acc;

    }

    @Override
    public Tuple3<Long, String, Integer> getResult(BJKSTAccumulator acc) {
        /*
        getRuntimeContext()
                .getMetricGroup()
                .gauge("WindowAt"+System.nanoTime(), new Gauge<Integer>()
                        {
                            @Override
                            public Integer getValue() {
                                return counter;
                            }
                        }
                );
        */

        ApproximateThroughputCounter myAT = ApproximateThroughputCounter.getInstance();
        Tuple2<String, Integer> tmp = new Tuple2<>("windowAt"+UUID.randomUUID(), this.counter);
        myAT.push(tmp);

        Tuple3<Long,String,Integer> res = new Tuple3<>();
        res.f0 = acc.f0;
        res.f1 = acc.f1;
        res.f2 = (int) acc.acc.cardinality();
        return res;
    }
}
