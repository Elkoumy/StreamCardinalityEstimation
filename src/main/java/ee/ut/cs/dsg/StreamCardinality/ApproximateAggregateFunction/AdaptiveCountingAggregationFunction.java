package ee.ut.cs.dsg.StreamCardinality.ApproximateAggregateFunction;


import ee.ut.cs.dsg.StreamCardinality.ApproximateThroughput.ApproximateThroughputCounter;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.UUID;

public class AdaptiveCountingAggregationFunction implements AggregateFunction<Tuple3<Long, String, Integer>, AdaptiveCountingAccumulator, Tuple3<Long,String,Integer>>{

    private Integer counter=0;

    public AdaptiveCountingAccumulator createAccumulator() { return new AdaptiveCountingAccumulator(); }

    public AdaptiveCountingAccumulator merge(AdaptiveCountingAccumulator a, AdaptiveCountingAccumulator b) {
        a.acc = a.acc.mergeAdaptiveCountingObjects(b.acc);
        return a;
    }

    public AdaptiveCountingAccumulator add(Tuple3<Long, String, Integer> value, AdaptiveCountingAccumulator acc) {
        this.counter++;

        acc.f0 = value.f0;
        acc.f1 = value.f1;
        long val = Math.round(value.f2);
        acc.acc.offer(val);

        return acc;
    }

    public Tuple3<Long, String, Integer> getResult(AdaptiveCountingAccumulator acc) {

        ApproximateThroughputCounter myAT = ApproximateThroughputCounter.getInstance();
        Tuple2<String, Integer> tmp = new Tuple2<>("windowAt"+ UUID.randomUUID(), this.counter);
        myAT.push(tmp);

        Tuple3<Long,String,Integer> res= new Tuple3<>();
        res.f0 = acc.f0;
        res.f1 = acc.f1;
        try{
            res.f2 = (int) acc.acc.cardinality();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return res;
    }
}
