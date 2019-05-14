package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityAggregateFunction;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class AdaptiveCountingAggregationFunction implements AggregateFunction<Tuple3<Long, String, Long>, AdaptiveCountingAccumulator, Tuple3<Long,String,Long>>{
    public AdaptiveCountingAccumulator createAccumulator() { return new AdaptiveCountingAccumulator(); }

    public AdaptiveCountingAccumulator merge(AdaptiveCountingAccumulator a, AdaptiveCountingAccumulator b) {
        a.acc = a.acc.mergeAdaptiveCountingObjects(b.acc);
        return a;
    }

    public AdaptiveCountingAccumulator add(Tuple3<Long, String, Long> value, AdaptiveCountingAccumulator acc) {
        acc.f0 = value.f0;
        acc.f1 = value.f1;
        long val = Math.round(value.f2);
        acc.acc.offer(val);

        return acc;
    }

    public Tuple3<Long, String, Long> getResult(AdaptiveCountingAccumulator acc) {
        Tuple3<Long,String,Long> res= new Tuple3<>();
        res.f0 = acc.f0;
        res.f1 = acc.f1;
        try{
            res.f2 = acc.acc.cardinality();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return res;
    }
}
