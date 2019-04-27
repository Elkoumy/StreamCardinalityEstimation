package ee.ut.cs.dsg.StreamCardinality.ApproximateAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.CardinalityMergeException;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.HyperLogLogPlus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class HyperLogLogPlusAggregationFunction implements AggregateFunction<Tuple3<Long, String, Integer>, HyperLogLogPlusAccumulator, Tuple3<Long,String,Integer>> {
    @Override
    public HyperLogLogPlusAccumulator createAccumulator() { return new HyperLogLogPlusAccumulator(); }

    @Override
    public HyperLogLogPlusAccumulator merge(HyperLogLogPlusAccumulator a, HyperLogLogPlusAccumulator b) {
        try {
            a.acc = (HyperLogLogPlus) a.acc.merge(b.acc);
        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        }
        return a;
    }

    @Override
    public HyperLogLogPlusAccumulator add(Tuple3<Long, String, Integer> value, HyperLogLogPlusAccumulator acc) {
        acc.f0 = value.f0;
        acc.f1 = value.f1;
        long val = Math.round(value.f2);
        acc.acc.offer(val);
        return acc;
    }

    @Override
    public Tuple3<Long, String, Integer> getResult(HyperLogLogPlusAccumulator acc) {
        Tuple3<Long,String,Integer> res = new Tuple3<>();
        res.f0 = acc.f0;
        res.f1 = acc.f1;
        res.f2 = (int) acc.acc.cardinality();
        return res;
    }
}
