package ee.ut.cs.dsg.StreamCardinality.ApproximateAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.CardinalityMergeException;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.HyperLogLog;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import scala.collection.generic.BitOperations;

public class HyperLogLogAggregationFunction implements AggregateFunction<Tuple3<Long, String, Integer>, HyperLogLogAccumulator, Tuple3<Long,String,Integer>> {
    public HyperLogLogAccumulator createAccumulator() { return new HyperLogLogAccumulator(); }

    public HyperLogLogAccumulator merge(HyperLogLogAccumulator a, HyperLogLogAccumulator b) {
        try {
            a.acc = (HyperLogLog) a.acc.merge(b.acc);
        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        }
        return a;
    }

    public HyperLogLogAccumulator add(Tuple3<Long, String, Integer> value, HyperLogLogAccumulator acc) {
        acc.f0 = value.f0;
        acc.f1 = value.f1;
        long val = Math.round(value.f2);
        acc.acc.offer(val);

        return acc;
    }

    public Tuple3<Long, String, Integer> getResult(HyperLogLogAccumulator acc) {
        Tuple3<Long,String,Integer> res= new Tuple3<>();
        res.f0 = acc.f0;
        res.f1 = acc.f1;
        res.f2 = (int) acc.acc.cardinality();

        return res;
    }
}
