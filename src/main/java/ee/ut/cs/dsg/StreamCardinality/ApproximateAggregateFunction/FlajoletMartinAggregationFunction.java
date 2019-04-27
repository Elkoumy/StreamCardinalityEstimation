package ee.ut.cs.dsg.StreamCardinality.ApproximateAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.FlajoletMartin;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class FlajoletMartinAggregationFunction implements AggregateFunction<Tuple3<Long, String, Integer>, FlajoletMartinAccumulator, Tuple3<Long,String,Integer>> {

    public FlajoletMartinAccumulator createAccumulator() { return new FlajoletMartinAccumulator(); }

    public FlajoletMartinAccumulator merge(FlajoletMartinAccumulator a, FlajoletMartinAccumulator b) {
        try {
            a.acc = (FlajoletMartin) a.acc.merge(b.acc);
        } catch (FlajoletMartin.FMException e) {
            e.printStackTrace();
        }
        return a;
    }

    @Override
    public FlajoletMartinAccumulator add(Tuple3<Long, String, Integer> value, FlajoletMartinAccumulator acc) {
        acc.f0 = value.f0;
        acc.f1 = value.f1;
        long val = Math.round(value.f2);
        acc.acc.offer(val);

        return acc;
    }

    @Override
    public Tuple3<Long, String, Integer> getResult(FlajoletMartinAccumulator acc) {
        Tuple3<Long,String,Integer> res = new Tuple3<>();
        res.f0 = acc.f0;
        res.f1 = acc.f1;

        res.f2 = (int) acc.acc.cardinality();

        return res;
    }
}
