package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.BJKST;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;


public class BJKSTAggregationFunction implements AggregateFunction<Tuple3<Long, String, Long>, BJKSTAccumulator, Tuple3<Long,String,Long>> {
    @Override
    public BJKSTAccumulator createAccumulator() { return new BJKSTAccumulator(); }

    @Override
    public BJKSTAccumulator merge(BJKSTAccumulator a, BJKSTAccumulator b) {
        try {
            a.acc = (BJKST) a.acc.merge(b.acc);
        } catch (BJKST.BJKSTException e) {
            e.printStackTrace();
        }

        return a;
    }

    @Override
    public BJKSTAccumulator add(Tuple3<Long, String, Long> value, BJKSTAccumulator acc) {
        acc.f0 = value.f0;
        acc.f1 = value.f1;
        long val = value.f2;
        acc.acc.offer(val);
        return acc;
    }

    @Override
    public Tuple3<Long, String, Long> getResult(BJKSTAccumulator acc) {
        Tuple3<Long,String,Long> res = new Tuple3<>();
        res.f0 = acc.f0;
        res.f1 = acc.f1;
        res.f2 =  acc.acc.cardinality();
        return res;
    }
}
