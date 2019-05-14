package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.KMinValues;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;


public class KMinValuesAggregationFunction implements AggregateFunction<Tuple3<Long, String, Integer>, KMinValuesAccumulator, Tuple3<Long,String,Integer>>{
    @Override
    public KMinValuesAccumulator createAccumulator() { return new KMinValuesAccumulator(); }

    @Override
    public KMinValuesAccumulator merge(KMinValuesAccumulator a, KMinValuesAccumulator b) {
        try {
            a.acc = (KMinValues) a.acc.merge(b.acc);
        } catch (KMinValues.KMinValuesException e) {
            e.printStackTrace();
        }
        return a;
    }

    @Override
    public KMinValuesAccumulator add(Tuple3<Long, String, Integer> value, KMinValuesAccumulator acc) {
        acc.f0 = value.f0;
        acc.f1 = value.f1;
        long val = Math.round(value.f2);
        acc.acc.offer(val);

        return acc;
    }

    @Override
    public Tuple3<Long, String, Integer> getResult(KMinValuesAccumulator acc) {
        Tuple3<Long,String,Integer> res = new Tuple3<>();
        res.f0 = acc.f0;
        res.f1 = acc.f1;
        res.f2 = (int) acc.acc.cardinality();
        return res;
    }


}

