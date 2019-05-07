package ee.ut.cs.dsg.StreamCardinality.ApproximateAggregateFunction;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class LinearCountingAggregationFunction implements AggregateFunction<Tuple3<Long, String, Long>, LinearCountingAccumulator, Tuple3<Long, String, Long>> {

    public LinearCountingAccumulator createAccumulator() { return new LinearCountingAccumulator(); }

    public LinearCountingAccumulator merge(LinearCountingAccumulator a, LinearCountingAccumulator b) {
        a.acc = a.acc.mergeLinearCountingObjects(b.acc);

        return a;
    }

    public LinearCountingAccumulator add(Tuple3<Long, String, Long> value, LinearCountingAccumulator acc) {
        acc.f0 = value.f0;
        acc.f1 = value.f1;
        long val = Math.round(value.f2);
        acc.acc.offer(val);

        return acc;
    }


    public Tuple3<Long, String, Long> getResult(LinearCountingAccumulator acc) {
        Tuple3<Long,String,Long> res= new Tuple3<>();
        res.f0 = acc.f0;
        res.f1 = acc.f1;
        try{
            // Should it be  byte[] getMap?
            res.f2 =acc.acc.cardinality();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return res;
    }

}
