package ee.ut.cs.dsg.StreamCardinality.ApproximateAggregateFunction;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class LogLogAggregationFunction implements AggregateFunction<Tuple3<Long, String, Long>, LogLogAccumulator, Tuple3<Long, String, Long>> {

    public LogLogAccumulator createAccumulator() { return new LogLogAccumulator();}

    public LogLogAccumulator merge(LogLogAccumulator a, LogLogAccumulator b) {
        a.acc = a.acc.mergeLogLogObjects(b.acc);
        return a;
    }

    public LogLogAccumulator add(Tuple3<Long, String, Long> value, LogLogAccumulator acc) {
        acc.f0 = value.f0;
        acc.f1 = value.f1;
        long val = Math.round(value.f2);
        acc.acc.offer(val);

        return acc;
    }

    public Tuple3<Long, String, Long> getResult(LogLogAccumulator acc) {
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
