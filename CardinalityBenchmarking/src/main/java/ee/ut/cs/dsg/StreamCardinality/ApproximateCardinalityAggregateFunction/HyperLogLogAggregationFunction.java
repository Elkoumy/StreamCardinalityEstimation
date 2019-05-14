package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.CardinalityMergeException;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.HyperLogLog;
import ee.ut.cs.dsg.StreamCardinality.ExperimentConfiguration;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class HyperLogLogAggregationFunction implements AggregateFunction<Tuple3<Long, String, Long>, HyperLogLogAccumulator, Tuple4<Long,String,Long,Long>> {
    public HyperLogLogAccumulator createAccumulator() { return new HyperLogLogAccumulator(); }

    public HyperLogLogAccumulator merge(HyperLogLogAccumulator a, HyperLogLogAccumulator b) {
        try {
            a.acc = (HyperLogLog) a.acc.merge(b.acc);
        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        }
        return a;
    }

    public HyperLogLogAccumulator add(Tuple3<Long, String, Long> value, HyperLogLogAccumulator acc) {
        if(ExperimentConfiguration.experimentType== ExperimentConfiguration.ExperimentType.Latency) {
            String curr = Long.toString(System.nanoTime());
            ExperimentConfiguration.async.hset(value.f0+"|"+value.f1+"|"+curr, "insertion_start", Long.toString(System.nanoTime()));
            acc.f0 = value.f0;
            acc.f1 = value.f1;
            acc.acc.offer(value.f2);
            ExperimentConfiguration.async.hset(value.f0+"|"+value.f1+"|"+curr, "insertion_end", Long.toString(System.nanoTime()));
        }else{
            acc.f0 = value.f0;
            acc.f1 = value.f1;
            acc.acc.offer(value.f2);
        }

        return acc;
    }

    public Tuple4<Long, String, Long,Long> getResult(HyperLogLogAccumulator acc) {
        Tuple4<Long,String,Long,Long> res= new Tuple4<>();
        res.f0 = acc.f0;
        res.f1 = acc.f1;
        try{
            res.f2 =  acc.acc.cardinality();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(ExperimentConfiguration.experimentType== ExperimentConfiguration.ExperimentType.Latency) {
            res.f3=System.nanoTime();
        }else{
            res.f3=null;
        }
        return res;
    }
}
