package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ExperimentConfiguration;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class LogLogAggregationFunction implements AggregateFunction<Tuple3<Long, String, Long>, LogLogAccumulator, Tuple4<Long, String, Long,Long>> {

    public LogLogAccumulator createAccumulator() { return new LogLogAccumulator();}

    public LogLogAccumulator merge(LogLogAccumulator a, LogLogAccumulator b) {
        a.acc = a.acc.mergeLogLogObjects(b.acc);
        return a;
    }

    public LogLogAccumulator add(Tuple3<Long, String, Long> value, LogLogAccumulator acc) {
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

    public Tuple4<Long, String, Long,Long> getResult(LogLogAccumulator acc) {
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
        }
        else  if(ExperimentConfiguration.experimentType== ExperimentConfiguration.ExperimentType.Throughput) {
            res.f3=(long)acc.acc.getCount();
        }
        else{
            res.f3=null;
        }

        return res;
    }

}
