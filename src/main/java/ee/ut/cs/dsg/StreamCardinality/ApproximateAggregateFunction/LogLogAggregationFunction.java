package ee.ut.cs.dsg.StreamCardinality.ApproximateAggregateFunction;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import ee.ut.cs.dsg.StreamCardinality.ApproximateThroughput.ApproximateThroughputCounter;
import ee.ut.cs.dsg.StreamCardinality.ApproximateThroughput.StreamCounter;

import java.util.UUID;

public class LogLogAggregationFunction implements AggregateFunction<Tuple3<Long, String, Integer>, LogLogAccumulator, Tuple3<Long, String, Integer>> {

    private Integer counter=0;

    public LogLogAccumulator createAccumulator() { return new LogLogAccumulator();}

    public LogLogAccumulator merge(LogLogAccumulator a, LogLogAccumulator b) {
        a.acc = a.acc.mergeLogLogObjects(b.acc);
        return a;
    }

    public LogLogAccumulator add(Tuple3<Long, String, Integer> value, LogLogAccumulator acc) {
        this.counter++;
        acc.f0 = value.f0;
        acc.f1 = value.f1;
        long val = Math.round(value.f2);
        acc.acc.offer(val);

        return acc;
    }

    public Tuple3<Long, String, Integer> getResult(LogLogAccumulator acc) {

        ApproximateThroughputCounter myAT = ApproximateThroughputCounter.getInstance();
        Tuple2<String, Integer> tmp = new Tuple2<>("windowAt"+ UUID.randomUUID(), this.counter);
        myAT.push(tmp);

        Tuple3<Long,String,Integer> res= new Tuple3<>();
        res.f0 = acc.f0;
        res.f1 = acc.f1;
        try{
            res.f2 = (int) acc.acc.cardinality();
        } catch (Exception e) {
            e.printStackTrace();
        }


        return res;
    }

}
