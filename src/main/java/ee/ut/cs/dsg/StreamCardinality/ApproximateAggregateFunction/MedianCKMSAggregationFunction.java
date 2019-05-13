package ee.ut.cs.dsg.StreamCardinality.ApproximateAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateThroughput.ApproximateThroughputCounter;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.UUID;

public class MedianCKMSAggregationFunction implements AggregateFunction<Tuple3<Long, String, Double>, MedianCKMSAccumulator, Tuple3<Long,String,Double>> {

    private Integer counter=0;

    public MedianCKMSAccumulator createAccumulator() {
        return new MedianCKMSAccumulator ();
    }

    public MedianCKMSAccumulator  merge(MedianCKMSAccumulator  a, MedianCKMSAccumulator  b) {
        a.acc = a.acc.merge(b.acc);
        return a;
    }

    public MedianCKMSAccumulator  add(Tuple3<Long, String, Double> value, MedianCKMSAccumulator  acc) {
        this.counter++;

        acc.f0 = value.f0;
        acc.f1 = value.f1;
        long val = Math.round(value.f2);
        acc.acc.offer(val);

        return acc;
    }

    public Tuple3<Long,String,Double> getResult(MedianCKMSAccumulator  acc) {
        ApproximateThroughputCounter myAT = ApproximateThroughputCounter.getInstance();
        Tuple2<String, Integer> tmp = new Tuple2<>("windowAt"+ UUID.randomUUID(), this.counter);
        myAT.push(tmp);

        Tuple3<Long,String,Double> res= new Tuple3<>();
        res.f0 = acc.f0;
        res.f1 = acc.f1;
        try {
            res.f2 = (double) acc.acc.getQuantile(0.5);
        } catch (Exception e) {
            e.printStackTrace();
        }


        return res;
    }

}
