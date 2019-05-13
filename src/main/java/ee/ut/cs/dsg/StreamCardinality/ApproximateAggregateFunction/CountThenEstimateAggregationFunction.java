package ee.ut.cs.dsg.StreamCardinality.ApproximateAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.CardinalityMergeException;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.CountThenEstimate;
import ee.ut.cs.dsg.StreamCardinality.ApproximateThroughput.ApproximateThroughputCounter;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.IOException;
import java.util.UUID;

public class CountThenEstimateAggregationFunction implements AggregateFunction<Tuple3<Long, String, Integer>, CountThenEstimateAccumulator, Tuple3<Long,String,Integer>> {

    private Integer counter=0;

    @Override
    public CountThenEstimateAccumulator createAccumulator() { return new CountThenEstimateAccumulator(); }

    @Override
    public CountThenEstimateAccumulator merge(CountThenEstimateAccumulator a, CountThenEstimateAccumulator b) {
        try {
            a.acc = (CountThenEstimate) a.acc.merge(b.acc);
        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return a;
    }

    @Override
    public CountThenEstimateAccumulator add(Tuple3<Long, String, Integer> value, CountThenEstimateAccumulator acc) {
        this.counter++;

        acc.f0 = value.f0;
        acc.f1 = value.f1;
        long val = Math.round(value.f2);
        acc.acc.offer(val);
        return acc;
    }

    @Override
    public Tuple3<Long, String, Integer> getResult(CountThenEstimateAccumulator acc) {

        ApproximateThroughputCounter myAT = ApproximateThroughputCounter.getInstance();
        Tuple2<String, Integer> tmp = new Tuple2<>("windowAt"+ UUID.randomUUID(), this.counter);
        myAT.push(tmp);

        Tuple3<Long,String,Integer> res = new Tuple3<>();
        res.f0 = acc.f0;
        res.f1 = acc.f1;
        res.f2 = (int) acc.acc.cardinality();
        return res;
    }
}
