package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.BloomFilter;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.CardinalityMergeException;
import ee.ut.cs.dsg.StreamCardinality.ExperimentConfiguration;
import org.apache.commons.math3.analysis.function.Exp;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.io.IOException;

public class BloomFilterAggregationFunction implements AggregateFunction<Tuple3<Long, String, Long>, BloomFilterAccumulator, Tuple4<Long, String, Long, Long>> {

    @Override
    public BloomFilterAccumulator createAccumulator() { return new BloomFilterAccumulator(); }

    @Override
    public BloomFilterAccumulator add(Tuple3<Long, String, Long> value, BloomFilterAccumulator acc) {
        if (ExperimentConfiguration.experimentType == ExperimentConfiguration.ExperimentType.Latency) {
            String curr = Long.toString(System.nanoTime());
            ExperimentConfiguration.async.hset(value.f0+"|"+value.f1+"|"+curr, "insertion_start", Long.toString(System.nanoTime()));
            acc.f0 = value.f0;
            acc.f1 = value.f1;
            acc.acc.offer(value.f2);
            ExperimentConfiguration.async.hset(value.f0+"|"+value.f1+"|"+curr, "insertion_end", Long.toString(System.nanoTime()));
        } else {
            acc.f0 = value.f0;
            acc.f1 = value.f1;
            acc.acc.offer(value.f2);
        }
        return acc;
    }

    @Override
    public Tuple4<Long, String, Long, Long> getResult(BloomFilterAccumulator acc) {
        Tuple4<Long, String, Long, Long> res = new Tuple4<>();
        res.f0 = acc.f0;
        res.f1 = acc.f1;

        res.f2 = acc.acc.cardinality();
        if (ExperimentConfiguration.experimentType == ExperimentConfiguration.ExperimentType.Latency){
            res.f3 = System.nanoTime();
        } else {
            res.f0 = null;
        }
        return res;
    }

    @Override
    public BloomFilterAccumulator merge(BloomFilterAccumulator a, BloomFilterAccumulator b) {
        try {
            a.acc = (BloomFilter) a.acc.merge(b.acc);
        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return a;
    }
}


/*




 */