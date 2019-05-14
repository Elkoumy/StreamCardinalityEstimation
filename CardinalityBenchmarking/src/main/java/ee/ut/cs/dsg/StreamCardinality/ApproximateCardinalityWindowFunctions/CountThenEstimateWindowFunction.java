package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityWindowFunctions;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowFunction.CloneablePartialStateFunction;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.AdaptiveCounting;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.CardinalityMergeException;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.CountThenEstimate;
import ee.ut.cs.dsg.StreamCardinality.ExperimentConfiguration;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.io.IOException;


public class CountThenEstimateWindowFunction implements AggregateFunction<Tuple3<Long, String, Long>,
        Tuple3<Long, String, CountThenEstimate>,
        Tuple4<Long, String, Long,Long>>,
        CloneablePartialStateFunction<Tuple3<Long, String, CountThenEstimate>>
{
    public CountThenEstimateWindowFunction() {}

    @Override
    public Tuple3<Long, String, CountThenEstimate> lift(Tuple3<Long, String, Long> inputTuple) {
        CountThenEstimate mh = new CountThenEstimate(1000, AdaptiveCounting.Builder.obyCount(1000000000));
        mh.offer(Math.round(inputTuple.f2));
        return new Tuple3<>(inputTuple.f0, inputTuple.f1, mh);
    }

    @Override
    public Tuple4<Long, String, Long,Long> lower(Tuple3<Long, String, CountThenEstimate> aggregate) {
        if(ExperimentConfiguration.experimentType== ExperimentConfiguration.ExperimentType.Latency) {
            return new Tuple4<>(aggregate.f0, aggregate.f1,  aggregate.f2.cardinality(), System.nanoTime()); // In the last part, aggregate.f2.getK() <- THE getK() is probably WRONG!
        }else{
            return new Tuple4<>(aggregate.f0, aggregate.f1, aggregate.f2.cardinality(), null);
        }
    }

    @Override
    public Tuple3<Long, String, CountThenEstimate> combine(Tuple3<Long, String, CountThenEstimate> partialAggregate1, Tuple3<Long, String, CountThenEstimate> partialAggregate2) {
        try {
            return new Tuple3<>(partialAggregate1.f0, partialAggregate1.f1, (CountThenEstimate) partialAggregate1.f2.merge(partialAggregate2.f2));
        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Tuple3<Long, String, CountThenEstimate> liftAndCombine(Tuple3<Long, String, CountThenEstimate> partialAggregate, Tuple3<Long, String, Long> inputTuple) {
        if (ExperimentConfiguration.experimentType== ExperimentConfiguration.ExperimentType.Latency) {
            String curr = Long.toString(System.nanoTime());
            ExperimentConfiguration.async.hset(inputTuple.f0 + "|" + inputTuple.f1 + "|" + curr, "insertion_start", Long.toString(System.nanoTime()));
            partialAggregate.f2.offer(Math.round(inputTuple.f2));
            ExperimentConfiguration.async.hset(inputTuple.f0 + "|" + inputTuple.f1 + "|" + curr, "insertion_end", Long.toString(System.nanoTime()));
        }else{
            partialAggregate.f2.offer(Math.round(inputTuple.f2));
        }
        return partialAggregate;
    }

    // CountThenEstimate REQUIRES A CLONING FUNCTION!
    @Override
    public Tuple3<Long, String, CountThenEstimate> clone(Tuple3<Long, String, CountThenEstimate> partialAggregate) {
        //return new Tuple3<>(partialAggregate.f0, partialAggregate.f1,partialAggregate.f2.);
        return null;
    }
}
