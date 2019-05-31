package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityWindowFunctions;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowFunction.CloneablePartialStateFunction;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.BloomFilter;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.CardinalityMergeException;
import ee.ut.cs.dsg.StreamCardinality.ExperimentConfiguration;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.io.IOException;


public class BloomFilterWindowFunction implements AggregateFunction<Tuple3<Long, String, Long>,
        Tuple3<Long, String, BloomFilter>,
        Tuple4<Long, String, Long, Long>>,
        CloneablePartialStateFunction<Tuple3<Long, String, BloomFilter>>{

    @Override
    public Tuple3<Long, String, BloomFilter> lift(Tuple3<Long, String, Long> inputTuple) {
        BloomFilter mh = new BloomFilter(8,8);
        mh.offer(Math.round(inputTuple.f2));
        return new Tuple3<>(inputTuple.f0, inputTuple.f1, mh);
    }

    @Override
    public Tuple4<Long, String, Long, Long> lower(Tuple3<Long, String, BloomFilter> aggregate) {
        if (ExperimentConfiguration.experimentType == ExperimentConfiguration.ExperimentType.Latency) {
            return new Tuple4<>(aggregate.f0, aggregate.f1, aggregate.f2.cardinality(), System.nanoTime());
        }
        else if (ExperimentConfiguration.experimentType== ExperimentConfiguration.ExperimentType.Throughput){
            return new Tuple4<>(aggregate.f0, aggregate.f1,  aggregate.f2.cardinality(), (long) aggregate.f2.getCount());
        }
        else {
            return new Tuple4<>(aggregate.f0, aggregate.f1, aggregate.f2.cardinality(), null);
        }
    }

    @Override
    public Tuple3<Long, String, BloomFilter> combine(Tuple3<Long, String, BloomFilter> partialAggregate1, Tuple3<Long, String, BloomFilter> partialAggregate2) {
        try {
            return new Tuple3<>(partialAggregate1.f0, partialAggregate1.f1, (BloomFilter) partialAggregate2.f2.merge(partialAggregate2.f2));
        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Tuple3<Long, String, BloomFilter> liftAndCombine(Tuple3<Long, String, BloomFilter> partialAggregate, Tuple3<Long, String, Long> inputTuple) {
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

    @Override
    public Tuple3<Long, String, BloomFilter> clone(Tuple3<Long, String, BloomFilter> partialAggregate) {
        return new Tuple3<>(partialAggregate.f0, partialAggregate.f1, (BloomFilter) partialAggregate.f2.clone());
    }
}


/*


 */