package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityWindowFunctions;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowFunction.CloneablePartialStateFunction;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.CardinalityMergeException;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.HyperLogLogPlus;
import org.apache.flink.api.java.tuple.Tuple3;

public class HyperLogLogPlusWindowFunction implements AggregateFunction<Tuple3<Long, String, Long>,
        Tuple3<Long, String, HyperLogLogPlus>,
        Tuple3<Long, String, Long>>,
        CloneablePartialStateFunction<Tuple3<Long, String, HyperLogLogPlus>>{

    HyperLogLogPlusWindowFunction() {}

    @Override
    public Tuple3<Long, String, HyperLogLogPlus> lift(Tuple3<Long, String, Long> inputTuple) {
        HyperLogLogPlus mh = new HyperLogLogPlus(0);
        mh.offer(Math.round(inputTuple.f2));
        return new Tuple3<>(inputTuple.f0, inputTuple.f1, mh);
    }

    @Override
    public Tuple3<Long, String, Long> lower(Tuple3<Long, String, HyperLogLogPlus> aggregate) {
        return new Tuple3<>(aggregate.f0, aggregate.f1, aggregate.f2.cardinality());
    }

    @Override
    public Tuple3<Long, String, HyperLogLogPlus> combine(Tuple3<Long, String, HyperLogLogPlus> partialAggregate1, Tuple3<Long, String, HyperLogLogPlus> partialAggregate2) {
        try {
            return new Tuple3<>(partialAggregate1.f0, partialAggregate1.f1, (HyperLogLogPlus) partialAggregate2.f2.merge(partialAggregate1.f2));
        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Tuple3<Long, String, HyperLogLogPlus> liftAndCombine(Tuple3<Long, String, HyperLogLogPlus> partialAggregate, Tuple3<Long, String, Long> inputTuple) {
        partialAggregate.f2.offer(Math.round(inputTuple.f2));
        return partialAggregate;
    }

    // NEEDS A CLONING FUNCTION
    @Override
    public Tuple3<Long, String, HyperLogLogPlus> clone(Tuple3<Long, String, HyperLogLogPlus> partialAggregate) {
        //return new Tuple3<>(partialAggregate.f0, partialAggregate.f1, partialAggregate.f2.clone());
        return null;
    }
}


/*

 */