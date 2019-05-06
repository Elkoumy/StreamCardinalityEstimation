package ee.ut.cs.dsg.StreamCardinality.ApproximateWindowFunctions;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowFunction.CloneablePartialStateFunction;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.CardinalityMergeException;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.HyperLogLog;
import org.apache.flink.api.java.tuple.Tuple3;


public class HyperLogLogWindowFunction implements AggregateFunction<Tuple3<Long, String, Long>,
        Tuple3<Long, String, HyperLogLog>,
        Tuple3<Long, String, Long>>,
        CloneablePartialStateFunction<Tuple3<Long, String, HyperLogLog>>{

    public HyperLogLogWindowFunction() {}

    @Override
    public Tuple3<Long, String, HyperLogLog> lift(Tuple3<Long, String, Long> inputTuple) {
        HyperLogLog mh = new HyperLogLog(.6);
        mh.offer(Math.round(inputTuple.f2));
        return new Tuple3<>(inputTuple.f0, inputTuple.f1, mh);
    }

    @Override
    public Tuple3<Long, String, Long> lower(Tuple3<Long, String, HyperLogLog> aggregate) {
        return new Tuple3<>(aggregate.f0, aggregate.f1, aggregate.f2.cardinality());
    }

    @Override
    public Tuple3<Long, String, HyperLogLog> combine(Tuple3<Long, String, HyperLogLog> partialAggregate1, Tuple3<Long, String, HyperLogLog> partialAggregate2) {
        try {
            return new Tuple3<>(partialAggregate1.f0, partialAggregate1.f1, (HyperLogLog) partialAggregate2.f2.merge(partialAggregate1.f2));
        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Tuple3<Long, String, HyperLogLog> liftAndCombine(Tuple3<Long, String, HyperLogLog> partialAggregate, Tuple3<Long, String, Long> inputTuple) {
        partialAggregate.f2.offer(Math.round(inputTuple.f2));
        return partialAggregate;
    }

    @Override
    public Tuple3<Long, String, HyperLogLog> clone(Tuple3<Long, String, HyperLogLog> partialAggregate) {
        return new Tuple3<>(partialAggregate.f0, partialAggregate.f1, partialAggregate.f2.cloneHyperLogLogObject(partialAggregate.f2));
    }
}
