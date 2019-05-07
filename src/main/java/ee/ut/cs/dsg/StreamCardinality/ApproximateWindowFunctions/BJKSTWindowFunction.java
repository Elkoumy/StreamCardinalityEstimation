package ee.ut.cs.dsg.StreamCardinality.ApproximateWindowFunctions;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowFunction.CloneablePartialStateFunction;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.BJKST;
import org.apache.flink.api.java.tuple.Tuple3;

public class BJKSTWindowFunction implements AggregateFunction<Tuple3<Long, String, Long>,
        Tuple3<Long, String, BJKST>,
        Tuple3<Long, String, Long>>,
        CloneablePartialStateFunction<Tuple3<Long, String, BJKST>> {

    public BJKSTWindowFunction() {}

    @Override
    public Tuple3<Long, String, BJKST> lift(Tuple3<Long, String, Long> inputTuple) {
        BJKST mh = new BJKST(100, 10, 0.3);
        mh.offer(Math.round(inputTuple.f2));
        return new Tuple3<>(inputTuple.f0, inputTuple.f1, mh);
    }

    @Override
    public Tuple3<Long, String, Long> lower(Tuple3<Long, String, BJKST> aggregate) {
        return new Tuple3<>(aggregate.f0, aggregate.f1, aggregate.f2.cardinality());
    }

    @Override
    public Tuple3<Long, String, BJKST> combine(Tuple3<Long, String, BJKST> partialAggregate1, Tuple3<Long, String, BJKST> partialAggregate2) {
        try {
            return new Tuple3<>(partialAggregate1.f0, partialAggregate1.f1, (BJKST) partialAggregate1.f2.merge(partialAggregate2.f2));
        } catch (BJKST.BJKSTException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Tuple3<Long, String, BJKST> liftAndCombine(Tuple3<Long, String, BJKST> partialAggregate, Tuple3<Long, String, Long> inputTuple) {
        partialAggregate.f2.offer(Math.round(inputTuple.f2));
        return partialAggregate;
    }

    @Override
    public Tuple3<Long, String, BJKST> clone(Tuple3<Long, String, BJKST> partialAggregate) {
        return new Tuple3<>(partialAggregate.f0, partialAggregate.f1, (BJKST) partialAggregate.f2.clone());
    }
}
