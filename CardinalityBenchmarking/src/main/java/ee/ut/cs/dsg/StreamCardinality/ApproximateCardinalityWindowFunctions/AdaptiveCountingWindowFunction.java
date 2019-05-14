package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityWindowFunctions;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowFunction.CloneablePartialStateFunction;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.AdaptiveCounting;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.LogLog;
import ee.ut.cs.dsg.StreamCardinality.ExperimentConfiguration;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class AdaptiveCountingWindowFunction implements AggregateFunction<Tuple3<Long, String, Long>,
        Tuple3<Long, String, AdaptiveCounting>,
        Tuple4<Long, String, Long,Long>>,
        CloneablePartialStateFunction<Tuple3<Long, String, AdaptiveCounting>>
{

    public AdaptiveCountingWindowFunction() {}

    @Override
    public Tuple3<Long, String, AdaptiveCounting> lift(Tuple3<Long, String, Long> inputTuple) {
        AdaptiveCounting mh = new AdaptiveCounting(1);
        mh.offer(Math.round(inputTuple.f2));

        return new Tuple3<>(inputTuple.f0, inputTuple.f1 , mh);
    }

    @Override
    public Tuple4<Long, String, Long,Long> lower(Tuple3<Long, String, AdaptiveCounting> aggregate) {
        try {
            if(ExperimentConfiguration.experimentType== ExperimentConfiguration.ExperimentType.Latency) {
                return new Tuple4<>(aggregate.f0, aggregate.f1,  aggregate.f2.cardinality(), System.nanoTime());  // In the last part, aggregate.f2.getB_e() <- THE getB_e() is probably WRONG!
            }else{
                return new Tuple4<>(aggregate.f0, aggregate.f1, aggregate.f2.cardinality(), null);  // In the last part, aggregate.f2.getB_e() <- THE getB_e() is probably WRONG!
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Tuple3<Long, String, AdaptiveCounting> combine(Tuple3<Long, String, AdaptiveCounting> partialAggregate1, Tuple3<Long, String, AdaptiveCounting> partialAggregate2) {

        try {
            return new Tuple3<>(partialAggregate1.f0, partialAggregate1.f1, (AdaptiveCounting) partialAggregate2.f2.merge(partialAggregate2.f2));
        } catch (LogLog.LogLogMergeException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Tuple3<Long, String, AdaptiveCounting> liftAndCombine(Tuple3<Long, String, AdaptiveCounting> partialAggregate, Tuple3<Long, String, Long> inputTuple) {
        partialAggregate.f2.offer(Math.round(inputTuple.f2));
        return partialAggregate;
    }

    @Override
    public Tuple3<Long, String, AdaptiveCounting> clone(Tuple3<Long, String, AdaptiveCounting> partialAggregate) {
        return new Tuple3<>(partialAggregate.f0, partialAggregate.f1, (AdaptiveCounting) partialAggregate.f2.cloneLogLogObjects(partialAggregate.f2));
    }


}

