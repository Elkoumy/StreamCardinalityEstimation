package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityWindowFunctions;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowFunction.CloneablePartialStateFunction;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.LogLog;
import ee.ut.cs.dsg.StreamCardinality.ExperimentConfiguration;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import static ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.LogLog.cloneLogLogObjects;

public class LogLogWindowFunction implements AggregateFunction<Tuple3<Long, String, Long>,
        Tuple3<Long, String, LogLog>,
        Tuple4<Long, String, Long,Long>>,
        CloneablePartialStateFunction<Tuple3<Long, String, LogLog>>
{
    public LogLogWindowFunction(){}

    @Override
    public Tuple3<Long, String, LogLog> lift(Tuple3<Long, String, Long> inputTuple) {
        LogLog mh = new LogLog(1);
        mh.offer(Math.round(inputTuple.f2));

        return new Tuple3<>(inputTuple.f0, inputTuple.f1, mh);
    }

    @Override
    public Tuple4<Long, String, Long,Long> lower(Tuple3<Long, String, LogLog> aggregate) {
        try {
            if(ExperimentConfiguration.experimentType== ExperimentConfiguration.ExperimentType.Latency) {
                return new Tuple4<>(aggregate.f0, aggregate.f1,  aggregate.f2.cardinality(), System.nanoTime()); // In the last part, aggregate.f2.getK() <- THE getK() is probably WRONG!
            }
            else if (ExperimentConfiguration.experimentType== ExperimentConfiguration.ExperimentType.Throughput){
                return new Tuple4<>(aggregate.f0, aggregate.f1,  aggregate.f2.cardinality(), (long) aggregate.f2.getCount());
            }
            else{
                return new Tuple4<>(aggregate.f0, aggregate.f1, aggregate.f2.cardinality(), null);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Tuple3<Long, String, LogLog> combine(Tuple3<Long, String, LogLog> partialAggregate1, Tuple3<Long, String, LogLog> partialAggregate2) {
        return new Tuple3<>(partialAggregate1.f0, partialAggregate1.f1, partialAggregate1.f2.mergeLogLogObjects(partialAggregate2.f2));  // Should the last bit use merge (IRichCardinality) or mergeLogLogObjects?
    }

    @Override
    public Tuple3<Long, String, LogLog> liftAndCombine(Tuple3<Long, String, LogLog> partialAggregate, Tuple3<Long, String, Long> inputTuple) {
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
    public Tuple3<Long, String, LogLog> clone(Tuple3<Long, String, LogLog> partialAggregate) {
        return new Tuple3(partialAggregate.f0,partialAggregate.f1, cloneLogLogObjects(partialAggregate.f2)); // Should it be cloneLogLogObjects(partialAggregate.f2) or partialAggregate.f2.cloneLogLogObjects(partialAggregate.f2)
    }

}
