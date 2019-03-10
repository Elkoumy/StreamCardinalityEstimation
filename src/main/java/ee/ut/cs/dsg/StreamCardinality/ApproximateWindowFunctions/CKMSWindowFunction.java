package ee.ut.cs.dsg.StreamCardinality.ApproximateWindowFunctions;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowFunction.CloneablePartialStateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateQuantiles.CKMSQuantiles;
import ee.ut.cs.dsg.StreamCardinality.ApproximateQuantiles.QuantilesException;
import org.apache.flink.api.java.tuple.Tuple3;

public class CKMSWindowFunction implements AggregateFunction<Tuple3<Long, String, Double>,
        Tuple3<Long, String, CKMSQuantiles>,
        Tuple3<Long, String, Double>>,
        CloneablePartialStateFunction<Tuple3<Long, String, CKMSQuantiles>>
{

//    private final double quantile;

    public CKMSWindowFunction() {

    }


    @Override
    public Tuple3<Long, String, CKMSQuantiles> lift(Tuple3<Long, String, Double> inputTuple) {
        CKMSQuantiles mh=new CKMSQuantiles(new double[]{0.5}, Math.round(inputTuple.f2));
        mh.offer(Math.round(inputTuple.f2));

        return new Tuple3<>(inputTuple.f0, inputTuple.f1 , mh);
    }

    @Override
    public Tuple3<Long, String, Double> lower(Tuple3<Long,String, CKMSQuantiles> aggregate) {
        try {
            return new Tuple3<>(aggregate.f0, aggregate.f1, (double) aggregate.f2.getQuantile(0.5));
        } catch (QuantilesException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Tuple3<Long,String, CKMSQuantiles> combine(Tuple3<Long,String, CKMSQuantiles> partialAggregate1, Tuple3<Long,String, CKMSQuantiles> partialAggregate2) {
        return new Tuple3<>(partialAggregate1.f0, partialAggregate1.f1 , partialAggregate1.f2.merge(partialAggregate2.f2) );
    }

    @Override
    public Tuple3<Long,String, CKMSQuantiles> liftAndCombine(Tuple3<Long,String, CKMSQuantiles> partialAggregate, Tuple3<Long, String, Double> inputTuple) {
        partialAggregate.f2.offer(Math.round(inputTuple.f2));
        return partialAggregate;
    }
    @Override
    public Tuple3<Long, String, CKMSQuantiles> clone(Tuple3<Long, String, CKMSQuantiles> partialAggregate) {
        return new Tuple3(partialAggregate.f0,partialAggregate.f1, partialAggregate.f2.clone());

    }
}
