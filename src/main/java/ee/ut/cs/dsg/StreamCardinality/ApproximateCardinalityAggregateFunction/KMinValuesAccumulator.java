package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.KMinValues;
import org.streaminer.util.hash.MurmurHash;


public class KMinValuesAccumulator <ACC> {
    Long f0;
    String f1;

    KMinValues acc = new KMinValues(3, MurmurHash.getInstance());
}

