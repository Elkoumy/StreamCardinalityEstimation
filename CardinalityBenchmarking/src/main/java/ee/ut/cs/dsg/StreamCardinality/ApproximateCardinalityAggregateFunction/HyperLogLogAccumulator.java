package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.HyperLogLog;

public class HyperLogLogAccumulator <ACC>{
    Long f0;
    String f1;

    HyperLogLog acc = new HyperLogLog(1);
}
