package ee.ut.cs.dsg.StreamCardinality.ApproximateAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.HyperLogLog;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.HyperLogLogPlus;

public class HyperLogLogPlusAccumulator <ACC>{
    Long f0;
    String f1;
    HyperLogLogPlus acc = new HyperLogLogPlus(1);
}
//