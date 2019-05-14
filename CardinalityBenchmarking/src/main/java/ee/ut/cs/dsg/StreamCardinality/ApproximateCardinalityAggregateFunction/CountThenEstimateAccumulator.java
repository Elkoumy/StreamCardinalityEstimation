package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.AdaptiveCounting;
import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.CountThenEstimate;

public class CountThenEstimateAccumulator<ACC> {
    Long f0;
    String f1;
    CountThenEstimate acc = new CountThenEstimate(1000, AdaptiveCounting.Builder.obyCount(1000000000));
}
