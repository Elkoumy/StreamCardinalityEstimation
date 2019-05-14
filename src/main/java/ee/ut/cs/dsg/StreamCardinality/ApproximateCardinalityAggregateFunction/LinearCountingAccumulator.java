package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.LinearCounting;

public class LinearCountingAccumulator {
    Long f0;
    String f1;

    LinearCounting acc = new LinearCounting(4);
}
