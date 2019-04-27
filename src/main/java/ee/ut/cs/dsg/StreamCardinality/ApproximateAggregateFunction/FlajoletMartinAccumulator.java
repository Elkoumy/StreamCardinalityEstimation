package ee.ut.cs.dsg.StreamCardinality.ApproximateAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.FlajoletMartin;

public class FlajoletMartinAccumulator<ACC> {
    Long f0;
    String f1;

    FlajoletMartin acc = new FlajoletMartin(10,3,3);
}


