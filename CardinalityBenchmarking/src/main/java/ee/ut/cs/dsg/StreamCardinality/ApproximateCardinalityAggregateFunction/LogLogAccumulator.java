package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.LogLog;

public class LogLogAccumulator <ACC>{
    Long f0;
    String f1;

    LogLog acc = new LogLog(2);
}
