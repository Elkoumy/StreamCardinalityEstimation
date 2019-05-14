package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.BJKST;


public class BJKSTAccumulator <ACC>{
    Long f0;
    String f1;

    BJKST acc = new BJKST(100,10,0.3);
}

