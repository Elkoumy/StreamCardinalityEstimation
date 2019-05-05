package ee.ut.cs.dsg.StreamCardinality.ApproximateAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateQuantiles.CKMSQuantiles;
public class MedianCKMSAccumulator <ACC>{
    Long f0;
    String f1;

   CKMSQuantiles acc = new CKMSQuantiles(new double[]{0.5}, 0.050);
}
