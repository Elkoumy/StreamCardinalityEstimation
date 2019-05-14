package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinalityAggregateFunction;

import ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality.BloomFilter;

public class BloomFilterAccumulator {
    Long f0;
    String f1;

    BloomFilter acc = new BloomFilter(8,8);
}
