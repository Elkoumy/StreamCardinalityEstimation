package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality;


/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 *@author Hazar.Harmouch
 *
 *Updated to provide cardinality estimation
 *Referances:
 *S. J. Swamidass and P. Baldi. Mathematical correction for ngerprint similarity measures to improve chemical retrieval. Journal of chemical information and modeling, 47(3):952-964, 2007
 *O. Papapetrou, W. Siberski, and W. Nejdl. Cardinality estimation and dynamic length adaptation for Bloom filters. Distributed and Parallel Databases, 28(2):119{156, 2010
 *
 */


import java.io.IOException;
import java.io.UnsupportedEncodingException;
        import java.util.BitSet;

public class BloomFilter implements IRichCardinality {

    public static void main(String[] args) throws CardinalityMergeException, IOException {


        System.out.println("Bloom Filter test:");
        BloomFilter bf = new BloomFilter(8, 8);
        bf.offer(15);
        bf.offer(25);
        bf.offer(35);
        bf.offer(45);
        bf.offer(55);
        bf.offer(65);
        bf.offer(75);
        bf.offer(69);
        bf.offer(79);
        long cad = bf.cardinality();
        System.out.print("Bloom Filter: ");
        System.out.println(cad);

        BloomFilter bf2 = new BloomFilter(8, 8);
        bf2.offer(115);
        bf2.offer(125);
        bf2.offer(135);
        bf2.offer(145);
        bf2.offer(155);
        bf2.offer(165);
        bf2.offer(175);
        System.out.print("Bloom Filter2: ");
        System.out.println(bf2.cardinality());

        System.out.print("Bloom Filter merged with Bloom Filter2: ");
        bf2 = (BloomFilter) bf2.merge(bf);
        System.out.println(bf2.cardinality());
        BloomFilter bf3 = new BloomFilter(8,8);
        bf3 = (BloomFilter) bf2.clone();
        System.out.print("Bloom Filter3 cloned with Bloom Filter2: ");
        System.out.println(bf3.cardinality());
        bf2.offer(1215);
        bf2.offer(1225);
        bf2.offer(1235);
        bf2.offer(1245);
        bf3.offer(1998);
        System.out.print("Bloom Filter3 cloned with Bloom Filter2: ");
        System.out.println(bf3.cardinality());

    }

    @Override
    public int getCount() { return count; }
@Override
    public void setCount(int count) { this.count = count; }

    private int count;
    private BitSet filter_; //the bitmap
    int hashCount; //number of hash functions

    private int numElements;
    private int bucketsPerElement;


    public BloomFilter(int numElements, int bucketsPerElement) {
        this.numElements = numElements;
        this.bucketsPerElement = bucketsPerElement;
        int nbits=numElements * bucketsPerElement + 20;
        this.count = 0;
        if(nbits<0)
        {nbits=Integer.MAX_VALUE;}
        hashCount=BloomCalculations.computeBestK(bucketsPerElement);
        filter_ =   new BitSet(nbits);
    }


    //-----------------------------------------------------------
    //empty the filter
    public void clear() {
        filter_.clear();
    }

    //size of the bloom filter
    public int buckets() {
        return filter_.size();
    }

    //number of 0 bits
    int emptyBuckets() {
        int n = 0;
        for (int i = 0; i < buckets(); i++) {
            if (!filter_.get(i)) {
                n++;
            }
        }
        return n;
    }
    //number of 1 bits
    int fullBuckets() {
        int n = 0;
        for (int i = 0; i < buckets(); i++) {
            if (filter_.get(i)) {
                n++;
            }
        }
        return n;
    }

    //------------------membership test----------------------------
    public boolean isPresent(String key) {
        for (int bucketIndex : getHashBuckets(key)) {
            if (!filter_.get(bucketIndex)) {
                return false;
            }
        }
        return true;
    }
    //----------------------add elements to the filter--------------------
    /*
     * @param key -- value whose hash is used to fill the filter_. This is a general purpose API.
     */
    public boolean offer(Object key) {
        this.count++;
        String k = key.toString();
        if(key!=null)
            for (int bucketIndex : getHashBuckets(k)) {
                filter_.set(bucketIndex);
            }
        else return false;
        return true;
    }

    //-----------------------------------------------

    public int getHashCount() {
        return hashCount;
    }

    BitSet filter() {
        return filter_;
    }

    //----------------------------------------------
    // Murmur is faster than an SHA-based approach and provides as-good collision
    // resistance. The combinatorial generation approach described in
    // http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
    // does prove to work in actual tests, and is obviously faster
    // than performing further iterations of murmur.
    int[] getHashBuckets(String key, int hashCount, int max) {
        byte[] b;
        try {
            b = key.getBytes("UTF-16");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return getHashBuckets(b, hashCount, max);
    }

    int[] getHashBuckets(byte[] b, int hashCount, int max) {
        int[] result = new int[hashCount];
        int hash1 = MurmurHash.hash(b, b.length, 0);
        int hash2 = MurmurHash.hash(b, b.length, hash1);
        for (int i = 0; i < hashCount; i++) {
            result[i] = Math.abs((hash1 + i * hash2) % max);
        }
        return result;
    }

    public int[] getHashBuckets(String key) {
        return getHashBuckets(key, hashCount, buckets());
    }


    public long cardinality_Swamidass()
    { int b=buckets();
        int m=hashCount;
        int x=fullBuckets();
        return (long) (-1* b/m* Math.log(1 - x / ((double) b))); }

    public long cardinality_Papapetrou()
    {int b=buckets();
        int m=hashCount;
        int x=fullBuckets();
        return (long) (    (Math.log(1 - x / ((double) b)))    / (m  *   (Math.log(1 - 1 / ((double) b))))) ; }

    @Override
    public boolean offerHashed(long hashedLong) {
        return false;
    }

    @Override
    public boolean offerHashed(int hashedInt) {
        return false;
    }

    @Override
    public int sizeof() {
        return 0;
    }

    @Override
    public byte[] getBytes() throws IOException {
        return new byte[0];
    }


    public static class BloomFilterException extends CardinalityMergeException
    {
        public BloomFilterException(String message)
        {
            super(message);
        }
    }

    private BitSet getBitSet()
    {
        return this.filter_;
    }


    public void setAll(BloomFilter bf)
    {
        this.filter_ = (BitSet)bf.getBitSet().clone();
        this.hashCount = bf.getHashCount();
    }

    public void mergeAll(BloomFilter bf)
    {
        this.filter_.or(bf.getBitSet());
    }

    public IRichCardinality clone()
    {
        BloomFilter newInstance = new BloomFilter(numElements, bucketsPerElement);
        newInstance.setAll(this);
        return newInstance;
    }

    @Override
    public IRichCardinality merge(IRichCardinality... estimators) throws CardinalityMergeException, IOException {

        BloomFilter newInstance = new BloomFilter(numElements, bucketsPerElement);
        newInstance.setAll(this);
        newInstance.setCount(this.getCount());
        int total_size=this.getCount();
        for (IRichCardinality estimator : estimators)
        {
            total_size+=estimator.getCount();
            if (!(this.getClass().isInstance(estimator)))
            {
                throw new BloomFilterException("Cannot merge estimators of different class");
            }
            if (estimator.sizeof() != this.sizeof())
            {
                throw new BloomFilterException("Cannot merge estimators of different sizes");
            }
            newInstance.mergeAll( (BloomFilter)estimator );
        }
        newInstance.setCount(total_size);
        return newInstance;
    }

    @Override
    public long cardinality() {
        return cardinality_Swamidass();
    }
}


class BloomCalculations {

    private static final int maxBuckets = 15;
    private static final int minBuckets = 2;
    private static final int minK = 1;
    private static final int maxK = 8;
    private static final int[] optKPerBuckets =
            new int[]{1, // dummy K for 0 buckets per element
                    1, // dummy K for 1 buckets per element
                    1, 2, 3, 3, 4, 5, 5, 6, 7, 8, 8, 9, 10, 10, 11, 12, 12, 13, 14};

    /**
     * In the following table, the row 'i' shows false positive rates if i buckets
     * per element are used.  Column 'j' shows false positive rates if j hash
     * functions are used.  The first row is 'i=0', the first column is 'j=0'.
     * Each cell (i,j) the false positive rate determined by using i buckets per
     * element and j hash functions.
     */
    static final double[][] probs = new double[][]{
            {1.0}, // dummy row representing 0 buckets per element
            {1.0, 1.0}, // dummy row representing 1 buckets per element
            {1.0, 0.393, 0.400},
            {1.0, 0.283, 0.237, 0.253},
            {1.0, 0.221, 0.155, 0.147, 0.160},
            {1.0, 0.181, 0.109, 0.092, 0.092, 0.101}, // 5
            {1.0, 0.154, 0.0804, 0.0609, 0.0561, 0.0578, 0.0638},
            {1.0, 0.133, 0.0618, 0.0423, 0.0359, 0.0347, 0.0364},
            {1.0, 0.118, 0.0489, 0.0306, 0.024, 0.0217, 0.0216, 0.0229},
            {1.0, 0.105, 0.0397, 0.0228, 0.0166, 0.0141, 0.0133, 0.0135, 0.0145},
            {1.0, 0.0952, 0.0329, 0.0174, 0.0118, 0.00943, 0.00844, 0.00819, 0.00846}, // 10
            {1.0, 0.0869, 0.0276, 0.0136, 0.00864, 0.0065, 0.00552, 0.00513, 0.00509},
            {1.0, 0.08, 0.0236, 0.0108, 0.00646, 0.00459, 0.00371, 0.00329, 0.00314},
            {1.0, 0.074, 0.0203, 0.00875, 0.00492, 0.00332, 0.00255, 0.00217, 0.00199, 0.00194},
            {1.0, 0.0689, 0.0177, 0.00718, 0.00381, 0.00244, 0.00179, 0.00146, 0.00129, 0.00121, 0.0012},
            {1.0, 0.0645, 0.0156, 0.00596, 0.003, 0.00183, 0.00128, 0.001, 0.000852, 0.000775, 0.000744}, // 15
            {1.0, 0.0606, 0.0138, 0.005, 0.00239, 0.00139, 0.000935, 0.000702, 0.000574, 0.000505, 0.00047, 0.000459},
            {1.0, 0.0571, 0.0123, 0.00423, 0.00193, 0.00107, 0.000692, 0.000499, 0.000394, 0.000335, 0.000302, 0.000287, 0.000284},
            {1.0, 0.054, 0.0111, 0.00362, 0.00158, 0.000839, 0.000519, 0.00036, 0.000275, 0.000226, 0.000198, 0.000183, 0.000176},
            {1.0, 0.0513, 0.00998, 0.00312, 0.0013, 0.000663, 0.000394, 0.000264, 0.000194, 0.000155, 0.000132, 0.000118, 0.000111, 0.000109},
            {1.0, 0.0488, 0.00906, 0.0027, 0.00108, 0.00053, 0.000303, 0.000196, 0.00014, 0.000108, 8.89e-05, 7.77e-05, 7.12e-05, 6.79e-05, 6.71e-05} // 20
    };  // the first column is a dummy column representing K=0.

    /**
     * Given the number of buckets that can be used per element, return the optimal
     * number of hash functions in order to minimize the false positive rate.
     *
     * @param bucketsPerElement
     * @return The number of hash functions that minimize the false positive rate.
     */
    public static int computeBestK(int bucketsPerElement) {
        assert bucketsPerElement >= 0;
        if (bucketsPerElement >= optKPerBuckets.length) {
            return optKPerBuckets[optKPerBuckets.length - 1];
        }
        return optKPerBuckets[bucketsPerElement];
    }

    /**
     * A wrapper class that holds two key parameters for a Bloom Filter: the
     * number of hash functions used, and the number of buckets per element used.
     */
    public static final class BloomSpecification {

        final int K; // number of hash functions.
        final int bucketsPerElement;

        public BloomSpecification(int k, int bucketsPerElement) {
            K = k;
            this.bucketsPerElement = bucketsPerElement;
        }
    }

    /**
     * Given a maximum tolerable false positive probability, compute a Bloom
     * specification which will give less than the specified false positive rate,
     * but minimize the number of buckets per element and the number of hash
     * functions used.  Because bandwidth (and therefore total bitvector size)
     * is considered more expensive than computing power, preference is given
     * to minimizing buckets per element rather than number of hash functions.
     *
     * @param maxFalsePosProb The maximum tolerable false positive rate.
     * @return A Bloom Specification which would result in a false positive rate
     * less than specified by the function call.
     */
    public static BloomSpecification computeBucketsAndK(double maxFalsePosProb) {
        // Handle the trivial cases
        if (maxFalsePosProb >= probs[minBuckets][minK]) {
            return new BloomSpecification(2, optKPerBuckets[2]);
        }
        if (maxFalsePosProb < probs[maxBuckets][maxK]) {
            return new BloomSpecification(maxK, maxBuckets);
        }

        // First find the minimal required number of buckets:
        int bucketsPerElement = 2;
        int K = optKPerBuckets[2];
        while (probs[bucketsPerElement][K] > maxFalsePosProb) {
            bucketsPerElement++;
            K = optKPerBuckets[bucketsPerElement];
        }
        // Now that the number of buckets is sufficient, see if we can relax K
        // without losing too much precision.
        while (probs[bucketsPerElement][K - 1] <= maxFalsePosProb) {
            K--;
        }

        return new BloomSpecification(K, bucketsPerElement);
    }

    /**
     * Calculate the probability of a false positive given the specified
     * number of inserted elements.
     *
     * @param bucketsPerElement number of inserted elements.
     * @param hashCount
     * @return probability of a false positive.
     */
    public static double getFalsePositiveProbability(int bucketsPerElement, int hashCount) {
        // (1 - e^(-k * n / m)) ^ k
        return Math.pow(1 - Math.exp(-hashCount * (1 / (double) bucketsPerElement)), hashCount);

    }
}