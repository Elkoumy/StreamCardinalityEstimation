package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality;

/*
 * Copyright (C) 2012 Clearspring Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//package org.streaminer.stream.cardinality;

import org.streaminer.util.hash.MurmurHash;
import org.streaminer.util.Bits;
import org.streaminer.util.IBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Java implementation of HyperLogLog (HLL) algorithm from this paper:
 * <p/>
 * http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
 * <p/>
 * HLL is an improved version of LogLog that is capable of estimating
 * the cardinality of a set with accuracy = 1.04/sqrt(m) where
 * m = 2^b.  So we can control accuracy vs space usage by increasing
 * or decreasing b.
 * <p/>
 * The main benefit of using HLL over LL is that it only requires 64%
 * of the space that LL does to get the same accuracy.
 * <p/>
 * This implementation implements a single counter.  If a large (millions)
 * number of counters are required you may want to refer to:
 * <p/>
 * http://dsiutils.dsi.unimi.it/
 * <p/>
 * It has a more complex implementation of HLL that supports multiple counters
 * in a single object, drastically reducing the java overhead from creating
 * a large number of objects.
 * <p/>
 * This implementation leveraged a javascript implementation that Yammer has
 * been working on:
 * <p/>
 * https://github.com/yammer/probablyjs
 * <p>
 * Note that this implementation does not include the long range correction function
 * defined in the original paper.  Empirical evidence shows that the correction
 * function causes more harm than good.
 * </p>
 *
 * <p>
 * Users have different motivations to use different types of hashing functions.
 * Rather than try to keep up with all available hash functions and to remove
 * the concern of causing future binary incompatibilities this class allows clients
 * to offer the value in hashed int or long form.  This way clients are free
 * to change their hash function on their own time line.  We recommend using Google's
 * Guava Murmur3_128 implementation as it provides good performance and speed when
 * high precision is required.  In our tests the 32bit MurmurHash function included
 * in this project is faster and produces better results than the 32 bit murmur3
 * implementation google provides.
 * </p>
 */
public class HyperLogLog implements IRichCardinality {

    private final static Logger LOGGER = Logger.getLogger(AdaptiveCounting.class.getName());

    private final RegisterSet registerSet;

    private final int log2m;

    private final double alphaMM;
@Override
    public int getCount() { return count; }
@Override
    public void setCount(int count) { this.count = count; }

    private int count;

    @Override
    public String toString() {
        return "HyperLogLog{" +
                "registerSet=" + registerSet +
                ", log2m=" + log2m +
                ", alphaMM=" + alphaMM +
                '}';
    }

    public RegisterSet getRegisterSet() { return registerSet; }

    public int getLog2m() { return log2m; }

    public double getAlphaMM() { return alphaMM; }

    /**
     * Create a new HyperLogLog instance using the specified standard deviation.
     *
     * @param rsd The relative standard deviation for the counter.
     *            smaller values create counters that require more space.
     */

    public HyperLogLog(double rsd) {
        this(log2m(rsd));
        this.count=0;
    }

    private static int log2m(double rsd) {
        return (int) (Math.log((1.106 / rsd) * (1.106 / rsd)) / Math.log(2));
    }

    /**
     * Create a new HyperLogLog instance.  The log2m parameter defines the accuracy of
     * the counter.  The larger the log2m the better the accuracy.
     * <p/>
     * accuracy = 1.04/sqrt(2^log2m)
     *
     * @param log2m The number of bits to use as the basis for the HLL instance
     */
    public HyperLogLog(int log2m) {
        this(log2m, new RegisterSet(1 << log2m));
        this.count = 0;
    }

    /**
     * Creates a new HyperLogLog instance using the given registers.
     * Used for unmarshalling a serialized instance and for merging multiple
     * counters together.
     *
     * @param log2m
     * @param registerSet The initial values for the register set
     * @throws IllegalArgumentException If log2m is not within the range [0, 30]
     */
    public HyperLogLog(int log2m, RegisterSet registerSet) {
        if (log2m < 0 || log2m > 30) {
            throw new IllegalArgumentException("log2m argument is "
                    + log2m + " and is outside the range [0, 30]");
        }
        this.registerSet = registerSet;
        this.log2m = log2m;
        int m = 1 << this.log2m;
        this.count = 0;

        // See the paper.
        switch (log2m) {
            case 4:
                alphaMM = 0.673 * m * m;
                break;
            case 5:
                alphaMM = 0.697 * m * m;
                break;
            case 6:
                alphaMM = 0.709 * m * m;
                break;
            default:
                alphaMM = (0.7213 / (1 + 1.079 / m)) * m * m;
        }
    }


    @Override
    public boolean offerHashed(long hashedValue) {
        // j becomes the binary address determined by the first b log2m of x
        // j will be between 0 and 2^log2m
        final int j = (int) (hashedValue >>> (Long.SIZE - log2m));
        final int r = Long.numberOfLeadingZeros((hashedValue << this.log2m) | (1 << (this.log2m - 1)) + 1) + 1;
        return registerSet.updateIfGreater(j, r);
    }

    @Override
    public boolean offerHashed(int hashedValue) {
        // j becomes the binary address determined by the first b log2m of x
        // j will be between 0 and 2^log2m
        final int j = hashedValue >>> (Integer.SIZE - log2m);
        final int r = Integer.numberOfLeadingZeros((hashedValue << this.log2m) | (1 << (this.log2m - 1)) + 1) + 1;
        return registerSet.updateIfGreater(j, r);
    }

    @Override
    public boolean offer(Object o) {
        this.count++;
        final int x = MurmurHash.getInstance().hash(o);
        return offerHashed(x);
    }

    @Override
    public long cardinality() {
        double registerSum = 0;
        int count = registerSet.count;
        double zeros = 0.0;
        for (int j = 0; j < registerSet.count; j++) {
            int val = registerSet.get(j);
            registerSum += 1.0 / (1<<val);
            if (val == 0) {
                zeros++;
            }
        }

        double estimate = alphaMM * (1 / registerSum);

        if (estimate <= (5.0 / 2.0) * count) {
            // Small Range Estimate
            return Math.round(count * Math.log(count / zeros));
        } else {
            return Math.round(estimate);
        }
    }

    @Override
    public int sizeof() {
        return registerSet.size * 4;
    }

    @Override
    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeInt(log2m);
        dos.writeInt(registerSet.size * 4);
        for (int x : registerSet.readOnlyBits()) {
            dos.writeInt(x);
        }

        return baos.toByteArray();
    }

    /** Add all the elements of the other set to this set.
     *
     * This operation does not imply a loss of precision.
     *
     * @param other A compatible Hyperloglog instance (same log2m)
     * @throws CardinalityMergeException if other is not compatible
     */
    public void addAll(HyperLogLog other) throws CardinalityMergeException {
        if (this.sizeof() != other.sizeof()) {
            throw new HyperLogLogMergeException("Cannot merge estimators of different sizes");
        }

        registerSet.merge(other.registerSet);
    }

    @Override
    public IRichCardinality merge(IRichCardinality... estimators) throws CardinalityMergeException {
        HyperLogLog merged = new HyperLogLog(log2m);

        merged.addAll(this);
        merged.setCount(this.getCount());
        if (estimators == null) {
            return merged;
        }

        int total_size=this.getCount();
        for (IRichCardinality estimator : estimators) {
            total_size+=estimator.getCount();
            if (!(estimator instanceof HyperLogLog)) {
                throw new HyperLogLogMergeException("Cannot merge estimators of different class");
            }
            HyperLogLog hll = (HyperLogLog) estimator;
            merged.addAll(hll);
        }
        merged.setCount(total_size);

        return merged;
    }

    public static class Builder implements IBuilder<IRichCardinality>, Serializable {

        private double rsd;
        public Builder(double rsd) {
            this.rsd = rsd;
        }

        @Override
        public HyperLogLog build() {
            return new HyperLogLog(rsd);
        }

        @Override
        public int sizeof() {
            int log2m = log2m(rsd);
            int k = 1 << log2m;
            return RegisterSet.getBits(k) * 4;
        }

        public static HyperLogLog build(byte[] bytes) throws IOException {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            DataInputStream oi = new DataInputStream(bais);
            int log2m = oi.readInt();
            int size = oi.readInt();
            byte[] longArrayBytes = new byte[size];
            oi.readFully(longArrayBytes);
            return new HyperLogLog(log2m, new RegisterSet(1 << log2m, Bits.getBits(longArrayBytes)));
        }

    }
    @SuppressWarnings("serial")
    protected static class HyperLogLogMergeException extends CardinalityMergeException {

        public HyperLogLogMergeException(String message) {
            super(message);
        }
    }

    public HyperLogLog cloneHyperLogLogObject(HyperLogLog object){
//        double rsd = object;
        HyperLogLog clone =new HyperLogLog(object.getLog2m());

//        HyperLogLog clone = new HyperLogLog(0.6);
        try {
            clone.addAll(object);

        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        }
        return clone;
    }

    public static void main(String[] args) throws CardinalityMergeException {
//        LOGGER.setLevel(Level.ALL);
//        HyperLogLog h = new HyperLogLog(.6);
//        h.offer(32);
//        h.offer(12);
//        h.offer(33);
//        h.offer(44);
//        h.offer(24);
//        System.out.println(h.cardinality());
//
//        HyperLogLog h2 = new HyperLogLog(.6);
//            h2.offer(22);
//            h2.offer(22);
//            h2.offer(23);
//            h2.offer(24);
//            h2.offer(24);
//            try {
//                h2= (HyperLogLog) h2.merge(h);
//            } catch (CardinalityMergeException e) {
//                e.printStackTrace();
//        }
//        System.out.println(h2.cardinality());
//        HyperLogLog h1 = new HyperLogLog(h.getLog2m());
//        h1.cloneHyperLogLogObject(h);
//        LOGGER.info("h1 cloning" + h1.toString());
//        HyperLogLog h1ToMerge = new HyperLogLog(h.getLog2m());
//        HyperLogLog hyperLogLogObject1 = new HyperLogLog(.6);
//        HyperLogLog hyperLogLogObject2 = new HyperLogLog(.6);
//        LOGGER.info("hyperLogLogObject1 cloning" + hyperLogLogObject1.toString());
//        LOGGER.info("hyperLogLogObject2 cloning" + hyperLogLogObject2.toString());
//        try {
//            hyperLogLogObject2 = (HyperLogLog)  hyperLogLogObject1.merge(hyperLogLogObject2);
//        } catch (CardinalityMergeException e) {
//            e.printStackTrace();
//        }
//        LOGGER.info("hyperLogLogObject2 cloning" + hyperLogLogObject2.toString());
//


        HyperLogLog card = new HyperLogLog(20);
        card.offer(12);
        card.offer(12);
        card.offer(13);
        card.offer(14);

        HyperLogLog card2 = new HyperLogLog(20);
        card2.offer(34);
        card2.offer(45);
        card2.offer(100);
        card2.offer(105);
        card2.offer(106);

        HyperLogLog card_merged = (HyperLogLog) card.merge(card2);

        System.out.println(card_merged.cardinality());

        HyperLogLog card_cloned=card.cloneHyperLogLogObject(card);

        card.offer(100);

        System.out.println("card "+card.cardinality());
        System.out.println("card_cloned "+card_cloned.cardinality());


    }

}