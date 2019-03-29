package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality;

/*
 * Copyright (C) 2011 Clearspring Technologies, Inc.
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

import java.io.Serializable;
import java.util.Arrays;
import java.util.ListIterator;

import ee.ut.cs.dsg.StreamCardinality.ApproximateQuantiles.CKMSQuantiles;
import org.streaminer.util.hash.Lookup3Hash;
import org.streaminer.util.IBuilder;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>
 * Based on the adaptive counting approach of:<br/>
 * <i>Fast and Accurate Traffic Matrix Measurement Using Adaptive Cardinality Counting</i><br>
 * by:  Cai, Pan, Kwok, and Hwang
 * </p>
 * <p/>
 * TODO: use 5 bits/bucket instead of 8 (37.5% size reduction)<br/>
 * TODO: super-LogLog optimizations
 */
public class AdaptiveCounting extends LogLog {
    /**
     * Number of empty buckets
     */
    protected int b_e;
    private final static Logger LOGGER = Logger.getLogger(AdaptiveCounting.class.getName());

    /**
     * Switching empty bucket ratio
     */
    protected final double B_s = 0.051;

    public AdaptiveCounting(int k) {
        super(k);
        b_e = m;
    }

    public AdaptiveCounting(byte[] M) {
        super(M);

        for (byte b : M) {
            if (b == 0) {
                b_e++;
            }
        }
    }




    public static void main(String[] args){
        LOGGER.setLevel(Level.ALL);
        AdaptiveCounting firstObject = new AdaptiveCounting(1);
        AdaptiveCounting secondObject = new AdaptiveCounting(1);
        //LogLog secondObject = new LogLog(1);
        AdaptiveCounting object3 = firstObject.mergeAdaptiveCountingObjects(secondObject);
        LOGGER.info("first object = " + object3.toString());

    }


    @Override
    public boolean offer(Object o) {
        boolean modified = false;

        long x = Lookup3Hash.lookup3ycs64(o.toString());
        int j = (int) (x >>> (Long.SIZE - k));
        byte r = (byte) (Long.numberOfLeadingZeros((x << k) | (1 << (k - 1))) + 1);
        if (M[j] < r) {
            Rsum += r - M[j];
            if (M[j] == 0) {
                b_e--;
            }
            M[j] = r;
            modified = true;
        }

        return modified;
    }

    @Override
    public long cardinality() {
        double B = (b_e / (double) m);
        if (B >= B_s) {
            return (long) Math.round(-m * Math.log(B));
        }

        return super.cardinality();
    }


    /**
     * Computes the position of the first set bit of the last Long.SIZE-k bits
     *
     * @param x
     * @param k
     * @return Long.SIZE-k if the last k bits are all zero
     */
    protected static byte rho(long x, int k) {
        return (byte) (Long.numberOfLeadingZeros((x << k) | (1 << (k - 1))) + 1);
    }

    /**
     * @param estimators The estimators to be merged
     * @return this if estimators is null or no arguments are passed
     * @throws LogLogMergeException if estimators are not mergeable (all estimators must be instances of LogLog of the same size)
     */
    @Override
    public IRichCardinality merge(IRichCardinality... estimators) throws LogLogMergeException {
        LogLog res = (LogLog) super.merge(estimators);
        return new AdaptiveCounting(res.M);
    }

    /**
     * Merges estimators to produce an estimator for their combined streams
     *
     * @param estimators
     * @return merged estimator or null if no estimators were provided
     * @throws LogLogMergeException if estimators are not mergeable (all estimators must be the same size)
     */
    public static AdaptiveCounting mergeEstimators(LogLog... estimators) throws LogLogMergeException {
        if (estimators == null || estimators.length == 0) {
            return null;
        }
        return (AdaptiveCounting) estimators[0].merge(Arrays.copyOfRange(estimators, 1, estimators.length));
    }

    public static class Builder implements IBuilder<IRichCardinality>, Serializable {
        private static final long serialVersionUID = 2205437102378081334L;
        protected final int k;

        public Builder() {
            this(16);
        }

        public Builder(int k) {
            this.k = k;
        }

        @Override
        public AdaptiveCounting build() {
            return new AdaptiveCounting(k);
        }

        @Override
        public int sizeof() {
            return 1 << k;
        }

        /**
         * <p>
         * For cardinalities less than 4.25M, obyCount provides a LinearCounting Builder
         * (see LinearCounting.Builder.onePercentError() ) using only the
         * space required to provide estimates within 1% of the actual cardinality,
         * up to ~65k.
         * </p>
         * <p>
         * For cardinalities greater than 4.25M, an AdaptiveCounting builder is returned
         * that allocates ~65KB and provides estimates with a Gaussian error distribution
         * with an average error of 0.5% and a standard deviation of 0.5%
         * </p>
         *
         * @param maxCardinality
         * @return Returns a Builder according to the maxCardinality
         * @throws IllegalArgumentException if maxCardinality is not a positive integer
         * @see LinearCounting.Builder#onePercentError(int)
         */
        public static IBuilder<IRichCardinality> obyCount(long maxCardinality) {
            if (maxCardinality <= 0) {
                throw new IllegalArgumentException("maxCardinality (" + maxCardinality + ") must be a positive integer");
            }

            if (maxCardinality < 4250000) {
                return LinearCounting.Builder.onePercentError((int) maxCardinality);
            }

            return new Builder(16);
        }
    }

    public AdaptiveCounting mergeAdaptiveCountingObjects(AdaptiveCounting object2){
        int k3 = this.k + object2.k;
        AdaptiveCounting mergedObject = new AdaptiveCounting(k3);
        mergedObject.m = this.m + object2.m;
        //mergedObject.Ca = mergedObject.mAlpha[mergedObject.k];
        mergedObject.Rsum = this.Rsum + object2.Rsum;
        //mergedObject.M = new byte[mergedObject.k];
        // Mida teeb this.m = 1 << k; et siis saab aru saada, mida m teeb, kui suur see on

        for (int i = 0; i < this.M.length; i++) {
            mergedObject.M[i] = this.M[i];
        }

        for (int i = 0; i < object2.M.length; i++) {
            mergedObject.M[i+this.k] = object2.M[i];
        }
        mergedObject.b_e = mergedObject.m;
        return mergedObject;
    }

    @Override
    public String toString() {
        return "AdaptiveCounting{" +
                "b_e=" + b_e +
                ", B_s=" + B_s +
                ", k=" + k +
                ", m=" + m +
                ", Ca=" + Ca +
                ", M=" + Arrays.toString(M) +
                ", Rsum=" + Rsum +
                '}';
    }
}