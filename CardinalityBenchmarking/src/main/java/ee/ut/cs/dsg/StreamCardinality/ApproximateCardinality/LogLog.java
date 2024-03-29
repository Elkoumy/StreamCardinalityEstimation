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

import org.streaminer.util.hash.MurmurHash;
import org.streaminer.util.IBuilder;
import sun.rmi.runtime.Log;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LogLog implements IRichCardinality
{
    private final static Logger LOGGER = Logger.getLogger(AdaptiveCounting.class.getName());
    /**
     * Gamma function computed using SciLab
     * ((gamma(-(m.^(-1))).* ( (1-2.^(m.^(-1)))./log(2) )).^(-m)).*m
     */
    protected static final double[] mAlpha = {
            0,
            0.44567926005415,
            1.2480639342271,
            2.8391255240079,
            6.0165231584811,
            12.369319965552,
            25.073991603109,
            50.482891762521,
            101.30047482549,
            202.93553337953,
            406.20559693552,
            812.74569741657,
            1625.8258887309,
            3251.9862249084,
            6504.3071471860,
            13008.949929672,
            26018.222470181,
            52036.684135280,
            104073.41696276,
            208139.24771523,
            416265.57100022,
            832478.53851627,
            1669443.2499579,
            3356902.8702907,
            6863377.8429508,
            11978069.823687,
            31333767.455026,
            52114301.457757,
            72080129.928986,
            68945006.880409,
            31538957.552704,
            3299942.4347441
    };

    protected final int k;
    protected int m;
    protected double Ca;
    protected byte[] M;
    protected int Rsum = 0;
@Override
    public int getCount() { return count; }
@Override
    public void setCount(int count) { this.count = count; }

    private int count;

    @Override
    public String toString() {
        return "LogLog{" +
                "k=" + k +
                ", m=" + m +
                ", Ca=" + Ca +
                ", M=" + Arrays.toString(M) +
                ", Rsum=" + Rsum +
                '}';
    }

    public static double[] getmAlpha() { return mAlpha; }

    public int getK() { return k; }

    public void setByteM(byte[] m) { M = m; }

    public byte[] getByteM() { return M; };

    public int getRsum() { return Rsum; }

    public void setRsum(int rsum) { Rsum = rsum; }

    public void setM(int m) { this.m = m; }

    public int getM() { return m; }

    public double getCa() { return Ca; }

    public void setCa(double ca) { Ca = ca; }

    public LogLog(int k)
    {
        if (k >= (mAlpha.length - 1))
        {
            throw new IllegalArgumentException(String.format("Max k (%d) exceeded: k=%d", mAlpha.length - 1, k));
        }

        this.k = k;
        this.m = 1 << k;
        this.Ca = mAlpha[k];
        this.M = new byte[m];
        this.count=0;
    }

    public LogLog(byte[] M)
    {
        this.M = M;
        this.m = M.length;
        this.k = Integer.numberOfTrailingZeros(m);
        assert (m == (1 << k)) : "Invalid array size: M.length must be a power of 2";
        this.Ca = mAlpha[k];
        this.count = 0;
        for (byte b : M)
        {
            Rsum += b;
        }
    }

    @Override
    public byte[] getBytes()
    {
        return M;
    }

    public int sizeof()
    {
        return m;
    }

    @Override
    public long cardinality()
    {
        /*
        for(int j=0; j<m; j++)
            System.out.print(M[j]+"|");
        System.out.println();
        */

        double Ravg = Rsum / (double) m;
        return (long) (Ca * Math.pow(2, Ravg));
    }

    @Override
    public boolean offerHashed(long hashedLong)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offerHashed(int hashedInt)
    {
        boolean modified = false;
        int j = hashedInt >>> (Integer.SIZE - k);
        byte r = (byte) (Integer.numberOfLeadingZeros((hashedInt << k) | (1 << (k - 1))) + 1);
        if (M[j] < r)
        {
            Rsum += r - M[j];
            M[j] = r;
            modified = true;
        }

        return modified;
    }

    @Override
    public boolean offer(Object o)
    {
        this.count++;
        int x = MurmurHash.getInstance().hash(o);
        return offerHashed(x);
    }

    /**
     * Computes the position of the first set bit of the last Integer.SIZE-k bits
     *
     * @return Integer.SIZE-k if the last k bits are all zero
     */
    protected static int rho(int x, int k)
    {
        return Integer.numberOfLeadingZeros((x << k) | (1 << (k - 1))) + 1;
    }

    /**
     * @return this if estimators is null or no arguments are passed
     * @throws LogLogMergeException if estimators are not mergeable (all estimators must be instances of LogLog of the same size)
     */
    @Override
    public IRichCardinality merge(IRichCardinality... estimators) throws LogLogMergeException
    {
        if (estimators == null)
        {
            LogLog result = new LogLog(M);
            result.setCount(this.getCount());
            return result;
        }

        byte[] mergedBytes = Arrays.copyOf(this.M, this.M.length);


        int total_count=this.getCount();
        for (IRichCardinality estimator : estimators)
        {
            total_count+=estimator.getCount();
            if (!(this.getClass().isInstance(estimator)))
            {
                throw new LogLogMergeException("Cannot merge estimators of different class");
            }
            if (estimator.sizeof() != this.sizeof())
            {
                throw new LogLogMergeException("Cannot merge estimators of different sizes");
            }

            LogLog ll = (LogLog) estimator;

            for (int i = 0; i < mergedBytes.length; ++i)
            {

                mergedBytes[i] = (byte) Math.max(mergedBytes[i], ll.M[i]);
            }
        }

        LogLog result = new LogLog(mergedBytes);
        result.setCount(total_count);
        return result;
    }

    /**
     * Merges estimators to produce an estimator for their combined streams
     *
     * @param estimators
     * @return merged estimator or null if no estimators were provided
     * @throws LogLogMergeException if estimators are not mergeable (all estimators must be the same size)
     */
    public static LogLog mergeEstimators(LogLog... estimators) throws LogLogMergeException
    {
        if (estimators == null || estimators.length == 0)
        {
            return null;
        }
        return (LogLog) estimators[0].merge(Arrays.copyOfRange(estimators, 1, estimators.length));
    }


    @SuppressWarnings("serial")
    public static class LogLogMergeException extends CardinalityMergeException
    {
        public LogLogMergeException(String message)
        {
            super(message);
        }
    }

    public static class Builder implements IBuilder<IRichCardinality>
    {
        protected final int k;

        public Builder()
        {
            this(16);
        }

        public Builder(int k)
        {
            this.k = k;
        }

        @Override
        public LogLog build()
        {
            return new LogLog(k);
        }

        @Override
        public int sizeof()
        {
            return 1 << k;
        }
    }

    public LogLog mergeLogLogObjects(LogLog object2){
        int k3 = this.k + object2.k;
        LogLog mergedObject = new LogLog(k3);
        mergedObject.m = this.m + object2.m;
        //mergedObject.Ca = mergedObject.mAlpha[mergedObject.k];
        mergedObject.Rsum = this.Rsum + object2.Rsum;
        //mergedObject.M = new byte[mergedObject.k];

        mergedObject.count = this.getCount() + object2.getCount();
        for (int i = 0; i < this.M.length; i++) {
            mergedObject.M[i] = this.M[i];
        }

        for (int i = 0; i < object2.M.length; i++) {
            mergedObject.M[i+this.k] = object2.M[i];
        }
        return mergedObject;
    }

    public static LogLog cloneLogLogObjects(LogLog objectToClone){
        int cloneK = objectToClone.getK();
        LogLog clone = new LogLog(cloneK);
        clone.setM(objectToClone.getM());
        clone.setRsum(objectToClone.getRsum());
        clone.setByteM(objectToClone.getByteM());
        clone.setCa(objectToClone.getCa());
        clone.setCount(objectToClone.getCount());
        return clone;
    }

    public static int PowerOf2(final int intnum) {
        int b = 1;
        while (b < intnum) {
            b = b << 1;
        }
        return b/2;
    }

    public static void main(String[] args) throws Exception {
//        LOGGER.setLevel(Level.ALL);
//        LogLog firstObject = new LogLog(1);
//        LogLog secondObject = new LogLog(3);
//        LOGGER.info("firstObject" + firstObject.toString());
//        LOGGER.info("secondObject" + secondObject.toString());
//
//        LogLog mergedObject = firstObject.mergeLogLogObjects(secondObject);
//        LOGGER.info("mergedObject" + mergedObject.toString());
//        LogLog clonedObject = null;
//        clonedObject = cloneLogLogObjects(firstObject);
//        LOGGER.info("clonedObject" + clonedObject.toString());
//        clonedObject.setCa(123);
//        clonedObject.setM(123);
//        LOGGER.info("clonedObject AFTER MODS" + clonedObject.toString());
//        LOGGER.info("firstObject AFTER MODS" + firstObject.toString());

        float error=0.5f;

//        error = 1/error;

        int numofbucket= PowerOf2((int) Math.pow(1.30/error, 2));
        int k=(int) (Math.log(numofbucket)/Math.log(2));
        LogLog card = new LogLog(k);
        card.offer(12);
        card.offer(12);
        card.offer(13);
        card.offer(14);

        LogLog card2 = new LogLog(k);
        card2.offer(34);
        card2.offer(45);
        card2.offer(100);
        card2.offer(105);
        card2.offer(106);

        card.mergeEstimators(card2);

        LogLog card_cloned = LogLog.cloneLogLogObjects(card);

        System.out.println(card.cardinality());
        card.offer(3333);
        card_cloned.offer(3444);
        System.out.println(card_cloned.cardinality());

    }

}