package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality;


/**
 * * Implementation of Linear Counting algorithm.
 * * Reference: Whang, K. Y., Vander-Zanden, B. T., & Taylor, H. M. (1990). A linear-time probabilistic counting algorithm for database applications.
 *   ACM Transactions on Database Systems (TODS), 15(2), 208-229
 * * @author Hazar.Harmouch
 * * * source with modification: https://github.com/addthis/stream-lib
 */


import java.io.IOException;
import java.util.Arrays;

import static java.lang.Math.exp;
import static java.lang.Math.max;
import static java.lang.Math.pow;

/**
 * See <i>A Linear-Time Probabilistic Counting Algorithm for Database Applications</i>
 * by Whang, Vander-Zanden, Taylor
 */
public class NLinearCounting implements IRichCardinality {

    public static void main(String[] args) throws Exception {

        float error= 0.3f;

        //error = 1/error;


        NLinearCounting card = new NLinearCounting(0.1, 20);
        card.offer(12);
        card.offer(12);
        card.offer(13);
        card.offer(14);

        NLinearCounting card2 = new NLinearCounting(0.1, 20);
        card2.offer(34);
        card2.offer(45);
        card2.offer(100);
        card2.offer(105);
        card2.offer(206);
        card2.offer(200);
        card2.offer(205);
        card2.offer(906);

        System.out.println(card.cardinality());


        NLinearCounting card_merged = card.merge(card2);

        NLinearCounting card_cloned = card.clone();

        System.out.println(card_merged.cardinality());
        card.offer(2333);
        card.offer(2335);
        card_cloned.offer(4333);
        card_cloned.offer(1333);
        card_cloned.offer(3444);
        System.out.println(card_cloned.cardinality());

    }

    /**
     * Bitmap
     * Hashed stream elements are mapped to bits in this array
     */
    protected byte[] map;

    /**
     * Size of the map in bits
     */
    protected final int length;


    /**
     * Number of bits left unset in the map
     */
    protected int count;

    /**
     * Taken from Table II of Whang et al.
     */
    protected final static int[] onePercentErrorLength =
            {
                    5034, 5067, 5100, 5133, 5166, 5199, 5231, 5264, 5296,                    // 100 - 900
                    5329, 5647, 5957, 6260, 6556, 6847, 7132, 7412, 7688,                    // 1000 - 9000
                    7960, 10506, 12839, 15036, 17134, 19156, 21117, 23029, 24897,            // 10000 - 90000
                    26729, 43710, 59264, 73999, 88175, 101932, 115359, 128514, 141441,       // 100000 - 900000
                    154171, 274328, 386798, 494794, 599692, 702246, 802931, 902069, 999894,  // 1000000 - 9000000
                    1096582                                                                  // 10000000
            };
    /**
     * Returns a LinearCounting estimator which keeps estimates below 1% error on average and has
     * a low likelihood of saturation (0.7%) for any stream with
     * cardinality less than maxCardinality
     *
     * @param maxCardinality
     */
    public NLinearCounting(int maxCardinality) {
        int size=onePercentError(maxCardinality);
        this.length = 8 * size;
        this.count = this.length;
        map = new byte[size];
    }

    public NLinearCounting(byte[] map) {
        this.map = map;
        this.length = 8 * map.length;
        this.count = computeCount();
    }

    /**
     * Builds Linear Counter with arbitrary standard error and maximum expected cardinality.
     * <p/>
     *
     * @param eps            standard error as a fraction (e.g. {@code 0.01} for 1%)
     * @param maxCardinality maximum expected cardinality
     */
    public NLinearCounting(double eps, int maxCardinality) {
        int sz = computeRequiredBitMaskLength(maxCardinality, eps);
        int size=(int) Math.ceil(sz / 8D);
        this.length = 8 * size;
        this.count = this.length;
        map = new byte[size];
    }


    public long cardinality() {
        return (long) (Math.round(length * Math.log(length / ((double) count))));
    }

    public byte[] getBytes() {
        return map;
    }

    @Override


    public NLinearCounting merge(IRichCardinality... estimators) throws IOException {
        if (estimators == null) {
            return new NLinearCounting(map);
        }
        NLinearCounting merged = null;
        if (estimators != null && estimators.length > 0) {
            int size = ((NLinearCounting)estimators[0]).getBytes().length;
            byte[] mergedBytes = new byte[size];

            for (IRichCardinality estimator : estimators) {

                for (int b = 0; b < size; b++) {
                    mergedBytes[b] |= ((NLinearCounting)estimator).getBytes()[b];
                }
            }

            merged = new NLinearCounting(mergedBytes);
        }
        return merged;
    }

    public NLinearCounting clone()
    {
        return new NLinearCounting(map);
    }



    public boolean offerHashed(long hashedLong) {
        throw new UnsupportedOperationException();
    }


    public boolean offerHashed(int hashedInt) {
        throw new UnsupportedOperationException();
    }


    public boolean offer(Object o) {

        boolean modified = false;
        if(o!=null){
            long hash = (long) MurmurHash.hash64(o);
            int bit = (int) ((hash & 0xFFFFFFFFL) % (long) length);
            int i = bit / 8;
            byte b = map[i];
            byte mask = (byte) (1 << (bit % 8));
            if ((mask & b) == 0) {
                map[i] = (byte) (b | mask);
                count--;
                modified = true;
            }
        }
        return modified;
    }


    public int sizeof() {
        return map.length;
    }

    protected int computeCount() {
        int c = 0;
        for (byte b : map) {
            c += Integer.bitCount(b & 0xFF);
        }

        return length - c;
    }

    /**
     * @return (# set bits) / (total # of bits)
     */
    public double getUtilization() {
        return (length - count) / (double) length;
    }

    public int getCount() {
        return count;
    }

    public boolean isSaturated() {
        return (count == 0);
    }

    /**
     * For debug purposes
     *
     * @return
     */
    protected String mapAsBitString() {
        StringBuilder sb = new StringBuilder();
        for (byte b : map) {
            String bits = Integer.toBinaryString(b);
            for (int i = 0; i < 8 - bits.length(); i++) {
                sb.append('0');
            }
            sb.append(bits);
        }
        return sb.toString();
    }



    /**
     * Runs binary search to find minimum bit mask length that holds precision inequality.
     *
     * @param n   expected cardinality
     * @param eps desired standard error
     * @return minimal required bit mask length
     */
    private static int computeRequiredBitMaskLength(double n, double eps) {
        if (eps >= 1 || eps <= 0) {
            throw new IllegalArgumentException("Epsilon should be in (0, 1) range");
        }
        if (n <= 0) {
            throw new IllegalArgumentException("Cardinality should be positive");
        }
        int fromM = 1;
        int toM = 100000000;
        int m;
        double eq;
        do {
            m = (toM + fromM) / 2;
            eq = precisionInequalityRV(n / m, eps);
            if (m > eq) {
                toM = m;
            } else {
                fromM = m + 1;
            }
        } while (toM > fromM);
        return m > eq ? m : m + 1;
    }

    /**
     * @param t   load factor for linear counter
     * @param eps desired standard error
     */
    private static double precisionInequalityRV(double t, double eps) {
        return max(1.0 / pow(eps * t, 2), 5) * (exp(t) - t - 1);
    }

    /**
     * Returns a LinearCounting estimator which keeps estimates below 1% error on average and has
     * a low likelihood of saturation (0.7%) for any stream with
     * cardinality less than maxCardinality
     *
     * @param maxCardinality
     * @return
     * @throws IllegalArgumentException if maxCardinality is not a positive integer
     */
    public static int onePercentError(int maxCardinality) {
        if (maxCardinality <= 0) {
            throw new IllegalArgumentException("maxCardinality (" + maxCardinality + ") must be a positive integer");
        }

        int length = -1;
        if (maxCardinality < 100) {
            length = onePercentErrorLength[0];
        } else if (maxCardinality < 10000000) {
            int logscale = (int) Math.log10(maxCardinality);
            int scaleValue = (int) Math.pow(10, logscale);
            int scaleIndex = maxCardinality / scaleValue;
            int index = 9 * (logscale - 2) + (scaleIndex - 1);
            int lowerBound = scaleValue * scaleIndex;
            length = lerp(lowerBound, onePercentErrorLength[index], lowerBound + scaleValue, onePercentErrorLength[index + 1], maxCardinality);

            //System.out.println(String.format("Lower bound: %9d, Max cardinality: %9d, Upper bound: %9d", lowerBound, maxCardinality, lowerBound+scaleValue));
            //System.out.println(String.format("Lower bound: %9d, Interpolated   : %9d, Upper bound: %9d", onePercentErrorLength[index], length, onePercentErrorLength[index+1]));
        } else if (maxCardinality < 50000000) {
            length = lerp(10000000, 1096582, 50000000, 4584297, maxCardinality);
        } else if (maxCardinality < 100000000) {
            length = lerp(50000000, 4584297, 100000000, 8571013, maxCardinality);
        } else if (maxCardinality <= 120000000) {
            length = lerp(100000000, 8571013, 120000000, 10112529, maxCardinality);
        } else {
            length = maxCardinality / 12;
        }

        return (int) Math.ceil(length / 8D);


    }


    /**
     * @param x0
     * @param y0
     * @param x1
     * @param y1
     * @param x
     * @return linear interpolation
     */
    protected static int lerp(int x0, int y0, int x1, int y1, int x) {
        return (int) Math.ceil(y0 + (x - x0) * (double) (y1 - y0) / (x1 - x0));
    }

}








/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based
 * lookup. See http://murmurhash.googlepages.com/ for more details.
 * <p/>
 * <p>
 * The C version of MurmurHash 2.0 found at that site was ported to Java by
 * Andrzej Bialecki (ab at getopt org).
 * </p>
 */
class NMurmurHash {

    public static int hash(Object o) {
        if (o == null) {
            return 0;
        }
        if (o instanceof Long) {
            return hashLong((Long) o);
        }
        if (o instanceof Integer) {
            return hashLong((Integer) o);
        }
        if (o instanceof Double) {
            return hashLong(Double.doubleToRawLongBits((Double) o));
        }
        if (o instanceof Float) {
            return hashLong(Float.floatToRawIntBits((Float) o));
        }
        if (o instanceof String) {
            return hash(((String) o).getBytes());
        }
        if (o instanceof byte[]) {
            return hash((byte[]) o);
        }
        return hash(o.toString());
    }

    public static int hash(byte[] data) {
        return hash(data, data.length, -1);
    }

    public static int hash(byte[] data, int seed) {
        return hash(data, data.length, seed);
    }

    public static int hash(byte[] data, int length, int seed) {
        int m = 0x5bd1e995;
        int r = 24;

        int h = seed ^ length;

        int len_4 = length >> 2;

        for (int i = 0; i < len_4; i++) {
            int i_4 = i << 2;
            int k = data[i_4 + 3];
            k = k << 8;
            k = k | (data[i_4 + 2] & 0xff);
            k = k << 8;
            k = k | (data[i_4 + 1] & 0xff);
            k = k << 8;
            k = k | (data[i_4 + 0] & 0xff);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // avoid calculating modulo
        int len_m = len_4 << 2;
        int left = length - len_m;

        if (left != 0) {
            if (left >= 3) {
                h ^= (int) data[length - 3] << 16;
            }
            if (left >= 2) {
                h ^= (int) data[length - 2] << 8;
            }
            if (left >= 1) {
                h ^= (int) data[length - 1];
            }

            h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

    public static int hashLong(long data) {
        int m = 0x5bd1e995;
        int r = 24;

        int h = 0;

        int k = (int) data * m;
        k ^= k >>> r;
        h ^= k * m;

        k = (int) (data >> 32) * m;
        k ^= k >>> r;
        h *= m;
        h ^= k * m;

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

    public static long hash64(Object o, int seed) {
        if (o == null) {
            return 0l;
        } else if (o instanceof String) {
            final byte[] bytes = ((String) o).getBytes();
            return hash64(bytes, bytes.length,seed);
        } else if (o instanceof byte[]) {
            final byte[] bytes = (byte[]) o;
            return hash64(bytes, bytes.length,seed);
        }
        return hash64(o.toString(),seed);
    }

    public static long hash64(Object o) {
        if (o == null) {
            return 0l;
        } else if (o instanceof String) {
            final byte[] bytes = ((String) o).getBytes();
            return hash64(bytes, bytes.length);
        } else if (o instanceof byte[]) {
            final byte[] bytes = (byte[]) o;
            return hash64(bytes, bytes.length);
        }
        return hash64(o.toString());
    }

    // 64 bit implementation copied from here:  https://github.com/tnm/murmurhash-java

    /**
     * Generates 64 bit hash from byte array with default seed value.
     *
     * @param data   byte array to hash
     * @param length length of the array to hash
     * @return 64 bit hash of the given string
     */
    public static long hash64(final byte[] data, int length) {
        return hash64(data, length, 0xe17a1465);
    }


    /**
     * Generates 64 bit hash from byte array of the given length and seed.
     *
     * @param data   byte array to hash
     * @param length length of the array to hash
     * @param seed   initial seed value
     * @return 64 bit hash of the given array
     */
    public static long hash64(final byte[] data, int length, int seed) {
        final long m = 0xc6a4a7935bd1e995L;
        final int r = 47;

        long h = (seed & 0xffffffffl) ^ (length * m);

        int length8 = length / 8;

        for (int i = 0; i < length8; i++) {
            final int i8 = i * 8;
            long k = ((long) data[i8 + 0] & 0xff) + (((long) data[i8 + 1] & 0xff) << 8)
                    + (((long) data[i8 + 2] & 0xff) << 16) + (((long) data[i8 + 3] & 0xff) << 24)
                    + (((long) data[i8 + 4] & 0xff) << 32) + (((long) data[i8 + 5] & 0xff) << 40)
                    + (((long) data[i8 + 6] & 0xff) << 48) + (((long) data[i8 + 7] & 0xff) << 56);

            k *= m;
            k ^= k >>> r;
            k *= m;

            h ^= k;
            h *= m;
        }

        switch (length % 8) {
            case 7:
                h ^= (long) (data[(length & ~7) + 6] & 0xff) << 48;
            case 6:
                h ^= (long) (data[(length & ~7) + 5] & 0xff) << 40;
            case 5:
                h ^= (long) (data[(length & ~7) + 4] & 0xff) << 32;
            case 4:
                h ^= (long) (data[(length & ~7) + 3] & 0xff) << 24;
            case 3:
                h ^= (long) (data[(length & ~7) + 2] & 0xff) << 16;
            case 2:
                h ^= (long) (data[(length & ~7) + 1] & 0xff) << 8;
            case 1:
                h ^= (long) (data[length & ~7] & 0xff);
                h *= m;
        }
        ;

        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;

        return h;
    }


}