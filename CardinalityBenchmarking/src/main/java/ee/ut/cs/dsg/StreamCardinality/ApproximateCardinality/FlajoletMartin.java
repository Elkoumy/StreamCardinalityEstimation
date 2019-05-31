package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality;

//package org.streaminer.stream.cardinality;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Flajolet-Martin algorithm approximates the number of unique objects in a
 * stream or a database in one pass.
 *
 * Reference:
 *   Flajolet, Philippe, and G. Nigel Martin. "Probabilistic counting algorithms
 *   for data base applications." Journal of computer and system sciences 31.2
 *   (1985): 182-209.
 *
 * Source code: https://github.com/rbhide0/Columbus
 *
 * @author Ravi Bhide
 */
public class FlajoletMartin implements IRichCardinality {


    private static final double PHI = 0.77351D;
    private int numHashGroups;
    private int numHashFunctionsInHashGroup;
    private int bitmapSize;



    private int count;

    private HashFunction[][] hashes;
    private boolean[][][] bitmaps;

    private long numWords;
@Override
    public int getCount() { return count; }
@Override
    public void setCount(int count) { this.count = count; }

    public static void main(String[] args) throws FMException {

        System.out.println("FM test:");
        FlajoletMartin fm = new FlajoletMartin(7, 3, 3);
        fm.offer(15);
        fm.offer(25);
        fm.offer(35);
        fm.offer(45);
        fm.offer(55);
        fm.offer(65);
        fm.offer(75);
        long cad = fm.cardinality();
        System.out.print("fm: ");
        System.out.println(cad);
        FlajoletMartin fm2 = new FlajoletMartin(7, 3, 3);
        fm2.offer(115);
        fm2.offer(125);
        fm2.offer(135);
        fm2.offer(145);
        fm2.offer(155);
        fm2.offer(165);
        fm2.offer(175);
        System.out.print("fm2: ");
        System.out.println(fm2.cardinality());
        System.out.print("kmv merged with fm2: ");
        fm2 = (FlajoletMartin)fm2.merge(fm);
        System.out.println(fm2.cardinality());
        FlajoletMartin fm3 = new FlajoletMartin(7, 3, 3);
        fm2.offer(1333);
        fm3 = (FlajoletMartin)fm2.clone();

        System.out.print("fm3 cloned with fm2: ");
        System.out.println(fm3.cardinality());
    }


    public FlajoletMartin(int bitmapSize, int numHashGroups, int numHashFunctionsInEachGroup) {
        this.numHashGroups = numHashGroups;
        this.numHashFunctionsInHashGroup = numHashFunctionsInEachGroup;
        this.bitmapSize = bitmapSize;
        this.count=0;
        bitmaps = new boolean[numHashGroups][numHashFunctionsInEachGroup][bitmapSize];
        hashes = new HashFunction[numHashGroups][numHashFunctionsInEachGroup];

        generateHashFunctions();
    }

    private void generateHashFunctions() {
        Map<Integer, Collection<Integer>> mnMap = new HashMap<Integer, Collection<Integer>>();
        for (int i=0; i<numHashGroups; i++) {
            for (int j=0; j<numHashFunctionsInHashGroup; j++) {
                hashes[i][j] = generateUniqueHashFunction(mnMap);
            }
        }
    }

    private HashFunction generateUniqueHashFunction(Map<Integer, Collection<Integer>> mnMap) {
        // Get odd numbers for both m and n.
        int m = 0;
        do {
            m = (int) (Integer.MAX_VALUE * Math.random());
        } while (m % 2 == 0);

        // Get pairs that we haven't seen before.
        int n = 0;
        do {
            n = (int) (Integer.MAX_VALUE * Math.random());
        } while ((n % 2 == 0) || contains(mnMap, m, n));

        // Make a note of the (m, n) pair, so we don't use it again.
        Collection<Integer> valueCollection = mnMap.get(m);
        if (valueCollection == null) {
            valueCollection = new HashSet<Integer>();
            mnMap.put(m, valueCollection);
        }
        valueCollection.add(n);

        // Generate hash function with the (m, n) pair.
        // System.out.println("Generating hashFunction with (m=" + m + ", n=" + n + ")");
        return new HashFunction(m, n, bitmapSize);
    }

    private static boolean contains(Map<Integer, Collection<Integer>> map, int m, int n) {
        Collection<Integer> valueList = map.get(m);
        return (valueList != null) && (valueList.contains(n));
    }

    public boolean offer(Object o) {
        this.count++;
        boolean affected = false;

        for (int i=0; i<numHashGroups; i++) {
            for (int j=0; j<numHashFunctionsInHashGroup; j++) {
                HashFunction f = hashes[i][j];
                long v = f.hash(o);
                int index = rho(v);
                if (!bitmaps[i][j][index]) {
                    bitmaps[i][j][index] = true;
                    affected = true;
                }
            }
        }

        return affected;
    }

    public long cardinality() {
        List<Double> averageR = new ArrayList<Double>();
        for (int i=0; i<numHashGroups; i++) {
            int sumR = 0;
            for (int j=0; j<numHashFunctionsInHashGroup; j++) {
                sumR += (getFirstZeroBit(bitmaps[i][j]));
            }
            averageR.add(sumR * 1.0 / numHashFunctionsInHashGroup);
        }

        // Find the median R and estimate unique items
        Collections.sort(averageR);
        double r = 0;
        int averageRMid = averageR.size() / 2;
        if (averageR.size() % 2 == 0) {
            r = (averageR.get(averageRMid) + averageR.get(averageRMid+1))/2;
        } else {
            r = averageR.get(averageRMid + 1);
        }

        return (long) (Math.pow(2, r) / PHI);
    }

    private int rho(long v) {
        int rho = 0;
        for (int i=0; i<bitmapSize; i++) { // size of long=64 bits.
            if ((v & 0x01) == 0) {
                v = v >> 1;
                rho++;
            } else {
                break;
            }
        }
        return rho == bitmapSize ? 0 : rho;
    }

    private static int getFirstZeroBit(boolean[] b) {
        for (int i=0; i<b.length; i++) {
            if (b[i] == false) {
                return i;
            }
        }
        return b.length;
    }

    private static class HashFunction {
        private int m_m;
        private int m_n;
        private int m_bitmapSize;
        private long m_pow2BitmapSize;

        public HashFunction(int m, int n, int bitmapSize) {
            if (bitmapSize > 64) {
                throw new IllegalArgumentException("bitmap size should be at max. 64");
            }
            this.m_m = m;
            this.m_n = n;
            m_bitmapSize = bitmapSize;

            m_pow2BitmapSize = 1 << m_bitmapSize;
        }

        public long hash(Object o) {
            if (o instanceof String)
                return hash(((String) o).hashCode());
            if (o instanceof Number)
                return hash(String.valueOf(o).hashCode());
            return hash(o.hashCode());
        }

        public long hash(long hashCode) {
            return m_m + m_n * hashCode;
        }
    }

    public static class FMException extends CardinalityMergeException
    {
        public FMException(String message)
        {
            super(message);
        }
    }

    public boolean[][][] getBitmaps()
    {
        return bitmaps;
    }

    private void setAll(boolean[][][] bitmaps) {
        for (int i=0; i<bitmaps.length; i++)
        {
            for (int j=0; j<bitmaps[i].length; j++){
                for (int k=0; k<bitmaps[i][j].length; k++)
                {
                    this.bitmaps[i][j][k] = bitmaps[i][j][k];
                }
            }
        }
    }


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

    @Override
    public IRichCardinality merge(IRichCardinality... estimators) throws FMException {
        FlajoletMartin newInstance = new FlajoletMartin(bitmapSize, numHashGroups, numHashFunctionsInHashGroup);
        newInstance.setAll(this.bitmaps);
        int total_size=this.getCount();
        for (IRichCardinality estimator : estimators)
        {
            total_size+=estimator.getCount();
            if (!(this.getClass().isInstance(estimator)))
            {
                throw new FMException("Cannot merge estimators of different class");
            }
            if (estimator.sizeof() != this.sizeof())
            {
                throw new FMException("Cannot merge estimators of different sizes");
            }
            newInstance.setAll(((FlajoletMartin)estimator).getBitmaps());
        }
        newInstance.setCount(total_size);
        return newInstance;
    }



    public IRichCardinality clone()
    {
        FlajoletMartin newInstance = new FlajoletMartin(bitmapSize, numHashGroups, numHashFunctionsInHashGroup);
        newInstance.setAll(this.bitmaps);
        return newInstance;
    }
}