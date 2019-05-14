package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality;

import java.io.IOException;

/** Implementation of HyperLogLog
 ** Reference:
 *  Flajolet, P., Fusy, É., Gandouet, O., & Meunier, F. (2008). Hyperloglog: the analysis of a near-optimal cardinality estimation algorithm. DMTCS Proceedings, (1).
 * * @author Hazar.Harmouch
 * *  source with modification: https://github.com/addthis/stream-lib
 */
public class NHyperLogLog implements IRichCardinality{

    private final RegisterSet registerSet;
    private final int log2m;// b in the paper
    private double error = 0.1;
    private final double alphaMM;// the multiplication of m and alpha m in the cardinality relation



    public NHyperLogLog(double error) {
        int m =PowerOf2((int) Math.pow(1.04/error, 2)); // get the number of registers according to the standard error
        this.log2m  =(int) (Math.log(m)/Math.log(2)); // size of the portion of the hash used to determine the register
        validateLog2m(log2m); // [4,16] for 32 bit i make it 32 as half of 64

        this.error = error;
        this.registerSet =new RegisterSet(1 << log2m);
        int n = 1 << this.log2m;
        alphaMM = getAlphaMM(log2m, n);

    }


    private static void validateLog2m(int log2m) {
        if (log2m < 4 || log2m > 32) {
            throw new IllegalArgumentException("log2m argument is "
                    + log2m + " and is outside the range [4, 32]");
        }
    }

    public boolean offer(Object o) {
        boolean affected=false;
        if(o!=null){
            final long x = MurmurHash.hash64(o);
            // j becomes the binary address determined by the first b log2m of x
            // j will be between 0 and 2^log2m
            final int j = (int) (x >>> (Long.SIZE - log2m));
            final int r = Long.numberOfLeadingZeros((x << this.log2m) | (1 << (this.log2m - 1)) + 1) + 1;
            affected= registerSet.updateIfGreater(j, r);
        }
        return affected;
    }

    public long cardinality() {
        double registerSum = 0;
        int count = registerSet.count;
        double zeros = 0.0;
        for (int j = 0; j < registerSet.count; j++) {
            int val = registerSet.get(j);
            registerSum += 1.0 / (1 << val);
            if (val == 0) {
                zeros++;
            }
        }

        double estimate = alphaMM * (1 / registerSum);

        if (estimate <= (5.0 / 2.0) * count) {
            // Small Range Estimate
            return Math.round(linearCounting(count, zeros));
        } else if (estimate > (1 / 30) * Math.pow(2, 64))
        //large range correction
        { return Math.round( -1*Math.pow(2, 64)* Math.log(1-estimate/Math.pow(2, 64)) );}
        else  {
            //intermediate range no correction
            return Math.round(estimate);
        }
    }

    protected static double getAlphaMM(final int p, final int m) {
        // See the paper.
        switch (p) {
            case 4:
                return 0.673 * m * m;
            case 5:
                return 0.697 * m * m;
            case 6:
                return 0.709 * m * m;
            default:
                return (0.7213 / (1 + 1.079 / m)) * m * m;
        }
    }

    protected static double linearCounting(int m, double V) {
        return m * Math.log(m / V);
    }

    public static int PowerOf2(final int intnum) {
        int b = 1;
        while (b < intnum) {
            b = b << 1;
        }
        return b/2;
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

    protected static class NHyperLogLogMergeException extends CardinalityMergeException
    {
        public NHyperLogLogMergeException(String message)
        {
            super(message);
        }
    }

    public void addAll(NHyperLogLog other) throws CardinalityMergeException {
        if (this.sizeof() != other.sizeof()) {
            throw new NHyperLogLogMergeException("Cannot merge estimators of different sizes");
        }

        registerSet.merge(other.registerSet);
    }

    @Override
    public IRichCardinality merge(IRichCardinality... estimators) throws CardinalityMergeException, IOException {
        NHyperLogLog merged = new NHyperLogLog(this.error);
        merged.addAll(this);

        if (estimators == null) {
            return merged;
        }

        for (IRichCardinality estimator : estimators) {
            if (!(estimator instanceof NHyperLogLog)) {
                throw new NHyperLogLogMergeException("Cannot merge estimators of different class");
            }
            NHyperLogLog hll = (NHyperLogLog) estimator;
            merged.addAll(hll);
        }

        return merged;
    }

    public IRichCardinality clone()
    {
        NHyperLogLog merged = new NHyperLogLog(this.error);
        try {
            merged.addAll(this);
        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        }
        return merged;
    }

    @SuppressWarnings("serial")
    protected static class HyperLogLogMergeException extends CardinalityMergeException {

        public HyperLogLogMergeException(String message) {
            super(message);
        }
    }


    public static void main(String[] args) throws Exception {


        NHyperLogLog card = new NHyperLogLog(0.1);
        card.offer(12);
        card.offer(12);
        card.offer(13);
        card.offer(14);
        card.offer(24);
        card.offer(74);
        System.out.println(card.cardinality());

        NHyperLogLog card2 = new NHyperLogLog(0.1);
        card2.offer(34);
        card2.offer(45);
        card2.offer(100);
        card2.offer(105);
        card2.offer(106);

        System.out.println(card2.cardinality());
        NHyperLogLog card_merged =(NHyperLogLog) card.merge(card2);

        NHyperLogLog card_cloned = (NHyperLogLog)card.clone();

        System.out.println(card_merged.cardinality());
        card.offer(3333);
        card.offer(2333);
        card.offer(1333);
        card.offer(9333);
        card_cloned.offer(3444);
        System.out.println(card_cloned.cardinality());

    }
}