package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality;


import java.io.IOException;
import java.util.BitSet;
import java.util.Random;


/**
 * * Implementation of Probabilistic counting algorithm or FM Sketch.
 * * Reference:
 *   Flajolet, Philippe, and G. Nigel Martin. "Probabilistic counting algorithms
 *   for data base applications." Journal of computer and system sciences 31.2
 *   (1985): 182-209.
 * * @author Hazar.Harmouch.
 */


public class NFlajoletMartin implements IRichCardinality{
    /**
     * correction factor
     */
    private static final double PHI = 0.77351D;
    /**
     * Number of hash functions
     */
    private int numHashFunctions=64;//m depends on the error
    /**
     * Size of the map in bits
     */
    private int bitmapSize=64; //L=64 this is the max for the hash functions
    /**
     * The u=generated hash functions
     */
    private  int[] seeds;

    private double error;

    /**
     * Each Bitmap represents whether we have seen a hash function value whose binary representation ends in 0*i1
     * one for each hash function
     */
    private BitSet[] bitmaps;


    public NFlajoletMartin(double error) {
        // standard error= 1/sqrt(m) => m=(1/error)^2
        this.error = error;
        this.numHashFunctions=nextPowerOf2((int)Math.pow(1/error, 2));
        bitmaps = new BitSet[numHashFunctions];
        for(int i=0;i<numHashFunctions;i++)
            bitmaps[i]=new BitSet(bitmapSize);
        seeds = new int[numHashFunctions];
        generateseeds();
    }




    public boolean offer(Object o) {
        boolean affected = false;
        if(o!=null){
            for (int j=0; j<numHashFunctions; j++) {
                int s = seeds[j];
                //non-negative hash values
                long v = MurmurHash.hash64(o,s);
                //index := pi(hash(x))
                int index =rho(v);
                //update the corresponding bit in the bitmap
                if (bitmaps[j].get(index)==false) {
                    bitmaps[j].set(index,true);
                    affected = true;
                }
            }
        }
        return affected;
    }



    public long cardinality() {
        double sumR=0;
        for (int j=0; j<numHashFunctions; j++) {
            sumR += bitmaps[j].nextClearBit(0);

        }

        return (long) (Math.pow(2, sumR/numHashFunctions) / PHI);
    }

    /**
     * @return the position of the least significant 1-bit in the binary representation of y
     *         rho(O)=0
     */
    private int rho(long y) {
        return Long.numberOfTrailingZeros(y);
    }

    /**
     * @return the next power of 2 larger than the input number.
     **/
    public static int nextPowerOf2(final int intnum) {
        int b = 1;
        while (b < intnum) {
            b = b << 1;
        }
        return b/2;
    }


    private void generateseeds() {
        Random generator = new Random(9001);
        for (int j=0; j<numHashFunctions; j++) {
            seeds[j] =generator.nextInt();
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

    private void setAll(BitSet[] bitmaps) {
        for (int i=0; i<this.bitmaps.length; i++)
        {
            this.bitmaps[i] = (BitSet)bitmaps[i].clone();
        }

    }

    private BitSet[] getBitmaps() {
        return this.bitmaps;
    }

    private void setOrAll(BitSet[] newBitSet)
    {
        //this.bitmaps.or(newBitSet);
        for (int i=0; i<this.bitmaps.length; i++)
        {
            this.bitmaps[i].or(newBitSet[i]);
        }
    }

    public static class NFMException extends CardinalityMergeException
    {
        public NFMException(String message)
        {
            super(message);
        }
    }

    @Override
    public IRichCardinality merge(IRichCardinality... estimators) throws CardinalityMergeException, IOException {
        NFlajoletMartin newInstance = new NFlajoletMartin(this.error);
        newInstance.setAll(this.bitmaps);

        for (IRichCardinality estimator : estimators)
        {
            if (!(this.getClass().isInstance(estimator)))
            {
                throw new NFMException("Cannot merge estimators of different class");
            }
            if (estimator.sizeof() != this.sizeof())
            {
                throw new NFMException("Cannot merge estimators of different sizes");
            }
            newInstance.setOrAll(((NFlajoletMartin)estimator).getBitmaps());
        }
        return newInstance;
    }

    public IRichCardinality clone()
    {
        NFlajoletMartin newInstance = new NFlajoletMartin(this.error);
        newInstance.setAll(this.bitmaps);
        return newInstance;
    }

    public static void main(String[] args) throws CardinalityMergeException, IOException {

        System.out.println("FM test:");
        NFlajoletMartin  fm = new NFlajoletMartin(0.1);
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


        NFlajoletMartin fm2 = new NFlajoletMartin(0.1);
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

        fm2 = (NFlajoletMartin)fm2.merge(fm);
        System.out.println(fm2.cardinality());
        NFlajoletMartin fm3 = new NFlajoletMartin(0.1);
        fm2.offer(1333);
        fm3 = (NFlajoletMartin)fm2.clone();

        System.out.print("fm3 cloned with fm2: ");
        System.out.println(fm3.cardinality());

        fm2.offer(1215);
        fm2.offer(1225);
        fm2.offer(1235);
        fm2.offer(1245);
        System.out.print("fm3 cloned with fm2: ");
        System.out.println(fm3.cardinality());
    }
}