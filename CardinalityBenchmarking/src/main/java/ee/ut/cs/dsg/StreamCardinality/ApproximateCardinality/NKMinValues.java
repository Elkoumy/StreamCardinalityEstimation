package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality;

import java.io.IOException;
import java.util.Collection;
import java.util.TreeSet;

/**
 * * Implementation of AKMV
 *  @author Hazar.Harmouch
 */

public class NKMinValues implements IRichCardinality{
    private TreeSet<Integer> kMin;
    //the order of the used minimum
    private int k=4096;//for eps=0.01
    private double eps = 0.1;

    private int count=0;
    @Override
    public int getCount() { return count; }
    @Override
    public void setCount(int count) { this.count = count; }
    /**
     * @param
     * eps theoretical standard error
     */
    public  NKMinValues(double eps) {
        this.kMin = new TreeSet<Integer>();
        this.eps = eps;
        this.k = PowerOf2( (int) (2+(2/(Math.PI*eps*eps))));
    }

    /**
     * @param
     * key a new element from the dataset
     */
    public boolean offer(Object key) {
        int idx =MurmurHash.hash(key)& Integer.MAX_VALUE;
        if (kMin.size() < k) {
            if (!kMin.contains(idx)) {
                kMin.add(idx);
                return true;
            }
        } else {
            if (idx < kMin.last())
                if (!kMin.contains(idx)) {
                    kMin.pollLast();
                    kMin.add(idx);
                    return true;
                }
        }

        return false;
    }

    /**
     * @return the cardinality estimation.
     **/
    public long cardinality() {
        if (kMin.size() < k)
            return kMin.size(); //exact
        else
        {
            return     (long) ((k - 1.0) * Integer.MAX_VALUE) / (kMin.last());}
    }



    /**
     * @return the next power of 2 larger than the input number.
     **/
    int PowerOf2(final int intnum) {
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

    @Override
    public IRichCardinality merge(IRichCardinality... estimators) throws CardinalityMergeException, IOException {

        if (estimators == null)
        {
            return new NKMinValues(eps);
        }

        NKMinValues newKMV = new NKMinValues(eps);

        Object[] ob = kMin.toArray();
        for ( int i=0; i<ob.length; i++)
        {
            int tmp = (int)ob[i];
            newKMV.offer(tmp);
        }

        for (IRichCardinality estimator : estimators)
        {
            if (!(this.getClass().isInstance(estimator)))
            {
                throw new KMinValuesException("Cannot merge estimators of different class");
            }
            if (estimator.sizeof() != this.sizeof())
            {
                throw new KMinValuesException("Cannot merge estimators of different sizes");
            }
            Object[] ob2 = ((NKMinValues)estimator).getTreeSet().toArray();
            for ( int i=0; i<ob2.length; i++)
            {

                int tmp = (int)ob2[i];
                newKMV.offer(tmp);
            }
        }
        return newKMV;


    }

    private TreeSet<Integer> getTreeSet() {
        return this.kMin;
    }


    public static class KMinValuesException extends CardinalityMergeException
    {
        public KMinValuesException(String message)
        {
            super(message);
        }
    }

    public NKMinValues clone()
    {
        NKMinValues nkmv = new NKMinValues(eps);
        try {
            nkmv = (NKMinValues)nkmv.merge(this);
        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return nkmv;
    }



    public static void main(String[] args) throws CardinalityMergeException, IOException, CloneNotSupportedException {

        System.out.println("KMV test:");
        NKMinValues kmv = new NKMinValues(0.1);
        kmv.offer(15);
        kmv.offer(25);
        kmv.offer(35);
        kmv.offer(45);
        kmv.offer(55);
        kmv.offer(65);
        kmv.offer(75);
        long cad = kmv.cardinality();
        System.out.print("kmv: ");
        System.out.println(cad);
        NKMinValues kmv2 = new NKMinValues(0.1);
        kmv2.offer(115);
        kmv2.offer(125);
        kmv2.offer(135);
        kmv2.offer(145);
        kmv2.offer(155);
        kmv2.offer(165);
        kmv2.offer(175);
        kmv2.offer(16);
        System.out.print("kmv2: ");
        System.out.println(kmv2.cardinality());
        System.out.print("kmv merged with kmv2: ");
        kmv2 = (NKMinValues)kmv2.merge(kmv);

        System.out.println(kmv2.cardinality());
        NKMinValues kmv3 = new NKMinValues(0.1);
        kmv3 = (NKMinValues)kmv2.clone();

        System.out.print("kmv2 : ");
        System.out.println(kmv2.cardinality());
        System.out.print("kmv3 cloned with kmv2: ");
        System.out.println(kmv3.cardinality());



        kmv2.offer(11215);
        kmv2.offer(11225);
        kmv2.offer(11235);
        kmv2.offer(11245);

        System.out.print("kmv2* : ");
        System.out.println(kmv2.cardinality());



        kmv2.offer(510);
        kmv2.offer(550);
        kmv2.offer(550);
        kmv2.offer(524);

        System.out.print("kmv2 : ");
        System.out.println(kmv2.cardinality());


        System.out.print("kmv3 cloned with kmv2: ");
        System.out.println(kmv3.cardinality());

        System.out.print("kmv2 : ");
        System.out.println(kmv2.cardinality());
    }
}



