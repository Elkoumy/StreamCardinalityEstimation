package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality;

//ackage org.streaminer.stream.cardinality;

import java.io.IOException;
import java.util.TreeSet;
import org.streaminer.util.hash.Hash;
import org.streaminer.util.hash.MurmurHash;

/**
 * K-Minimum Values.
 * Python Source Code: https://github.com/mynameisfiber/countmemaybe
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class KMinValues implements IRichCardinality {
    private TreeSet<Integer> kMin;
    private int k;
    private Hash hasher;

    public static void main(String[] args) throws KMinValuesException {

        System.out.println("KMV test:");
        KMinValues kmv = new KMinValues(3, MurmurHash.getInstance());
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
        KMinValues kmv2 = new KMinValues(3, MurmurHash.getInstance());
        kmv2.offer(115);
        kmv2.offer(125);
        kmv2.offer(135);
        kmv2.offer(145);
        kmv2.offer(155);
        kmv2.offer(165);
        kmv2.offer(175);
        System.out.print("kmv2: ");
        System.out.println(kmv2.cardinality());
        System.out.print("kmv merged with kmv2: ");
        kmv2 = (KMinValues)kmv2.merge(kmv);
        System.out.println(kmv2.cardinality());
        KMinValues kmv3 = new KMinValues(3, MurmurHash.getInstance());
        kmv3 = (KMinValues)kmv2.clone();
        System.out.print("kmv3 cloned with kmv2: ");
        System.out.println(kmv3.cardinality());
    }

    public KMinValues(int k) {
        this(k, Hash.getInstance(Hash.MURMUR_HASH3));
    }

    public KMinValues(int k, Hash hasher) {
        this.kMin = new TreeSet<Integer>();
        this.k = k;
        this.hasher = hasher;
    }

    public boolean offer(Object key) {
        int idx = index(key);

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

    private boolean addOffer(int idx)
    {
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

    public long cardinality() {
        if (kMin.size() < k)
            return kMin.size();
        else
            return (long) cardHelp(kMin, k);
    }

    public void union(KMinValues... others) {
        int newK = smallestK(others);
        for (KMinValues o : others)
            kMin.addAll(o.kMin);

        kMin = new TreeSet<Integer>(kMin.subSet(0, newK));
    }

    public double jaccard(KMinValues other) {
        DirectSum ds = directSum(other);
        return ds.n / (1.0 * ds.x.size());
    }

    public double cardinalityUnion(KMinValues... others) {
        DirectSum ds = directSum(others);
        double cardX = cardHelp(ds.x, ds.x.size());
        return cardX;
    }

    private double cardHelp(TreeSet<Integer> kMin, int k) {
        return ((k - 1.0) * Integer.MAX_VALUE) / (kMin.last());
    }

    private boolean inAll(int item, KMinValues... others) {
        for (KMinValues o : others)
            if (!o.kMin.contains(item))
                return false;
        return true;
    }

    private DirectSum directSum(KMinValues... others) {
        DirectSum ds = new DirectSum();
        int k = smallestK(others);

        for (KMinValues o : others)
            ds.x.addAll(o.kMin);

        ds.x = new TreeSet<Integer>(ds.x.subSet(0, k));

        for (int item : ds.x)
            if (kMin.contains(item) && inAll(item, others))
                ds.n++;

        return ds;
    }

    private int smallestK(KMinValues... others) {
        int newK = Integer.MAX_VALUE;
        for (KMinValues o : others) {
            if (o.k < newK)
                newK = o.k;
        }
        return newK;
    }

    private int index(Object key) {
        return hasher.hash(key) & Integer.MAX_VALUE;
    }

    private static class DirectSum {
        public int n = 0;
        public TreeSet<Integer> x;
    }

    private IRichCardinality mergeTreeSet( IRichCardinality estimator,TreeSet<Integer> newTreeSet)
    {

        Object[] ob = newTreeSet.toArray();
        for ( int i=0; i<ob.length; i++)
        {
            estimator.offer(ob[i]);
        }
        return estimator;
    }

    public TreeSet<Integer> getTreeSet()
    {
        return kMin;
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
    public IRichCardinality merge(IRichCardinality... estimators) throws KMinValuesException {
        if (estimators == null)
        {
            return new KMinValues(k, hasher);
        }


        KMinValues newKMV = new KMinValues(k, hasher);

        Object[] ob = kMin.toArray();
        for ( int i=0; i<ob.length; i++)
        {
            int tmp = (int)ob[i];
            newKMV.addOffer(tmp);
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
            Object[] ob2 = ((KMinValues)estimator).getTreeSet().toArray();
            for ( int i=0; i<ob2.length; i++)
            {

                int tmp = (int)ob2[i];
                newKMV.addOffer(tmp);
            }
        }
        return newKMV;
    }

    protected static class KMinValuesException extends CardinalityMergeException
    {
        public KMinValuesException(String message)
        {
            super(message);
        }
    }

    public IRichCardinality clone()
    {
        KMinValues newInstance = new KMinValues(k, hasher);
        Object[] ob = kMin.toArray();
        for ( int i=0; i<ob.length; i++)
        {
            int tmp = (int)ob[i];
            newInstance.addOffer(tmp);
        }
        return newInstance;
    }

}
