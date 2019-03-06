package ee.ut.cs.dsg.SWAGwithSCOTTY.ApproximateAggregateFunction;

import ee.ut.cs.dsg.SWAGwithSCOTTY.ApproximateQuantiles.TDigest;
import org.apache.mahout.common.RandomUtils;

import java.util.Random;

public class ApproximateTraditional {
    public static void main(String[] args) throws Exception {
            Random gen = RandomUtils.getRandom();
        TDigest dist = new TDigest(100.0,gen);


        for (int i = 1 ; i<3333333; i++) {
            dist.add(i);

        }
////        dist.compress();
//
        TDigest dist2 = new TDigest(100.0,gen);

        for (int i = 3333333; i<6666666; i++) {
            dist2.add(i);

        }
////        dist2.compress();
//
        TDigest dist3 = new TDigest(100.0,gen);

        for (int i = 6666666 ; i<=10000000; i++) {
            dist3.add(i);

        }
////        dist3.compress();
//
//        List<TDigest> many = Lists.newArrayList();
//        many.add(dist);
//        many.add(dist2);
//        many.add(dist3);
//
//        TDigest total = TDigest.merge(100.0, many);
//        System.out.println(total.quantile(0.5));
//        System.out.println(100*(5000000.0-total.quantile(0.5))/5000000);
//        System.out.println(dist.quantile(.5));
//        System.out.println(dist2.quantile(.5));
//        System.out.println(dist3.quantile(.5));
        dist.add(dist2);
        dist.add(dist3);
        System.out.println(dist.quantile(.5));
        System.out.println(100*(5000000.0-dist.quantile(0.5))/5000000);
    }
}
