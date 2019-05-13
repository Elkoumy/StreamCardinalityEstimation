package ee.ut.cs.dsg.StreamCardinality.ApproximateThroughput;

import org.apache.flink.api.java.tuple.Tuple2;
import java.util.ArrayList;

public class ApproximateThroughputCounter {

    private static ApproximateThroughputCounter at = null;
    private static Integer WindowCounter = 0;
    private static ArrayList<Tuple2<String, Integer>> Throughput = new ArrayList<Tuple2<String, Integer>>();

    private ApproximateThroughputCounter() {}

    public static ApproximateThroughputCounter getInstance() {
        if (at == null) {
            synchronized (ApproximateThroughputCounter.class) {
                if (at == null) {
                    at = new ApproximateThroughputCounter();
                }
            }

        }
        return at;
    }

    public ArrayList<Tuple2<String, Integer>> getResults() {
        return Throughput;
    }

    public void push(Tuple2 record)
    {
        this.WindowCounter++;
        this.Throughput.add(record);
    }
}
