package ee.ut.cs.dsg.StreamCardinality;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;

public class ExperimentConfiguration {
    public static RedisClient client = RedisClient.create("redis://redis:6379");
    public static StatefulRedisConnection<String, String> connection = client.connect();
    public static RedisAsyncCommands<String, String> async = connection.async();

    public static  ExperimentType experimentType=ExperimentType.UnInitialized;

    public  enum ExperimentType{
        UnInitialized,
        Latency,
        Throughput,
        Scalability,
        Accuracy
    }
}
