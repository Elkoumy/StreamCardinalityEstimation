package ee.ut.cs.dsg.StreamCardinality.ApproximateQuantiles;

public interface IQuantiles<T>
{
    void offer(T value);

    T getQuantile(double q) throws Exception;


}
