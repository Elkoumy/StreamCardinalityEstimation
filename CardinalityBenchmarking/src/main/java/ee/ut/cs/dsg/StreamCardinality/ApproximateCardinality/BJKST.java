package ee.ut.cs.dsg.StreamCardinality.ApproximateCardinality;

//package org.streaminer.stream.cardinality;

import java.io.IOException;
import java.util.*;
import org.streaminer.util.hash.function.HashFunction;
import org.streaminer.util.hash.function.MurmurHashFunction;

/**
 * Implementation of the BJKST algprothm for distinct counting.
 *
 * Source code: https://github.com/ananthc/streamstats
 *
 * Reference:
 *   Bar-Yossef, Ziv, et al. "Counting distinct elements in a data stream."
 *   Randomization and Approximation Techniques in Computer Science. Springer
 *   Berlin Heidelberg, 2002. 1-10.
 *
 * @author ananthc
 */
public class BJKST implements IRichCardinality {
    private static final long serialVersionUID = -2032575802259420762L;

    private int numMedians=25;
    private int sizeOfMedianSet;

    private double error = 0.02f;

    private List<Integer> limits;

    private int bufferSize = 100;
    private List<HashSet<String>> buffers;

    private List<HashFunction<Object>> hHashers;
    private List<HashFunction<Object>> gHashers;

//    private int intLength = Integer.toString(Integer.MAX_VALUE).length();
    private int intLength = Long.toString(Long.MAX_VALUE).length();

    private String lengthOfIntegerRepresentation = null;



    private int count;

    @Override
    public int getCount() { return count; }
@Override
    public void setCount(int count) { this.count = count; }

    public static void main(String[] args) throws BJKSTException {
        System.out.println("BJKST test:");
        BJKST bjkst = new BJKST(100, 10, 0.1);
        bjkst.offer(15);
        bjkst.offer(25);
        bjkst.offer(35);
        bjkst.offer(45);
        bjkst.offer(55);
        bjkst.offer(65);
        bjkst.offer(75);
        bjkst.offer(85);
        bjkst.offer(74);
        long cad = bjkst.cardinality();
        System.out.print("BJKST: ");
        System.out.println(cad);
        BJKST bjkst2 = new BJKST(100, 10, 0.1);
        bjkst2.offer(115);
        bjkst2.offer(125);
        bjkst2.offer(135);
        bjkst2.offer(145);
        bjkst2.offer(155);
        bjkst2.offer(165);
        bjkst2.offer(175);
        System.out.print("BJKST2: ");
        System.out.println(bjkst2.cardinality());
        System.out.print("BJKST merged with BJKST2: ");
        bjkst2 = (BJKST) bjkst2.merge(bjkst);
        System.out.println(bjkst2.cardinality());
        BJKST bjkst3 = new BJKST(100, 10, 0.1);
        bjkst3 = (BJKST) bjkst2.clone();
        bjkst2.offer(1555);
        System.out.print("BJKST3 cloned with BJKST2: ");
        System.out.println(bjkst3.cardinality());
    }

    public BJKST(int numberOfMedianAttempts, int sizeOfEachMedianSet) {
        this.numMedians = numberOfMedianAttempts;
        this.sizeOfMedianSet = sizeOfEachMedianSet;
        this.count=0;
        init();
    }

    public BJKST(int numberOfMedianAttempts, int sizeOfEachMedianSet, double allowedError) {
        if (allowedError < 0 || allowedError > 1) {
            throw new IllegalArgumentException("Permitted error should be < 1 and in float format");
        }
        this.count=0;
        this.numMedians = numberOfMedianAttempts;
        this.sizeOfMedianSet = sizeOfEachMedianSet;
        this.error = allowedError;
        init();
    }

    private void init() {
        this.bufferSize =  (int) ((this.sizeOfMedianSet) / Math.pow(this.error,2.0) ) ;
        lengthOfIntegerRepresentation = ("%0" + intLength + "d");

        limits   = new ArrayList<Integer>(numMedians);
        buffers  = new ArrayList<HashSet<String>>(numMedians);
        hHashers = new ArrayList<HashFunction<Object>>(numMedians);
        gHashers = new ArrayList<HashFunction<Object>>(numMedians);

        for ( int i =0 ; i < numMedians; i++) {
            limits.add(0);
            buffers.add(new HashSet<String>());
            hHashers.add(new MurmurHashFunction<Object>());
            gHashers.add(new MurmurHashFunction<Object>());
        }
    }

    public boolean offer(Object o) {

        this.count++;

        for ( int i =0 ; i < numMedians; i++) {
            String binaryRepr = Long.toBinaryString(hHashers.get(i).hash(o));

            int zereosP = binaryRepr.length() - binaryRepr.lastIndexOf('1');
            int currentZ = limits.get(i);

            if (zereosP >= currentZ) {
                HashSet<String> currentBuffer = buffers.get(i);

                currentBuffer.add(String.format(lengthOfIntegerRepresentation, gHashers.get(i).hash(o)) +
                        String.format(lengthOfIntegerRepresentation, zereosP));

                while (currentBuffer.size() > bufferSize) {
                    currentZ = currentZ + 1;
                    for (Iterator<String> itr = currentBuffer.iterator(); itr.hasNext();) {
                        String element = itr.next();
                        long zeroesOld = Long.parseLong(element.substring(intLength));
//                        long zeroesOld = Long.parseLong(element);
                        if (zeroesOld < currentZ) {
                            itr.remove();
                        }
                    }
                }
            }
        }
        return true;
    }

    public long cardinality() {
        HashMap<Integer,Integer> results = new HashMap<Integer,Integer>();
        for ( int i =0 ; i < numMedians; i++) {
            int currentGuess = (int)  (buffers.get(i).size() * Math.pow(2,limits.get(i)));
            if (!results.containsKey(currentGuess)) {
                results.put(currentGuess,1);
            }
            else {
                int currentCount = results.get(currentGuess);
                results.put(currentGuess,(currentCount + 1));
            }
        }

        int finalEstimate = 0;
        int highestVote = 0;
        for (Map.Entry<Integer,Integer> pair : results.entrySet()) {
            int possibleAnswer = pair.getValue();
            if (possibleAnswer > highestVote ) {
                highestVote = possibleAnswer;
                finalEstimate = pair.getKey();
            }
        }

        return finalEstimate;
    }

    public static class BJKSTException extends CardinalityMergeException
    {
        public BJKSTException(String message)
        {
            super(message);
        }
    }

    private void putAll(ArrayList<Integer> limits, ArrayList<HashSet<String>> buffer)
    {
        this.limits = (ArrayList<Integer>)limits.clone();
        this.buffers = (ArrayList<HashSet<String>>)buffer.clone();
    }

    public ArrayList<Integer> getLimits()
    {
        return (ArrayList<Integer>)limits;
    }

    public ArrayList<HashSet<String>> getBuffer()
    {
        return (ArrayList<HashSet<String>>)buffers;
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
    public IRichCardinality merge(IRichCardinality... estimators) throws BJKSTException {

        BJKST newInstance = new BJKST(numMedians, sizeOfMedianSet, error);
        newInstance.putAll((ArrayList<Integer>)this.limits, (ArrayList<HashSet<String>>)this.buffers);
        newInstance.setCount(this.getCount());
        int total_count =this.getCount();
        for (IRichCardinality estimator : estimators)
        {
            total_count+=estimator.getCount();
            if (!(this.getClass().isInstance(estimator)))
            {
                throw new BJKSTException("Cannot merge estimators of different class");
            }
            if (estimator.sizeof() != this.sizeof())
            {
                throw new BJKSTException("Cannot merge estimators of different sizes");
            }

            ArrayList<Integer> newLimits = ((BJKST)estimator).getLimits();
            ArrayList<HashSet<String>> newBuffers = ((BJKST)estimator).getBuffer();
            ArrayList<Integer> tmpLimits = new ArrayList<Integer>(numMedians);
            ArrayList<HashSet<String>> tmpBuffers = new ArrayList<HashSet<String>>(numMedians);

            for(int i=0; i<numMedians; i++)
            {
                HashSet<String> tmp = newBuffers.get(i);
                tmp.addAll(buffers.get(i));
                if ( newLimits.get(i) > (limits.get(i)) ) {
                    tmpLimits.add(newLimits.get(i));

                    while (tmp.size() > bufferSize) {
                        for (Iterator<String> itr = tmp.iterator(); itr.hasNext();) {
                            String element = itr.next();
                            long zeroesOld = Long.parseLong(element.substring(intLength));
                            if (zeroesOld < newLimits.get(i)) {
                                itr.remove();
                            }
                        }
                    }

                    tmpBuffers.add(newBuffers.get(i));
                } else {
                    tmpLimits.add(limits.get(i));

                    while (tmp.size() > bufferSize) {
                        for (Iterator<String> itr = tmp.iterator(); itr.hasNext();) {
                            String element = itr.next();
                            long zeroesOld = Long.parseLong(element.substring(intLength));
                            if (zeroesOld < limits.get(i)) {
                                itr.remove();
                            }
                        }
                    }

                    tmpBuffers.add(tmp);
                }

            }
            newInstance.putAll(tmpLimits, tmpBuffers);

        }
        newInstance.setCount(total_count);
        return newInstance;
    }

    public IRichCardinality clone()
    {
        BJKST newInstance = new BJKST(numMedians, sizeOfMedianSet, error);

        newInstance.putAll((ArrayList<Integer>)this.limits, (ArrayList<HashSet<String>>)this.buffers);

        return newInstance;
    }
}