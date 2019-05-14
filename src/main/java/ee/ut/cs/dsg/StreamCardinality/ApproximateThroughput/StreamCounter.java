package ee.ut.cs.dsg.StreamCardinality.ApproximateThroughput;

import java.util.ArrayList;

public class StreamCounter {

    private static StreamCounter at = null;

    private static ArrayList<String> CounterInfo = new ArrayList<String>();
    private static Integer WindowCounter = 0;
    private static Integer WindowCounter1 = 0;
    private static Integer WindowCounter2 = 0;
    private static Integer WindowCounter3 = 0;
    private static Integer WindowCounter4 = 0;
    private static Integer WindowCounter5 = 0;

    private StreamCounter() {
        CounterInfo.add("default counter 1");
        CounterInfo.add("default counter 2");
        CounterInfo.add("default counter 3");
        CounterInfo.add("default counter 4");
        CounterInfo.add("default counter 5");
    }

    public static StreamCounter getInstance() {
        if (at == null) {
            synchronized (StreamCounter.class) {
                if (at == null) {
                    at = new StreamCounter();
                }
            }

        }
        return at;
    }

    public Integer getResults() {
        return WindowCounter;
    }

    public void push()
    {
        this.WindowCounter++;
    }
    public void pushTo(Integer idt)
    {
        switch (idt)
        {
            case 1:
                this.WindowCounter1++;
                break;
            case 2:
                this.WindowCounter2++;
                break;
            case 3:
                this.WindowCounter3++;
                break;
            case 4:
                this.WindowCounter4++;
                break;
            case 5:
                this.WindowCounter5++;
                break;
            default:
                this.WindowCounter++;

        }
    }

    public Integer getOf(Integer idt)
    {

        switch (idt)
        {
            case 1:
                return WindowCounter1;
            case 2:
                return WindowCounter2;
            case 3:
                return WindowCounter3;
            case 4:
                return WindowCounter4;
            case 5:
                return WindowCounter5;
            default:
                return WindowCounter;

        }
    }

    public void registerNameAt(String name, Integer nr)
    {
        this.CounterInfo.set(nr, name);
    }

    public String getRegisterNameOf(String name, Integer nr)
    {
        return this.CounterInfo.get(nr);
    }
}
