package ee.ut.cs.dsg.StreamCardinality;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import sun.nio.cs.ArrayEncoder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class YetAnotherSource extends RichSourceFunction<Tuple3<Long,String,Long>> {

    private boolean running=true;
    private String filePath;
    private Random random = new Random();
    private long duration;
    private long watermark;
    private long lastWatermark=0;
    private long tps;
    private long tps_per_10ms=10;
    public YetAnotherSource(String file, long duration,long watermark, long tps)
    {
        this.filePath = file;
        this.duration=duration;
        this.watermark=watermark;
        this.tps=tps;
        this.tps_per_10ms=tps/100;

    }
    public YetAnotherSource(String file, long duration,long watermark)
    {
        filePath = file;
        this.duration=duration;
        this.watermark=watermark;

    }

//    @Override
//    public void run(SourceContext<Tuple3<Long, String, Double>> sourceContext) throws Exception {
//
//        long firstTimeStamp=0;
//        try
//        {
//            BufferedReader reader = new BufferedReader(new FileReader(filePath));
//            String line;
//            line = reader.readLine();
//            firstTimeStamp=Long.parseLong(line.split(",")[0]);
//            lastWatermark=Long.parseLong(line.split(",")[0]);
//            int count=0;
//            while (running && line != null)
//            {
//                if (line.startsWith("*"))
//                {
//                    line = reader.readLine();
//                    continue;
//                }
//
//                long ts; double temperature; String key;
//                String[] data = line.split(",");
//
//                if (data.length == 3)
//                {
//                    ts = Long.parseLong(data[0]);
//                    temperature = Double.parseDouble(data[2]);
//                    key = data[1];
////                    if (Integer.parseInt(key)==0 |Integer.parseInt(key)==2 |Integer.parseInt(key)==3|Integer.parseInt(key)==4){
////
////                        continue;
////                    }
//                }
//                else
//                {
//                    ts = Long.parseLong(data[0]);
//                    temperature = Math.round(((random.nextGaussian()*5)+20)*100.0)/100.0;
//                    key = "DUMMY";
//                }
//
////                if (key.equals("W")) // This is a watermark timestamp
////                {
////                    sourceContext.emitWatermark(new Watermark(ts));
////                }
////                else
////                {
////                    sourceContext.collectWithTimestamp( new Tuple3<>(ts,key,temperature), ts);
////                }
//
//                if(ts>watermark+lastWatermark){
//                    lastWatermark=ts;
//                    sourceContext.emitWatermark(new Watermark(ts));
//
//                }else{
//                    sourceContext.collectWithTimestamp( new Tuple3<>(ts,key,temperature), ts);
//                }
//
//                line = reader.readLine();
//                if(Long.parseLong(line.split(",")[0])>duration+firstTimeStamp){
////                    sourceContext.emitWatermark(new Watermark(Long.MAX_VALUE));
//                    cancel();
//                }
//                count++;
//            }
//
//            reader.close();
//        }
//        catch (IOException ioe)
//        {
//            ioe.printStackTrace();
//        }
//    }

    @Override
    public void run(SourceContext<Tuple3<Long, String, Long>> sourceContext) throws Exception{
        long firstTimeStamp=0;
        CsvParserSettings settings = new CsvParserSettings();
        CsvParser parser = new CsvParser(settings);
        File f = new File(this.filePath);
        List<String[]> allRows = parser.parseAll(f);
        parser.stopParsing();
        firstTimeStamp=Long.parseLong(allRows.get(0)[0]);
        lastWatermark=Long.parseLong(allRows.get(0)[0]);
        long cnt = 0;
        long cur = System.currentTimeMillis();


        for (String[] record : allRows) {
            if(Long.parseLong(record[0])>duration+firstTimeStamp){

                cancel();
                sourceContext.emitWatermark(new Watermark(Long.MAX_VALUE));
            }

            if(!running){
                break;
            }
            long ts; long value; String key;

            if (record.length == 3)
            {
                ts = Long.parseLong(record[0]);
                value = Long.parseLong(record[2]);
                key = record[1];
            }
            else
            {
                ts = Long.parseLong(record[0]);
                value = Math.round(((random.nextGaussian()*5)+20)*100.0);
                key = "DUMMY";
            }


            if(ts>watermark+lastWatermark){
                lastWatermark=ts;
                sourceContext.emitWatermark(new Watermark(ts));

            }else{
                sourceContext.collectWithTimestamp( new Tuple3<>(ts,key,value), ts);
            }

            cnt += 1;
            if(cnt >=tps_per_10ms){
                Thread.sleep(cur+10-System.currentTimeMillis());
            }

            long t = System.currentTimeMillis();
            if (t >= 10 + cur) {
//                System.out.println(cnt);
                cnt = 0;
                cur = System.currentTimeMillis();
            }

        }
    }

    @Override
    public void cancel() {
        running = false;

    }
}
