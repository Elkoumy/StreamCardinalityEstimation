package ee.ut.cs.dsg.ExperimentResults.parsers;
import au.com.bytecode.opencsv.CSVWriter;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import redis.clients.jedis.Jedis;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;


public class GetResults {

    public static void main(String args[]){
//        KLL scotty "C:\Gamal Elkoumy\PhD\OneDrive - Tartu Ülikool\Stream Processing\SWAG & Scotty\initial_results_out" 100000 normal
//        String algorithm=System.getProperty("alg");
//        String approach=System.getProperty("approach");
//        String outDir= System.getProperty("outDir");
//        String tps=System.getProperty("tps");
//        String dist=System.getProperty("dist");
        String algorithm=args[0];
        String approach=args[1];
        String outDir= args[2];
        String tps=args[3];
        String dist=args[4];

        System.out.println("**************************************************************");
        System.out.println("Starting collecting the data from redis to output files");
        Jedis j=new Jedis("redis");
        j.connect();

        Set<String> keys = j.keys("*");
//        ArrayList<Tuple4<String,String,String,String>> query_time = new ArrayList<Tuple4<String,String,String,String>>();
        ArrayList<Tuple4<String,String,String,String>> insertion_time = new ArrayList<Tuple4<String,String,String,String>>();
        ArrayList<Tuple5<String,String,String,String,String>> window = new ArrayList<Tuple5<String,String,String,String,String>>();
        ArrayList<Tuple4<String,String,String,String>> throughput = new ArrayList<Tuple4<String,String,String,String>>();
//        ArrayList<Tuple3<String,String,String>> aggr = new ArrayList<Tuple3<String,String,String>>();
        for (String key : keys){
            Map<String, String> row = j.hgetAll(key);

            if(row.keySet().contains("insertion_start") && row.keySet().contains("insertion_end")){
                insertion_time.add( new Tuple4<String,String,String,String>(key.substring(0,key.indexOf("|"))  ,key.substring(key.indexOf("|")+1,key.lastIndexOf("|"))  ,row.get("insertion_start").toString(),row.get("insertion_end").toString())  );

            }else if (row.keySet().contains("window_count")){
                throughput.add(new Tuple4<String,String,String,String>(key.substring(1,key.indexOf("|")),row.get("window_end").toString(),row.get("window_count").toString(),row.get("out_time").toString()));
            }
            else if(row.keySet().contains("query_start")){
                window.add(new Tuple5<String,String,String,String,String>(key.substring(1,key.indexOf("|")),row.get("window_end_time").toString(), /*key */ key.substring(key.indexOf("|")+1),row.get("query_start").toString(),row.get("query_end").toString()));
            }

        }
        j.disconnect();
        j.quit();
        j.close();
        CSVWriter writer = null;
        /**
         * saving the insertion time
         */
        System.out.println("**** Writing Insertion time ****");
        if (insertion_time.size()!=0) {
//        String parent_dir="C:\\Gamal Elkoumy\\PhD\\OneDrive - Tartu Ülikool\\Stream Processing\\SWAG & Scotty\\initial_results\\";


            try {
                writer = new CSVWriter(new FileWriter(Paths.get(outDir, approach + "_" + algorithm + "_" + tps + "_" + dist + "_insertionTime_" + System.currentTimeMillis() + ".csv").toString()), ',');
                writer.writeNext(new String[]{"item_event_time", "key", "insertion_start", "insertion_end"});
                for (Tuple4 t : insertion_time) {
                    writer.writeNext(new String[]{t.f0.toString(), t.f1.toString(), t.f2.toString(), t.f3.toString()});
                }
                writer.close();
                insertion_time.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }

            insertion_time.clear();
            System.out.println("Writing insertion time finished");
        }

        if (window.size()!=0) {
            System.out.println("**** Writing query time ****");

            writer = null;
            try {
                writer = new CSVWriter(new FileWriter(Paths.get(outDir, approach + "_" + algorithm + "_" + tps + "_" + dist + "_queryTime_" + System.currentTimeMillis() + ".csv").toString()), ',');
                writer.writeNext(new String[]{"window_start", "window_end", "query_start", "query_end"});
                for (Tuple5 t : window) {
                    writer.writeNext(new String[]{t.f0.toString(), t.f1.toString(), t.f2.toString(), t.f3.toString(), t.f4.toString()});
                }
                writer.close();
                window.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println("Writing query time finished");
        }

        if (throughput.size()!=0) {
            System.out.println("**** Writing throughput ****");

            /**
             * saving the throughput
             */

            writer = null;
            try {
                writer = new CSVWriter(new FileWriter(Paths.get(outDir, approach + "_" + algorithm + "_" + tps + "_" + dist + "_throughput_" + System.currentTimeMillis() + ".csv").toString()), ',');
                writer.writeNext(new String[]{"window_start", "window_end", "window_count", "out_time"});
                for (Tuple4 t : throughput) {
                    writer.writeNext(new String[]{t.f0.toString(), t.f1.toString(), t.f2.toString(), t.f3.toString()});
                }
                writer.close();
                window.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println("Writing throughput finished");
        }
        System.out.println("**** Flushing the data from redis ****");
        j=new Jedis("redis");
        j.connect();
        j.flushAll();
        j.disconnect();
        j.quit();
        j.close();


    }

}
