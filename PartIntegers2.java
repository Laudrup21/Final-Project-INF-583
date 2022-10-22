import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.io.*;
import java.util.*;
/**
 * A. List of Integers!
 *
 */
public class PartIntegers2
{
    public static void main( String[] args ) throws Exception
    {
        String inputFile = args[0];
        String outputFolder = args[1];
        // Create a Java Spark Context
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("integersCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaSparkContext jsc = new JavaSparkContext(jssc.ssc().sc());

        // Load our input data.
        FileReader fr = new FileReader(new File(inputFile));
        BufferedReader br = new BufferedReader(fr);
        String line;
        int count = 0;
        ArrayList <String > batch = new ArrayList <String >();
        Queue <JavaRDD <String >> rdds = new LinkedList <>();
        while ((line = br.readLine()) != null) {
            count +=1;
            if(count == 10){
                JavaRDD <String > rdd = jsc.parallelize(batch); rdds.add(rdd);
                batch = new ArrayList <String >();
                count = 0;
            }
            batch.add(line);
        }
        JavaRDD <String> rdd = jsc.parallelize(batch);
        rdds.add(rdd);
        JavaDStream<String> input_raw = jssc.queueStream(rdds, true);
        JavaDStream<Integer> input = input_raw.map(a -> Integer.parseInt(a));

        // - The largest integer

        /*
        JavaDStream<Integer> JDStreamMaximum = input.reduce((x, y) -> Math.max(x, y));
        List<Integer> ListMaximum = new ArrayList<Integer>();
        JDStreamMaximum.foreachRDD(x -> {
            if (! x.isEmpty()) {
                ListMaximum.add(x.first());
                System.out.println("The current largest integer is : " + Collections.max(ListMaximum));
            }
        });
        */

        // - The average of all the integers.

        /*
        JavaPairDStream<Integer, Integer> StreamSum = input.mapToPair(x -> new Tuple2<>(0, new Tuple2<>(x, 1))).reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2)).mapToPair(x -> new Tuple2<>(x._2._1, x._2._2));
        List<Integer> ListSum = new ArrayList<>();
        List<Integer> ListOcc = new ArrayList<>();
        StreamSum.foreachRDD(x -> {
            if (! x.isEmpty()) {
                Integer somme = x.first()._1;
                Integer n = x.first()._2;
                ListSum.add(somme);
                ListOcc.add(n);
                Integer totalsum = 0;
                Integer totalocc = 0;
                for(int j = 0; j < ListSum.size(); j++){
                    totalsum += ListSum.get(j);
                    totalocc += ListOcc.get(j);
                }
                System.out.println("The current average of integers is : "+(totalsum/totalocc));
            }
        });
        */

        // - The count of the number of distinct integers in the input (Flajolet Martin's Algorithm)

        ///*
        JavaDStream<Integer> hashInput = input.map(x -> (5*x+17)%( (int)Math.pow(2, 8) ));
        JavaDStream<Integer> trailingZeros = hashInput.map(
            new Function<Integer, Integer>() {
                public Integer call(Integer x) {
                    int a = x;
                    if(x == 0){
                        return 0;
                    }else{
                        int result = 0;
                        while(a%2 == 0){
                            result += 1;
                            a = a/2;
                        }
                        return result;
                    }
                }
            }
        );
        JavaDStream<Integer> listMaxZeros = trailingZeros.reduce((a, b) -> Math.max(a, b));
        List<Integer> ListMaximum2 = new ArrayList<>();
        listMaxZeros.foreachRDD(x -> {
            if( ! x.isEmpty()){
                ListMaximum2.add(x.first());
                //System.out.println(ListMaximum2);
                System.out.println("The current approximated number of distinct integers is : "+Math.pow(2, Collections.max(ListMaximum2)));
            }
        });
        //*/

        // The end
        jssc.start();
        try {jssc.awaitTermination();} catch (InterruptedException e) {e.printStackTrace();}
        jssc.stop();
    }
}

