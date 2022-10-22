import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;


/**
 * A. List of Integers!
 *
 */
public class PartIntegers
{
    public static void main( String[] args )
    {
        String inputFile = args[0];
        String outputFolder = args[1];
        // Create a Java Spark Context
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("integersCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load our input data.
        JavaRDD<String> input_raw = sc.textFile(inputFile);
        JavaRDD<Integer> input = input_raw.map(a -> Integer.parseInt(a));

        //Integer.parseInt() - > int Integer.valueOf() - > Integer
        // - The largest integer
        long maximum = input.reduce((a, b) -> Math.max(a, b));
        System.out.println("The largest integer is : "+maximum);
        // - The average of all the integers.
        double average =  input.reduce((a, b) -> a + b)/input.count();
        System.out.println("The average of all the integers is : "+average);
        // - The same set of integers, but with each integer appearing only once.
        JavaPairRDD<Integer, Integer> distinct_integers = input.mapToPair(number -> new Tuple2<>(number, 1));
        JavaPairRDD<Integer, Integer> distinct_integers_ = distinct_integers.reduceByKey((a, b) -> a + b);
        JavaRDD<Integer> new_input = distinct_integers_.keys().distinct();
        new_input.saveAsTextFile(outputFolder);
        // - The count of the number of distinct integers in the input
        System.out.println("The number of distinct integers in the input is : "+new_input.count());
    }
}
