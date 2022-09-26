package org.com.ufjf.lucas.RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class test2 {
    public static void main(String[] args) {

        List<String> inputData = new ArrayList<String>();
        inputData.add("banana maça arroz");
        inputData.add("maça china cartago catarro");
        inputData.add("mariscos nordestinos");
        inputData.add("bola arroz arroz");
        inputData.add("futebol");


        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> myrdd = sc.parallelize(inputData);

        JavaRDD<String> singleWords = myrdd.flatMap(value -> Arrays.asList(value.toLowerCase().split(" ")).iterator());

        JavaPairRDD<String,Integer> tuplesRdd = singleWords.mapToPair(value -> new Tuple2<>(value,1));

        JavaPairRDD<String,Integer> countWordsRdd = tuplesRdd.reduceByKey((value1,value2)->value1+value2);

        countWordsRdd.foreach(values -> System.out.println(values._1+": "+values._2));

        sc.close();


    }
}
