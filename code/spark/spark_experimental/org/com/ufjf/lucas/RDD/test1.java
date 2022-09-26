package org.com.ufjf.lucas.RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class test1 {
    public static void main(String[] args) {

        List<Integer> inputData = new ArrayList<Integer>();
        inputData.add(10);
        inputData.add(10000);
        inputData.add(90);
        inputData.add(20);


        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<Integer> myrdd = sc.parallelize(inputData);

        Integer result = myrdd.reduce((value1,value2) -> value1 + value2);

        JavaRDD<Double> sqrtRdd = myrdd.map(value -> Math.sqrt(value));

        sqrtRdd.foreach( value -> System.out.println(value) );

        JavaRDD<Integer> singeIntegerRdd = sqrtRdd.map(value -> 1);

        System.out.println(singeIntegerRdd.reduce((value1, value2) -> value1+value2));

        System.out.println(result);

        sc.close();


    }
}
