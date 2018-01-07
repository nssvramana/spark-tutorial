package com.myapp.myspark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word Count").setMaster("local")
    val sc = new SparkContext(conf);
    //System.setProperty("hadoop.home.dir", "N:\\GOPAL HADOOP MATERIALS\\Winutils\\");
    //val input = sc.textFile("empdata.txt")
    val input = sc.textFile("/home/gopalkrishna/empdata.txt")
   val count = input.flatMap(line => line.split(" ")).map(word =>(word,1)).reduceByKey(_ + _).saveAsTextFile("/home/gopalkrishna/myoutputFile")
   
   //count.foreach(println);
       //  count.saveAsTextFile("outputFile")
    
  }
}