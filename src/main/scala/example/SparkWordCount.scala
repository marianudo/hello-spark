package example

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD

object SparkWordCount {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "Word Count", "/usr/local/bin")
    val input = sc.textFile("input.txt")
    val count = wordCount(input)
    count.saveAsTextFile("outfile")
    System.out.println("OK");
  }

  def wordCount(phrases: RDD[String]): RDD[(String, Int)] = {
    phrases.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
  }
}
