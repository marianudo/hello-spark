package example

import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class WordCountSuite extends FunSuite with SharedSparkContext {
  test("testFilterWordCount") {
    // Fixture
    val inputRDD = sc.parallelize(Array("the cat and the hat", "the car is blue", "the cat is in a car", "the cat lost his hat"))

    // Action
    val filteredWordCount: RDD[(String, Int)] = SparkWordCount.wordCount(inputRDD)

    // Assertions
    val map = filteredWordCount.collect().toMap
    assert(map.getOrElse("cat", 0) == 3)
    assert(map.getOrElse("the", 0) == 5)
    assert(map.getOrElse("hat", 0) == 2)
    assert(map.getOrElse("and", 0) == 1)
    assert(map.getOrElse("is", 0) == 2)
  }
}
