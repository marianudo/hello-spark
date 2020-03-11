package example

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SharedSparkContext extends BeforeAndAfterAll { self: Suite =>
  @transient private var _sc: SparkContext = _

  def sc: SparkContext = _sc

  var conf = new SparkConf(false)

  override def beforeAll(): Unit = {
    _sc = new SparkContext("local[*]", "test", conf)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    _sc.stop()
    _sc = null
    super.afterAll()
  }
}
