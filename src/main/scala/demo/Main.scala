package demo

import geotrellis.raster.Tile
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.vector.ProjectedExtent
import org.apache.spark.SparkContext

import scala.reflect.io.Path

object Main {
  def helloSentence = "Hello GeoTrellis"

  def main(args: Array[String]): Unit = {
    val conf = new org.apache.spark.SparkConf()
      .set("spark.executor.memory", "4096m")
      .set("spark.memory.fraction", "0.2")
    conf.setMaster("local[*]")
    implicit val sc:SparkContext = geotrellis.spark.util.SparkUtils.createSparkContext("sarDemo", conf)

    // musi być COG tiff inaczej outofmemory
    // gdal_translate ori.tiff out.tiff -co COMPRESS=LZW -co TILED=YES
    val rdd = HadoopGeoTiffRDD.spatial("hdfs://localhost:9000/geotiffs/cogtest.tif")
    println(rdd)

    rdd.foreach(t => {
      val (projectedExtent, tile) = t
      println("tile " + projectedExtent.toString + " " + tile.rows)
    })

    println("\nMy watch has ended")

  }
}
