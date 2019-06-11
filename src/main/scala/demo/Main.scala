package demo

import geotrellis.raster.Tile
import geotrellis.spark.io.hadoop._
import geotrellis.vector.ProjectedExtent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.RDDFunctions._


object Main {

  type Coords = (Double,Double)
  def helloSentence = "Hello GeoTrellis"
  val shift:Shift = Horizontal


  def main(args: Array[String]): Unit = {
    val conf = new org.apache.spark.SparkConf()
      .set("spark.executor.memory", "4g")
      .set("spark.driver.memory", "4g")
      .set("spark.driver.maxResultSize", "2g")
      .set("spark.memory.fraction", "0.6")
      .set("spark.kryoserializer.buffer.max", "1g")

    conf.setMaster("local[*]")
    implicit val sc:SparkContext = geotrellis.spark.util.SparkUtils.createSparkContext("sarDemo", conf)

    // musi być COG tiff inaczej outofmemory
    // gdal_translate ori.tiff out.tiff -co COMPRESS=LZW -co TILED=YES

    val rdd: RDD[(ProjectedExtent,Tile)] = HadoopGeoTiffRDD.spatial("/home/robert/Downloads/tif2/measurement/test.tiff")
    println(rdd)

    // not enough memory to handle all possibilities at once so why not handle it in 3 distinct runs?
    shift match{
      case Horizontal =>
        rdd.sortBy{case (extent,_) => (extent.ymax,extent.xmax)}.sliding(2).flatMap{
          case Array(head,tail) => head.horizontalMerge(tail)
        }.collect()
      case Vertical =>
        rdd.sortBy{case (extent,_) => (extent.xmax,extent.ymax)}.sliding(2).flatMap{
          case Array(head,tail) => head.verticalMerge(tail)
        }.collect()
      case None => println(rdd.count())
    }
    println("\nMy watch has ended")

  }
}
