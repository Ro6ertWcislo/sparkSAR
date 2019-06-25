package demo

import geotrellis.raster.{Tile, UShortRawArrayTile}
import geotrellis.spark.io.hadoop._
import geotrellis.vector.ProjectedExtent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.opencv.core.{Core, CvType, Mat}


object Main {

  type Coords = (Double,Double)
  def helloSentence = "Hello GeoTrellis"
  val shift:Shift = None


  def main(args: Array[String]): Unit = {
    print(System.getProperty("java.library.path"))
    val conf = new org.apache.spark.SparkConf()
      .set("spark.executor.memory", "4g")
      .set("spark.driver.memory", "4g")
      .set("spark.driver.maxResultSize", "2g")
      .set("spark.memory.fraction", "0.6")
      .set("spark.kryoserializer.buffer.max", "1g")

    conf.setMaster("local[*]")
    implicit val sc:SparkContext = geotrellis.spark.util.SparkUtils.createSparkContext("sarDemo", conf)
    System.loadLibrary(Core.NATIVE_LIBRARY_NAME)


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
      case None =>

        val tile = rdd.map(_._2.asInstanceOf[UShortRawArrayTile]).map { tile:Tile =>
          val rows = tile.rows
          val cols = tile.cols

          val mat: Mat = new Mat()
          mat.create(rows, cols, CvType.CV_16U)

          for (row <- 0 until rows) {
            for (col <- 0 until cols) {
              mat.put(row, col, tile.get(col, row).asInstanceOf[Short])
            }
          }

          mat
        }.count()
        println(tile)
    }
    println("\nMy watch has ended")



  }
}
