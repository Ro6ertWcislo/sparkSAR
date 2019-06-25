package demo

import java.io.File

import geotrellis.raster.{Tile, UShortRawArrayTile}
import geotrellis.spark.io.hadoop._
import geotrellis.vector.ProjectedExtent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.opencv.core.{Core, CvType, Mat}

import java.io.IOException

object Main {

  type Coords = (Double,Double)
  def helloSentence = "Hello GeoTrellis"
  val shift:Shift = None

  @throws[IOException]
  def addDir(s: String): Unit = {
    try {
      // This enables the java.library.path to be modified at runtime
      // From a Sun engineer at http://forums.sun.com/thread.jspa?threadID=707176
      //
      val field = classOf[ClassLoader].getDeclaredField("usr_paths")
      field.setAccessible(true)
      val paths = field.get(null).asInstanceOf[Array[String]]
      var i = 0
      while ( {
        i < paths.length
      }) {
        if (s == paths(i)) return

        {
          i += 1; i - 1
        }
      }
      val tmp = new Array[String](paths.length + 1)
      System.arraycopy(paths, 0, tmp, 0, paths.length)
      tmp(paths.length) = s
      field.set(null, tmp)
      System.setProperty("java.library.path", System.getProperty("java.library.path") + File.pathSeparator + s)
    } catch {
      case e: IllegalAccessException =>
        throw new IOException("Failed to get permissions to set library path")
      case e: NoSuchFieldException =>
        throw new IOException("Failed to get field handle to set library path")
    }
  }


  def main(args: Array[String]): Unit = {
    val conf = new org.apache.spark.SparkConf()
      .set("spark.executor.memory", "4g")
      .set("spark.driver.memory", "4g")
      .set("spark.driver.maxResultSize", "2g")
      .set("spark.memory.fraction", "0.6")
      .set("spark.kryoserializer.buffer.max", "1g")

    conf.setMaster("local[*]")
    implicit val sc:SparkContext = geotrellis.spark.util.SparkUtils.createSparkContext("sarDemo", conf)

    addDir("./opencv/build/lib")
    print(System.getProperty("java.library.path"))
    System.loadLibrary(Core.NATIVE_LIBRARY_NAME)


    // musi być COG tiff inaczej outofmemory
    // gdal_translate ori.tiff out.tiff -co COMPRESS=LZW -co TILED=YES

    val rdd = HadoopGeoTiffRDD.spatial("/home/robert/Downloads/tif2/measurement/test.tiff")
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

          val mat = new Mat()
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
