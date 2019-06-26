package demo

import java.io.{File, IOException}

import com.typesafe.config.Config
import geotrellis.spark.io.hadoop._
import geotrellis.vector.Extent
import org.apache.spark.SparkContext
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.opencv.core._
import org.opencv.features2d.MSER

import scala.collection.JavaConverters._

object Main {

  type Coords = (Double, Double)

  def helloSentence = "Hello GeoTrellis"



  import com.typesafe.config.ConfigFactory
  val config: Config = ConfigFactory.load("sar.conf")
  val shift: Shift = Shift.apply(config.getString("shift"))
  var square: Boolean = config.getBoolean("square") //return squares containing mser or just points made of average mser points
  var boudExtent: Extent = if(config.getBoolean("extent.limit")){
    new Extent(
    config.getInt("extent.xmin"),
      config.getInt("extent.ymin"),
      config.getInt("extent.xmax"),
      config.getInt("extent.ymax")
  )
  }
  else null
  val uri: String = config.getString("uri")

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
          i += 1;
          i - 1
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
    implicit val sc: SparkContext = geotrellis.spark.util.SparkUtils.createSparkContext("sarDemo", conf)

    addDir("./opencv/build/lib")
    print(System.getProperty("java.library.path"))
    System.loadLibrary(Core.NATIVE_LIBRARY_NAME)


    // musi być COG tiff inaczej outofmemory
    // gdal_translate ori.tiff out.tiff -co COMPRESS=LZW -co TILED=YES

    val rdd = HadoopGeoTiffRDD.spatial(uri)
    println(rdd)



    // not enough memory to handle all possibilities at once so why not handle it in 3 distinct runs?
    val shiftedRDD = shift match {
      case Horizontal =>
        rdd.sortBy { case (extent, _) => (extent.ymax, extent.xmax) }.sliding(2).flatMap {
          case Array(head, tail) => head.horizontalMerge(tail)
        }
      case Vertical =>
        rdd.sortBy { case (extent, _) => (extent.xmax, extent.ymax) }.sliding(2).flatMap {
          case Array(head, tail) => head.verticalMerge(tail)
        }
      case None =>
        rdd
    }
    shiftedRDD
      .filter {
        case (extent, tile) =>
          if (boudExtent != null)
            boudExtent.interiorIntersects(extent.extent)
          else true
      }
      .map { case (extent, tile) =>
        val rows = tile.rows
        val cols = tile.cols

        val mat = new Mat()
        mat.create(rows, cols, CvType.CV_8UC1)

        val msers = new java.util.ArrayList[MatOfPoint]
        val bboxes = new MatOfRect

        // fill the mat with tile's values
        for (row <- 0 until rows) {
          for (col <- 0 until cols) {
            mat.put(row, col, tile.get(col, row).asInstanceOf[Short])
          }
        }


        val mser = MSER.create()
        mser.detectRegions(mat, msers, bboxes)
        val size = msers.size()


        val result = if (square) {
          msers.asScala.map { matOfPoint =>
            val enclosedMser = encloseMserInSquare(matOfPoint)
            toRealExtent(enclosedMser, extent.extent, cols, rows)
          }
        }
        else {
          msers.asScala.map { matOfPoint =>
            val avgPoint = minMaxPoint(matOfPoint)
            toRealPoint(avgPoint, extent.extent, cols, rows)
          }
        }

        // anti-SIGSEGV stuff
        mat.release()
        bboxes.release()
        for (i <- 0 until size) {
          msers.get(i).release()
        }
        mser.clear()

        result
      }.flatMap(x => x.toList).collect().foreach {
      case ex: Extent => println(ex)
      case p: Point => println(p)
    }

  }

  println("\nMy watch has ended")


  private def minMaxPoint(matOfPoint: MatOfPoint): Point = {
    val points = matOfPoint.toArray
    val size = points.length
    var xsum = 0.0
    var ysum = 0.0

    for (point <- points) {
      xsum += point.x
      ysum += point.y
    }
    new Point(xsum / size, ysum / size)
  }


  private def toRealPoint(avgPoint: Point, tileExtent: Extent, cols: Int, rows: Int): Point = {
    new Point(
      toRealCoordX(tileExtent, avgPoint.x, cols),
      toRealCoordY(tileExtent, avgPoint.y, rows)
    )
  }

  private def toRealCoordX(tileExtent: Extent, posInTile: Double, cols: Int) =
    tileExtent.xmin + (posInTile / cols) * tileExtent.width

  private def toRealCoordY(tileExtent: Extent, posInTile: Double, rows: Int) =
    tileExtent.ymin + (posInTile / rows) * tileExtent.height


  private def toRealExtent(mserSquare: Extent, tileExtent: Extent, cols: Int, rows: Int): Extent = {
    Extent(
      toRealCoordX(tileExtent, mserSquare.xmin, cols),
      toRealCoordY(tileExtent, mserSquare.ymin, rows),
      toRealCoordX(tileExtent, mserSquare.xmax, cols),
      toRealCoordY(tileExtent, mserSquare.ymax, rows)
    )
  }

  private def encloseMserInSquare(matOfPoint: MatOfPoint): Extent = {
    val points = matOfPoint.toArray
    var xmin = Double.MaxValue
    var xmax = Double.MinValue
    var ymin = Double.MaxValue
    var ymax = Double.MinValue

    for (point <- points) {
      xmax = math.max(xmax, point.x)
      xmin = math.min(xmin, point.x)

      ymax = math.max(ymax, point.y)
      ymin = math.min(ymin, point.y)
    }
    Extent(
      xmin,
      ymin,
      xmax,
      ymax
    )
  }

}



