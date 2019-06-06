package demo
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff._
import geotrellis.raster.Tile
import geotrellis.spark.io._
import geotrellis.spark.{LayerId, Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.spark.io.FilteringLayerReader
import geotrellis.spark.io.file.FileLayerReader
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def helloSentence = "Hello GeoTrellis"

  def main(args: Array[String]): Unit = {
    val conf = new org.apache.spark.SparkConf()
    conf.setMaster("local[*]")
    implicit val sc:SparkContext = geotrellis.spark.util.SparkUtils.createSparkContext("sarDemo", conf)

    val path: String = "/home/robert/Downloads/tif2/measurement/s1b-iw-grd-vv-20190605t075801-20190605t075818-016558-01f2c1-001.tiff"
    val geoTiff: SinglebandGeoTiff = GeoTiffReader.readSingleband(path)
    print(geoTiff)
  }
}
