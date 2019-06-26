import geotrellis.proj4.CRS
import geotrellis.raster.{IntRawArrayTile, ShortRawArrayTile, Tile, UShortCellType, UShortRawArrayTile}
import geotrellis.vector.{Extent, ProjectedExtent}

import math.{max, min}

package object demo {

  trait Shift
  object Shift{
    def apply(s:String): Shift ={
      s match {
        case "None" => None
        case "Horizontal" => Horizontal
        case "Vertical" => Vertical
      }
    }
  }

  final case object Horizontal extends Shift

  final case object Vertical extends Shift
  final case object None extends Shift

  implicit class ProjectedExtentOps(val projectedExtent: ProjectedExtent) {
    def xmax: Double = {
      projectedExtent.extent.xmax
    }

    def ymax: Double = {
      projectedExtent.extent.ymax
    }
  }

  implicit class tupleOps(val t: (ProjectedExtent, Tile)) {
    def xmin: Double = t._1.extent.xmin

    def xmax: Double = t._1.extent.xmax

    def ymin: Double = t._1.extent.ymin

    def ymax: Double = t._1.extent.ymax

    def rows: Int = t._2.rows

    def cols: Int = t._2.cols

    def shortTile: Tile = t._2.convert(UShortCellType)

    def avg(x: Double, y: Double): Double = (x + y) / 2

    def crs: CRS = t._1.crs


    def horizontalMerge(t2: (ProjectedExtent, Tile)): Array[(ProjectedExtent, Tile)] = {
      if (t.ymax == t2.ymax && t.ymin == t2.ymin) { // otherwise they're from different rows
        val x1 = avg(t.xmin, t.xmax)
        val x2 = avg(t2.xmin, t2.xmax)
        val newExtent = ProjectedExtent(
          new Extent(min(x1, x2), ymin, max(x1, x2), ymax),
          crs
        )
        if (t._2.size == t2._2.size) {
          val arr = t.shortTile.crop(t._1.extent, new Extent(avg(t.xmin, t.xmax), t.ymin, t.xmax, t.ymax)).toArray() ++
            t2.shortTile.crop(t2._1.extent, new Extent(avg(t2.xmin, t2.xmax), t2.ymin, t2.xmax, t2.ymax)).toArray()
          return Array((newExtent, IntRawArrayTile(arr,t.cols,t.rows).convert(UShortCellType)))
        }
      }
      Array(t)
    }

    def verticalMerge(t2: (ProjectedExtent, Tile)): Array[(ProjectedExtent, Tile)] = {
      if (t.xmax == t2.xmax && t.xmin == t2.xmin) { // otherwise they're from different columns
        val y1 = avg(t.ymin, t.ymax)
        val y2 = avg(t2.ymin, t2.ymax)
        val newExtent = ProjectedExtent(
          new Extent(xmin, min(y1, y2), xmax, max(y1, y2)),
          crs
        )
        if (t._2.size == t2._2.size) {
          val arr = t.shortTile.crop(t._1.extent, new Extent(t.xmin, avg(t.ymin,t.ymax), t.xmax, t.ymax)).toArray() ++
            t2.shortTile.crop(t2._1.extent, new Extent(t2.xmin, avg(t2.ymin,t2.ymax), t2.xmax, t2.ymax)).toArray()
          return Array((newExtent, IntRawArrayTile(arr,t.cols,t.rows).convert(UShortCellType)))
        }
      }
      Array(t)
    }
  }


}
