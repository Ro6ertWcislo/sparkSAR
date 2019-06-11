import geotrellis.proj4.CRS
import geotrellis.raster.{IntRawArrayTile, ShortRawArrayTile, Tile, UShortCellType, UShortRawArrayTile}
import geotrellis.vector.{Extent, ProjectedExtent}

import math.{max, min}

package object demo {



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
    def height: Double = t._1.extent.height
    def width: Double = t._1.extent.width

    def shortTile:Tile = t._2.convert(UShortCellType)

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
          val arr = t.shortTile.crop(t._1.extent,new Extent(avg(t.xmin,t.xmax),t.ymin,t.xmax,t.ymax)).toArray() ++
            t2.shortTile.crop(t2._1.extent,new Extent(avg(t2.xmin,t2.xmax),t2.ymin,t2.xmax,t2.ymax)).toArray()
          return Array(t,(newExtent,IntRawArrayTile(arr,2,2).convert(UShortCellType)))
        }
      }
      Array(t)
    }
  }


}