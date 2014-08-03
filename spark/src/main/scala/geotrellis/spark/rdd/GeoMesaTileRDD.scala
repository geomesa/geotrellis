package geotrellis.spark.rdd

import geotrellis.raster
import geotrellis.raster._
import geotrellis.spark.TmsTile
import geotrellis.spark.metadata.Context
import geotrellis.spark.tiling.TileExtent
import geotrellis.spark.tiling.TmsTiling
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.mapreduce.lib.util.{InputConfigurator, ConfiguratorBase => CB}
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken
import org.apache.accumulo.core.data.{Value, Key, Range => ARange}
import org.apache.spark.SparkContext
import scala.collection.JavaConversions._
import java.awt.image.DataBuffer


object GeoMesaTileRDD {

  val WORLD_EPSG4326 = new org.geowebcache.grid.GridSetBroker(false, false).WORLD_EPSG4326
  val minx = WORLD_EPSG4326.getBounds.getMinX
  val miny = WORLD_EPSG4326.getBounds.getMinY
  val maxx = WORLD_EPSG4326.getBounds.getMaxX
  val maxy = WORLD_EPSG4326.getBounds.getMaxY

  def apply(table: String,
            zoom: Int,
            instance: ZooKeeperInstance,
            user: String,
            authToken: AuthenticationToken,
            sc: SparkContext): RasterRDD = {
    val grid = WORLD_EPSG4326.getGrid(zoom)
    val ctx =
      Context(zoom, TileExtent(0, 0, 256, 256), 0,
        RasterExtent(Extent(WORLD_EPSG4326.getBounds), grid.getNumTilesWide, grid.getNumTilesHigh),
        TileLayout(grid.getNumTilesWide.toInt, grid.getNumTilesHigh.toInt, 256, 256),
        raster.TypeInt, new TileIdPartitioner)
    val conf = sc.hadoopConfiguration
    CB.setConnectorInfo(classOf[AccumuloInputFormat], conf, user, authToken)
    CB.setZooKeeperInstance(classOf[AccumuloInputFormat], conf, instance.getInstanceName, instance.getZooKeepers)

    InputConfigurator.setInputTableName(classOf[AccumuloInputFormat], conf, table)
    InputConfigurator.setRanges(classOf[AccumuloInputFormat], conf, List(new ARange()))

    val rdd = sc.newAPIHadoopRDD(conf, classOf[AccumuloInputFormat], classOf[Key], classOf[Value])
    rdd.mapPartitions { partition =>
      val keyRegex = ".*EPSG_4326_(\\p{Digit}{2})[^/]*/[^/]*/(\\p{Digit}{1,})_(\\p{Digit}{1,})\\.png".r
      partition.map { case (key, value) =>
        val row = key.getRow
        val keyRegex(z, x, y) = row.toString
        val img = ImageIO.read(new ByteArrayInputStream(value.get()))
        val cellType = img.getData.getDataBuffer.getDataType
        val raster = img.getRaster.getDataElements(0, 0, img.getWidth, img.getHeight, null)
        val tile = cellType match {
          case DataBuffer.TYPE_DOUBLE => ArrayTile(castRaster[Double](raster), 256, 256)
          case DataBuffer.TYPE_FLOAT  => ArrayTile(castRaster[Float](raster), 256, 256)
          case DataBuffer.TYPE_INT    => ArrayTile(castRaster[Int](raster), 256, 256)
          case DataBuffer.TYPE_SHORT  => ArrayTile(castRaster[Short](raster), 256, 256)
          case DataBuffer.TYPE_BYTE   => ArrayTile(castRaster[Byte](raster), 256, 256)
          case _          => sys.error("Unrecognized AWT type - " + cellType)
        }
        new TmsTile(TmsTiling.tileId(x.toLong, y.toLong, z.toInt), tile)
      }
    }.withContext(ctx)
  }

  def castRaster[T](v: AnyRef) = v.asInstanceOf[Array[T]]

}
