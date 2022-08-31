import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader;
import org.apache.sedona.core.serde.SedonaKryoRegistrator;
import org.apache.sedona.core.spatialOperator.RangeQuery;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.*;

public class SpatialQuery {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkDemo")
                .master("local")
                .config("spark.serializer", KryoSerializer.class.getName())
                .config("spark.kryo.registrator", SedonaKryoRegistrator.class.getName())
                .getOrCreate();

        SedonaSQLRegistrator.registerAll(spark);
        SedonaVizRegistrator.registerAll(spark);

        query(spark);

        spark.stop();
    }

    private static void query(SparkSession spark) throws Exception {
        String inputPath = System.getProperty("user.dir") + "/data/shp";
        SpatialRDD<Geometry> rdd = ShapefileReader.readToGeometryRDD(new JavaSparkContext(spark.sparkContext()), inputPath);

        // 构建索引
        // 如果只需要在做空间分析的时候构建索引,则设置为true
        boolean buildOnSpatialPartitionedRdd = false;
        rdd.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRdd);

        // Envelop
        Envelope rangeQueryWindow = new Envelope(-126, -107, 50, 30);
        // Only return geometries fully covered by the window
        boolean considerBoundaryIntersection = false;
        boolean usingIndex = false;
        JavaRDD<Geometry> queryResult = RangeQuery.SpatialRangeQuery(rdd, rangeQueryWindow, considerBoundaryIntersection, usingIndex);
        System.out.println("查询结果总数为: " + queryResult.count());

        // Geometry
        GeometryFactory geometryFactory = new GeometryFactory();
        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(-126, 50);
        coordinates[1] = new Coordinate(-126, 30);
        coordinates[2] = new Coordinate(-107, 30);
        coordinates[3] = new Coordinate(-107, 50);
        // The last coordinate is the same as the first coordinate in order to compose a closed ring
        coordinates[4] = coordinates[0];
        Polygon polygonObject = geometryFactory.createPolygon(coordinates);
        usingIndex = true;
        queryResult = RangeQuery.SpatialRangeQuery(rdd, polygonObject, considerBoundaryIntersection, usingIndex);
        System.out.println("查询结果总数为: " + queryResult.count());
    }
}
