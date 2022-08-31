import org.apache.sedona.core.formatMapper.GeoJsonReader;
import org.apache.sedona.core.serde.SedonaKryoRegistrator;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;

public class GeojsonReader {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkDemo")
                .master("local")
                .config("spark.serializer", KryoSerializer.class.getName())
                .config("spark.kryo.registrator", SedonaKryoRegistrator.class.getName())
                .getOrCreate();

        SedonaSQLRegistrator.registerAll(spark);
        SedonaVizRegistrator.registerAll(spark);

        readGeojson(spark);

        spark.stop();
    }

    private static void readGeojson(SparkSession spark) {
        String inputPath = System.getProperty("user.dir") + "/data/geojson/demo.geojson";

        boolean allowTopologyInvalidGeometries = true;
        boolean skipSyntaxInvalidGeometries = false;
        SpatialRDD<Geometry> rdd = GeoJsonReader.readToGeometryRDD(new JavaSparkContext(spark.sparkContext()), inputPath, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries);

        Dataset<Row> rawDf = Adapter.toDf(rdd, spark);
        rawDf.show();
        rawDf.printSchema();
    }
}
