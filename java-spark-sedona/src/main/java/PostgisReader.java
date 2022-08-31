import org.apache.sedona.core.serde.SedonaKryoRegistrator;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PostgisReader {
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

        readPostgis(spark);

        spark.stop();
    }

    private static void readPostgis(SparkSession spark) {
        Dataset<Row> rawDf = spark.read().format("jdbc")
                .option("driver", "org.postgresql.Driver")
                .option("url", "jdbc:postgresql://10.181.41.7:5432/demo")
                .option("dbtable", "geometries")
                .option("user", "postgres")
                .option("password", "bigdata@ft2021")
                .load();

        rawDf.show();
        rawDf.createOrReplaceTempView("pgsource");

        // 从pg读出的geom是wkb格式，转换geom格式
        Dataset<Row> geomDf = spark.sql("select name, st_geomfromwkb(geom) as geom from pgsource");
        geomDf.show();
        geomDf.createOrReplaceTempView("pgsource");
    }
}
