import org.apache.commons.lang.StringUtils;
import org.apache.sedona.core.enums.FileDataSplitter;
import org.apache.sedona.core.serde.SedonaKryoRegistrator;
import org.apache.sedona.core.spatialRDD.PointRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;

public class CsvReader {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkDemo");
        conf.setMaster("local");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        conf.set("spark.kryo.registrator", SedonaKryoRegistrator.class.getName());

        JavaSparkContext sc = new JavaSparkContext(conf);

        String csvPath = System.getProperty("user.dir") + "/data/csv/demo.csv";

        // 经纬度从第0列开始
        Integer pointRddOffset = 0;
        FileDataSplitter pointRddSplitter = FileDataSplitter.CSV;

        // 属性
        boolean carryOtherAttributes = true;
        PointRDD rdd = new PointRDD(sc, csvPath, pointRddOffset, pointRddSplitter, carryOtherAttributes);
        rdd.rawSpatialRDD.foreach((point -> {
            String[] attrs = point.getUserData().toString().split("\t");
            System.out.println(StringUtils.join(attrs, "|"));
        }));

        rdd.analyze();

        System.out.println(rdd.approximateTotalCount);
        rdd.rawSpatialRDD.collect().forEach(System.out::println);

        // 坐标系转换
        String sourceCrsCode = "epsg:4326";
        String targetCrsCode = "epsg:3857";
        rdd.CRSTransform(sourceCrsCode, targetCrsCode, false);
        rdd.rawSpatialRDD.collect().forEach(System.out::println);

        sc.stop();
    }
}
