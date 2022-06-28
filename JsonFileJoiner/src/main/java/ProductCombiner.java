import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

public class ProductCombiner {
    public static void main(String[] args)throws Exception {
        run(args);
    }
    public static void run(String args[]) {
        SparkSession sess = SparkSession.builder().appName("json-file-joiner").getOrCreate();

        String outputPath = args[4];;

        Dataset<Row> ds1 = sess.read().json(args[0]);
        Dataset<Row> ds2 = sess.read().json(args[1]);
        Dataset<Row> ds3 = sess.read().json(args[2]);
        Dataset<Row> ds4 = sess.read().json(args[3]);

        Dataset<Row> ds2_2 = ds2.withColumnRenamed("product_id", "join_id");
        Dataset<Row> ds1_2 = ds1.join(ds2_2,ds1.col("product_id").equalTo(ds2_2.col("join_id"))).drop("join_id");

        Dataset<Row> ds4_2 = ds4.withColumnRenamed("product_id", "join_id");
        Dataset<Row> ds3_2 = ds3.join(ds4_2,ds3.col("product_id").equalTo(ds4_2.col("join_id"))).drop("join_id");
        
        Dataset<Row> ds = ds3_2.withColumnRenamed("product_id", "join_id");
        Dataset<Row> finalData = ds1_2.join(ds,ds1_2.col("product_id").equalTo(ds.col("join_id"))).drop("join_id");

        finalData.write().mode(SaveMode.Overwrite).json(outputPath);
    }
}
