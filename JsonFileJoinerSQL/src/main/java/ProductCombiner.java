
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

public class ProductCombiner {
    public static void main(String[] args) throws Exception {
        run(args);
    }

    public static void run(String args[]) throws Exception {
        SparkSession sess = SparkSession.builder().appName("json-file-joiner").getOrCreate();

        String outputPath = args[4];
        ;

        Dataset<Row> ds1 = sess.read().json(args[0]);
        Dataset<Row> ds2 = sess.read().json(args[1]);
        //Dataset<Row> data3 = sess.read().option("multiline","true").json(args[2]);
        Dataset<Row> ds4 = sess.read().json(args[3]);

        ds1.createOrReplaceTempView("d1");
        ds2.createOrReplaceTempView("d2");
        ds4.createOrReplaceTempView("d4");

        Dataset<Row> finalData = sess.sql("SELECT * FROM ds1 JOIN ds2 USING (product_id) JOIN ds4 USING (product_id)");
        //interData.createOrReplaceTempView("iD");
        //Dataset<Row> finalData = sess.sql("SELECT * FROM iD JOIN d4 USING (product_id)");

        finalData.write().mode(SaveMode.Overwrite).json(outputPath);
    }
}