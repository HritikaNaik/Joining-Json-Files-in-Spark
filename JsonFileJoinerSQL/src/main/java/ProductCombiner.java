
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

public class ProductCombiner {
    public static void main(String[] args) throws Exception {
        run(args);
    }

    public static void run(String args[]) {
        SparkSession sess = SparkSession.builder().appName("json-file-joiner").getOrCreate(); //Make the session of spark

        String outputPath = args[4]; //Get the output path from the arguments

        //Read all the inputs from paths given in the arguments
        Dataset<Row> input1 = sess.read().json(args[0]);
        Dataset<Row> input2 = sess.read().json(args[1]);
        Dataset<Row> input3 = sess.read().json(args[2]);
        Dataset<Row> input4 = sess.read().json(args[3]);

        //Create views from the input datasets that can be used as hive tables in Spark SQL
        input1.createOrReplaceTempView("data1");
        input2.createOrReplaceTempView("data2");
        input3.createOrReplaceTempView("data3");
        input4.createOrReplaceTempView("data4");

        Dataset<Row> finalData = sess.sql("SELECT * FROM data1 JOIN data2 USING (product_id) JOIN data3 USING (product_id) JOIN data4 USING (product_id)");

        //Overwrite while saving incase there is already an output of a previous job at the given location
        finalData.write().mode(SaveMode.Overwrite).json(outputPath);
    }
}
