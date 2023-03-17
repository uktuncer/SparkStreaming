import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

public class Application {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        System.setProperty("haddop.home.dir","C:\\hadoop-common-2.2.0-bin-master");

        SparkSession sparkSession=SparkSession.builder().appName("SparkStreamingMessageListener").master("local").getOrCreate();

        Dataset<Row> rawData = sparkSession.readStream().format("socket").option("host", "localhost").option("port", "8005").load();

        Dataset<String> data = rawData.as(Encoders.STRING());

        Dataset<String> stringDataset = data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        }, Encoders.STRING());

        Dataset<Row> groupedData = stringDataset.groupBy("value").count();

        StreamingQuery start = groupedData.writeStream().outputMode("complate").format("console").start();

        start.awaitTermination();
    }
}
