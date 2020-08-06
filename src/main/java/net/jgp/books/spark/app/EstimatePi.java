package net.jgp.books.spark.app;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import net.jgp.books.spark.mapper.DartMapper;
import net.jgp.books.spark.mapper.DartReducer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class EstimatePi {

    public static void main(String[] args) {
        EstimatePi pi = new EstimatePi();
        pi.start(10);
    }

    private void start(int slices) {
        int numberOfThrows = 100000 * slices;
        System.out.println("About to throw " + numberOfThrows + " darts, ready? Stay away from the target!");

        Stopwatch stopwatch = Stopwatch.createStarted();
        SparkSession session = SparkSession.builder()
                .appName("Spark pi")
                .master("local[*]")
                .getOrCreate();
        long duration = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        System.out.println("Session initialized in " + duration + "ms");

        // 创建整型数据集
        List<Integer> listOfRows = Lists.newArrayListWithExpectedSize(numberOfThrows);
        for (int i = 0; i < numberOfThrows; i++) {
            listOfRows.add(i);
        }
        Dataset<Row> incrementalDF = session.createDataset(listOfRows, Encoders.INT())
                .toDF();
        long end = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        System.out.println("Initial dataframe built in " + (end - duration) + "ms");

        // 映射数据
        Dataset<Integer> dartsDs = incrementalDF.map(new DartMapper(0), Encoders.INT());
        long dartsDone = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        System.out.println("Throwing darts done in " + (dartsDone - end) + "ms");

        // 合并数据
        int dartsInCircle = dartsDs.reduce(new DartReducer());
        long analyzingDone = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        System.out.println("Analyzing result in " + (analyzingDone - dartsDone) + "ms");

        System.out.println("Pi is roughly " + 4.0 * dartsInCircle / numberOfThrows);

        session.stop();
    }
}
