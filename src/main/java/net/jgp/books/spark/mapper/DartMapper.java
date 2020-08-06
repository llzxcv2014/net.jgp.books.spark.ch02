package net.jgp.books.spark.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

public class DartMapper implements MapFunction<Row, Integer> {

    private static final long serialVersionUID = 38446L;

    private int counter;

    public DartMapper(int counter) {
        this.counter = counter;
    }

    @Override
    public Integer call(Row value) throws Exception {
        double x = Math.random() * 2 - 1;
        double y = Math.random() * 2 - 1;

        counter++;

        if (counter % 100000 == 0) {
            System.out.println("" + counter + " darts thrown so far");
        }
        return ((x * x + y * y) <= 1) ? 1 : 0;
    }
}
