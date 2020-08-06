package net.jgp.books.spark.mapper;

import org.apache.spark.api.java.function.ReduceFunction;

public class DartReducer implements ReduceFunction<Integer> {

    private static final long serialVersionUID = 12859L;

    @Override
    public Integer call(Integer x, Integer y) throws Exception {
        return x + y;
    }
}
