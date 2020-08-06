package net.jgp.books.spark.mapper;

import net.jgp.books.spark.model.Book;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.text.SimpleDateFormat;

public class BookMapper implements MapFunction<Row, Book> {

    @Override
    public Book call(Row value) throws Exception {
        Book b = new Book();
        b.setId(value.getAs("id"));
        b.setAuthorId(value.getAs("authorId"));
        b.setLink(value.getAs("link"));
        b.setTitle(value.getAs("title"));

        String dateAsString = value.getAs("releaseDate");

        if (dateAsString != null) {
            SimpleDateFormat parser = new SimpleDateFormat("M/d/yy");
            b.setReleaseDate(parser.parse(dateAsString));
        }
        return b;
    }
}
