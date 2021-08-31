package ggcd.tp1.models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;

public class Movie implements WritableComparable<Movie> {
    private Text movieId;
    private Text title;
    private LongWritable year;
    private DoubleWritable averageRating;
    private LongWritable numVotes;

    public Movie() {
        this.movieId = new Text();
        this.title = new Text();
        this.year = new LongWritable();
        this.averageRating = new DoubleWritable();
        this.numVotes = new LongWritable();
    }

    public Movie(String movieId, String title, long year, long numVotes) {
        this.movieId = new Text(movieId);
        this.title = new Text(title);
        this.year = new LongWritable(year);
        this.numVotes = new LongWritable(numVotes);
        this.averageRating = new DoubleWritable(0.0);
    }

    public Movie(String movieId, String title, long year, double rating) {
        this.movieId = new Text(movieId);
        this.title = new Text(title);
        this.year = new LongWritable(year);
        this.numVotes = new LongWritable(0);
        this.averageRating = new DoubleWritable(rating);
    }

    private Movie(Movie m) {
        this(
                m.getMovieId().toString(),
                m.getTitle().toString(),
                m.getYear().get(),
                m.getAverageRating().get());
    }

    public Text getMovieId() {
        return movieId;
    }

    public Text getTitle() {
        return title;
    }

    public LongWritable getYear() {
        return year;
    }

    public DoubleWritable getAverageRating() {
        return averageRating;
    }

    public LongWritable getNumVotes() {
        return numVotes;
    }

    @Override
    public boolean equals(Object b) {
        Movie p = (Movie) b;
        return p.getMovieId().toString().equals(this.getMovieId().toString());
    }

    @Override
    public int hashCode() {
        return this.getMovieId().toString().hashCode();
    }

    @Override
    public String toString() {
        return this.getMovieId().toString();
    }

    @Override
    public int compareTo(Movie o) {
        return this.getMovieId().toString().compareTo(o.getMovieId().toString());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        movieId.write(dataOutput);
        title.write(dataOutput);
        year.write(dataOutput);
        averageRating.write(dataOutput);
        numVotes.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        movieId.readFields(dataInput);
        title.readFields(dataInput);
        year.readFields(dataInput);
        averageRating.readFields(dataInput);
        numVotes.readFields(dataInput);
    }

    @Override
    public Movie clone() {
        return new Movie(this);
    }
}
