package ggcd.tp1.models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * The GenreRatingPair class enable us to represent a composite type of (genre, movie, rating). To
 * persist a composite type (actually any data type) in Hadoop, it has to implement the
 * org.apache.hadoop.io.Writable interface.
 *
 * <p>To compare composite types in Hadoop, it has to implement the
 * org.apache.hadoop.io.WritableComparable interface.
 */

// EXERCISE 3

public class GenreRatingPair implements Writable, WritableComparable<GenreRatingPair> {

    private Text genre = new Text(); // natural key
    private Text name = new Text();
    private DoubleWritable rating = new DoubleWritable(); // secondary key

    public GenreRatingPair() {}

    public GenreRatingPair(String genre, String movie, double rating) {
        this.genre.set(genre);
        this.name.set(movie);
        this.rating.set(rating);
    }

    public static GenreRatingPair read(DataInput in) throws IOException {
        GenreRatingPair pair = new GenreRatingPair();
        pair.readFields(in);
        return pair;
    }

    /** This comparator controls the sort order of the keys. */
    @Override
    public int compareTo(GenreRatingPair pair) {
        int compareValue = this.genre.compareTo(pair.getGenre());
        if (compareValue == 0) {
            compareValue = -1 * rating.compareTo(pair.getRating());
        }
        // return -1*compareValue;    // to sort ascending
        return compareValue; // to sort descending
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        genre.write(dataOutput);
        name.write(dataOutput);
        rating.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        genre.readFields(dataInput);
        name.readFields(dataInput);
        rating.readFields(dataInput);
    }

    public Text getGenre() {
        return genre;
    }

    public DoubleWritable getRating() {
        return rating;
    }

    public Text getMovie() {
        return name;
    }

    public void setGenre(String genreString) {
        genre.set(genreString);
    }

    public void setRating(double ratingDouble) {
        rating.set(ratingDouble);
    }

    public void setMovie(String m) {
        name.set(m);
    }
}
