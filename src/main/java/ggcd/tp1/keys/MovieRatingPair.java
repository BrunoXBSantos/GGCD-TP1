package ggcd.tp1.keys;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class MovieRatingPair implements Writable, WritableComparable<MovieRatingPair> {

    private Text movieId;
    private DoubleWritable rating;

    public MovieRatingPair() {}

    public MovieRatingPair(String id, double rat) {
        this.movieId = new Text(id);
        this.rating = new DoubleWritable(rat);
    }

    public Text getMovieId() {
        return movieId;
    }

    public DoubleWritable getRating() {
        return rating;
    }

    @Override
    public int compareTo(MovieRatingPair o) {
        int titval = this.getMovieId().toString().compareTo(o.getMovieId().toString());
        int ratcomp = Double.compare(this.getRating().get(), o.getRating().get());
        int compval = titval;

        if (compval != 0) {
            if (ratcomp != 0) {
                compval = ratcomp;
            }
        }
        return compval;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        movieId.write(dataOutput);
        rating.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        movieId.readFields(dataInput);
        rating.readFields(dataInput);
    }
}
