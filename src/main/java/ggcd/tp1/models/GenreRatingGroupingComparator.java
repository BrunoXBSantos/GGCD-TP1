package ggcd.tp1.models;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * The GenreRatingGroupingComparator class enable us to compare two GenreRatingPair objects. This
 * class is needed for sorting purposes.
 */
public class GenreRatingGroupingComparator extends WritableComparator {

    public GenreRatingGroupingComparator() {
        super(GenreRatingPair.class, true);
    }

    public int compare(WritableComparable wc1, WritableComparable wc2) {
        GenreRatingPair pair = (GenreRatingPair) wc1;
        GenreRatingPair pair2 = (GenreRatingPair) wc2;
        return pair.getGenre().compareTo(pair2.getGenre());
    }
}
