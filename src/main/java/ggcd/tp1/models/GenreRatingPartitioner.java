package ggcd.tp1.models;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * The GenreRatingPartitioner is a custom partitioner class, which partitions data by the natural
 * key only (using the genre). Without custom partitioner, Hadoop will partition your mapped data
 * based on a hash code.
 *
 * <p>In Hadoop, the partitioning phase takes place after the map() phase and before the reduce()
 * phase
 */
public class GenreRatingPartitioner extends Partitioner<GenreRatingPair, Text> {

    @Override
    public int getPartition(GenreRatingPair genreRatingPair, Text text, int numerOfPartitions) {
        return Math.abs(genreRatingPair.getGenre().hashCode() % numerOfPartitions);
    }
}
