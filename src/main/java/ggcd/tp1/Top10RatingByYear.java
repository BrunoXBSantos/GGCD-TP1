package ggcd.tp1;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import ggcd.tp1.keys.MovieRatingPair;
import ggcd.tp1.models.Movie;
import ggcd.tp1.utils.MovieHandler;
import ggcd.tp1.utils.SchemaHandler;
import java.io.IOException;
import java.util.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;

@Parameters(commandDescription = "Top 10 ranking movies per year")
public final class Top10RatingByYear {
    private static final Top10RatingByYear SINGLETON = new Top10RatingByYear();

    private Top10RatingByYear() {}

    public static Top10RatingByYear of() {
        return SINGLETON;
    }

    @Parameter(
            names = {"--input"},
            description = "Path of input avro+parquet file",
            required = true)
    private String input;

    @Parameter(
            names = {"--output"},
            description = "Path of output avro+parquet file")
    private String output = ggcd.tp1.config.Configuration.OUTPUT_DIR;

    public static final class Top10RatingByYearMapper
            extends Mapper<Void, GenericRecord, LongWritable, Movie> {
        private Map<Long, TreeMap<MovieRatingPair, Movie>> yearlyRanking;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.yearlyRanking = new HashMap<>();
        }

        @Override
        protected void map(final Void key, final GenericRecord value, final Context context)
                throws IOException, InterruptedException {
            Long year = MovieHandler.getStartYear(value);
            Double rating = MovieHandler.getRating(value);
            if (!(MovieHandler.hasTitleType(value, "movie")
                    && year != null
                    && rating != null
                    && value.get("tconst") != null)) return;

            TreeMap<MovieRatingPair, Movie> rank =
                    yearlyRanking.computeIfAbsent(year, k -> new TreeMap<>());

            rank.put(
                    new MovieRatingPair(value.get("tconst").toString(), rating),
                    new Movie(
                            value.get("tconst").toString(),
                            value.get("primaryTitle").toString(),
                            year,
                            rating));

            if (rank.size() > 10) {
                rank.remove(rank.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Long, TreeMap<MovieRatingPair, Movie>> entry :
                    yearlyRanking.entrySet()) {
                LongWritable year = new LongWritable(entry.getKey());
                Collection<Movie> rank = entry.getValue().values();

                for (Movie mov : rank) {
                    context.write(year, mov);
                }
            }
        }
    }

    public static final class Top10RatingByYearReducer
            extends Reducer<LongWritable, Movie, Void, GenericRecord> {
        private Schema outputSchema;
        private Schema rankEntrySchema;

        @Override
        protected void setup(final Context context) throws IOException {
            outputSchema = SchemaHandler.getOutputSchema("query3.parquet");
            rankEntrySchema = outputSchema.getField("ranking").schema().getElementType();
        }

        @Override
        protected void reduce(
                final LongWritable key, final Iterable<Movie> values, final Context context)
                throws IOException, InterruptedException {
            GenericRecord record = new GenericData.Record(outputSchema);
            TreeMap<MovieRatingPair, Movie> ranking = new TreeMap<>();

            for (Movie movie : values) {
                ranking.put(
                        new MovieRatingPair(
                                movie.getMovieId().toString(), movie.getAverageRating().get()),
                        movie.clone());

                if (ranking.size() > 10) {
                    ranking.remove(ranking.firstKey());
                }
            }

            List<GenericRecord> rankingList = new ArrayList<>();
            Map<MovieRatingPair, Movie> treemapordering = ranking.descendingMap();

            for (Map.Entry<MovieRatingPair, Movie> movie : treemapordering.entrySet()) {
                GenericRecord rrec = new GenericData.Record(rankEntrySchema);
                rrec.put("movie_id", movie.getValue().getMovieId().toString());
                rrec.put("primaryTitle", movie.getValue().getTitle().toString());
                rrec.put("rating", movie.getValue().getAverageRating().get());
                rankingList.add(rrec);
            }

            record.put("year", key.get());
            record.put("ranking", rankingList);

            context.write(null, record);
        }
    }

    public void run() throws Exception {
        Job job = Job.getInstance(new Configuration(), "Top10RatingByYear");

        job.setJarByClass(Top10RatingByYear.class);
        job.setMapperClass(Top10RatingByYearMapper.class);
        job.setReducerClass(Top10RatingByYearReducer.class);
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(GenericRecord.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Movie.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job, new Path(this.input));
        AvroParquetInputFormat.setRequestedProjection(
                job, SchemaHandler.getInputSchema("query3.parquet"));

        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setOutputPath(job, new Path(this.output));
        AvroParquetOutputFormat.setSchema(job, SchemaHandler.getOutputSchema("query3.parquet"));

        job.waitForCompletion(true);
    }
}
