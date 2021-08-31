package ggcd.tp1;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import ggcd.tp1.models.Movie;
import ggcd.tp1.utils.MovieHandler;
import ggcd.tp1.utils.SchemaHandler;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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

@Parameters(commandDescription = "Most voted movie per year")
public final class MostVotedByYear {
    private static final MostVotedByYear SINGLETON = new MostVotedByYear();

    private MostVotedByYear() {}

    public static MostVotedByYear of() {
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

    public static final class MoviesByYearMapper
            extends Mapper<Void, GenericRecord, LongWritable, Movie> {
        private Map<Long, Long> votesPerYear;
        private Map<Long, Movie> bestCandidatePerYear;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.votesPerYear = new HashMap<>();
            this.bestCandidatePerYear = new HashMap<>();
        }

        @Override
        protected void map(final Void key, final GenericRecord value, final Context context)
                throws IOException, InterruptedException {
            Long year = MovieHandler.getStartYear(value);
            Long votes = MovieHandler.getNumVotes(value);
            if (!(MovieHandler.hasTitleType(value, "movie")
                    && year != null
                    && votes != null
                    && value.get("tconst") != null)) return;

            if (!votesPerYear.containsKey(year) || votes > votesPerYear.get(year)) {
                Movie mov =
                        new Movie(
                                value.get("tconst").toString(),
                                value.get("primaryTitle").toString(),
                                year,
                                votes);

                votesPerYear.put(year, votes);
                bestCandidatePerYear.put(year, mov);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Long, Movie> entry : bestCandidatePerYear.entrySet()) {
                context.write(new LongWritable(entry.getKey()), entry.getValue());
            }
        }
    }

    public static final class MoviesByYearReducer
            extends Reducer<LongWritable, Movie, Void, GenericRecord> {
        private Schema outputSchema;

        @Override
        protected void setup(final Context context) throws IOException {
            outputSchema = SchemaHandler.getOutputSchema("query2.parquet");
        }

        @Override
        protected void reduce(
                final LongWritable key, final Iterable<Movie> values, final Context context)
                throws IOException, InterruptedException {
            long topVotes = -1;
            Movie topMovie = null;

            for (Movie m : values) {
                long votes = m.getNumVotes().get();

                if (votes > topVotes) {
                    topVotes = votes;
                    topMovie = m;
                }
            }

            if (topMovie != null) {
                GenericRecord record = new GenericData.Record(outputSchema);
                record.put("year", topMovie.getYear().get());
                record.put("movie_id", topMovie.getMovieId().toString());
                record.put("primaryTitle", topMovie.getTitle().toString());
                record.put("numVotes", topMovie.getNumVotes().get());
                context.write(null, record);
            }
        }
    }

    public void run() throws Exception {
        Job job = Job.getInstance(new Configuration(), "MostVotedByYear");

        job.setJarByClass(MostVotedByYear.class);
        job.setMapperClass(MoviesByYearMapper.class);
        job.setReducerClass(MoviesByYearReducer.class);
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(GenericRecord.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Movie.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job, new Path(this.input));
        AvroParquetInputFormat.setRequestedProjection(
                job, SchemaHandler.getInputSchema("query2.parquet"));

        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setOutputPath(job, new Path(this.output));
        AvroParquetOutputFormat.setSchema(job, SchemaHandler.getOutputSchema("query2.parquet"));

        job.waitForCompletion(true);
    }
}
