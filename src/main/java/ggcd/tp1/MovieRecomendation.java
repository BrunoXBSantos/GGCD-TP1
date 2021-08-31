package ggcd.tp1;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import ggcd.tp1.models.GenreRatingGroupingComparator;
import ggcd.tp1.models.GenreRatingPair;
import ggcd.tp1.models.GenreRatingPartitioner;
import ggcd.tp1.utils.MovieHandler;
import ggcd.tp1.utils.SchemaHandler;
import java.io.IOException;
import java.util.HashMap;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;

@Parameters(commandDescription = "Movie recomendation from the same genres")
public class MovieRecomendation {

    private static final MovieRecomendation SINGLETON = new MovieRecomendation();

    private MovieRecomendation() {}

    public static MovieRecomendation of() {
        return SINGLETON;
    }

    @Parameter(
            names = {"--input"},
            description = "Path of input avro+parquet file")
    private String input;

    @Parameter(
            names = {"--output"},
            description = "Path of output avro+parquet file")
    private String output = ggcd.tp1.config.Configuration.OUTPUT_DIR;

    /*
     * (genero, (Rating, Movie))
     *
     * Natural key   -> genero
     * Natural Value -> (Rating, name)
     * Secondary Key -> Rating
     * Composite Key -> (genero, Rating)
     *
     * */

    public static final class SecondarySortMapper
            extends Mapper<Void, GenericRecord, GenreRatingPair, Text> {

        private final Text theRating = new Text();
        private final GenreRatingPair pair = new GenreRatingPair();

        @Override
        protected void map(Void key, GenericRecord value, Context context)
                throws IOException, InterruptedException {
            GenericData.Array<String> list = (GenericData.Array<String>) value.get("genres");
            Double rating = MovieHandler.getRating(value);
            String name = (String) value.get("primaryTitle");

            if (!(MovieHandler.hasTitleType(value, "movie")
                    && name != null
                    && rating != null
                    && list.size() != 0
                    && value.get("tconst") != null)) return;

            String firstGenre = list.get(0);
            pair.setGenre(firstGenre);
            pair.setRating(rating);
            pair.setMovie(name);
            theRating.set(String.valueOf(rating));

            context.write(pair, theRating);
        }
    }

    public static final class SecondarySortReduce
            extends Reducer<GenreRatingPair, Text, Text, Text> {

        HashMap<String, String> bestMovies2Genre;
        int flag;

        @Override
        protected void setup(Context context) {
            this.bestMovies2Genre = new HashMap<>();
            flag = 0;
        }

        @Override
        protected void reduce(GenreRatingPair key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                if (!bestMovies2Genre.containsKey(key.getGenre().toString())) {
                    bestMovies2Genre.put(
                            key.getGenre().toString(),
                            String.format("%s :: %s", key.getMovie(), key.getRating()));
                    flag = 0;
                } else {
                    String movie = String.format("%s :: %s", key.getMovie(), key.getRating());
                    if (flag == 0) {
                        flag = 1;
                        context.write(
                                new Text(
                                        key.getGenre().toString()
                                                + " ::: "
                                                + bestMovies2Genre.get(key.getGenre().toString())),
                                new Text("--->\t" + movie));
                    }
                    context.write(
                            new Text(key.getGenre().toString() + " ::: " + movie),
                            new Text("--->\t" + bestMovies2Genre.get(key.getGenre().toString())));
                }
            }
        }
    }

    public void run() throws Exception {

        Job job = Job.getInstance(new Configuration(), "MovieRecomendation");

        job.setJarByClass(MovieRecomendation.class);

        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReduce.class);
        job.setPartitionerClass(GenreRatingPartitioner.class);
        job.setGroupingComparatorClass(GenreRatingGroupingComparator.class);

        job.setOutputKeyClass(GenreRatingPair.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job, new Path(this.input));

        AvroParquetInputFormat.setRequestedProjection(
                job, SchemaHandler.getInputSchema("exercise3.parquet"));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(this.output));

        job.waitForCompletion(true);
    }
}
