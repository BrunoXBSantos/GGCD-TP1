package ggcd.tp1;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import ggcd.tp1.utils.MovieHandler;
import ggcd.tp1.utils.SchemaHandler;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.IterableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;

@Parameters(commandDescription = "Count total number of movies per year")
public final class MoviesByYear {
    private static final MoviesByYear SINGLETON = new MoviesByYear();

    private MoviesByYear() {}

    public static MoviesByYear of() {
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
            extends Mapper<Void, GenericRecord, LongWritable, LongWritable> {
        @Override
        protected void map(final Void key, final GenericRecord value, final Context context)
                throws IOException, InterruptedException {
            Long year = MovieHandler.getStartYear(value);
            if (!(MovieHandler.hasTitleType(value, "movie") && year != null)) return;

            context.write(new LongWritable(year), new LongWritable(1));
        }
    }

    public static final class MoviesByYearReducer
            extends Reducer<LongWritable, LongWritable, Void, GenericRecord> {
        private Schema outputSchema;

        @Override
        protected void setup(final Context context) throws IOException {
            outputSchema = SchemaHandler.getOutputSchema("query1.parquet");
        }

        @Override
        protected void reduce(
                final LongWritable key, final Iterable<LongWritable> values, final Context context)
                throws IOException, InterruptedException {
            GenericRecord record = new GenericData.Record(outputSchema);

            record.put("year", key.get());
            record.put("movieCount", IterableUtils.size(values));

            context.write(null, record);
        }
    }

    public void run() throws Exception {
        Job job = Job.getInstance(new Configuration(), "MoviesByYear");

        job.setJarByClass(MoviesByYear.class);
        job.setMapperClass(MoviesByYearMapper.class);
        job.setReducerClass(MoviesByYearReducer.class);
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(GenericRecord.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job, new Path(this.input));
        AvroParquetInputFormat.setRequestedProjection(
                job, SchemaHandler.getInputSchema("query1.parquet"));

        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setOutputPath(job, new Path(this.output));
        AvroParquetOutputFormat.setSchema(job, SchemaHandler.getOutputSchema("query1.parquet"));

        job.waitForCompletion(true);
    }
}
