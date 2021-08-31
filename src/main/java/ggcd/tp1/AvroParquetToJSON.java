package ggcd.tp1;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import ggcd.tp1.utils.Converter;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;

@Parameters(commandDescription = "Convert avro+parquet file to JSON")
public final class AvroParquetToJSON {
    private static final AvroParquetToJSON SINGLETON = new AvroParquetToJSON();

    private AvroParquetToJSON() {}

    public static AvroParquetToJSON of() {
        return SINGLETON;
    }

    @Parameter(
            names = {"--input"},
            description = "Path of input avro+parquet file",
            required = true)
    private String input;

    @Parameter(
            names = {"--output"},
            description = "Path of output folder")
    private String output = ggcd.tp1.config.Configuration.OUTPUT_DIR;

    public static final class AvroParquetToJSONMapper
            extends Mapper<Void, GenericRecord, Void, Text> {
        @Override
        protected void map(final Void key, final GenericRecord value, final Context context)
                throws IOException, InterruptedException {
            context.write(null, new Text(Converter.avroToJson(value)));
        }
    }

    public void run() throws Exception {
        Job job = Job.getInstance(new Configuration(), "AvroParquetToJSON");

        job.setJarByClass(AvroParquetToJSON.class);
        job.setMapperClass(AvroParquetToJSONMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job, new Path(this.input));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(this.output));

        job.waitForCompletion(true);
    }
}
