package ggcd.tp1;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import ggcd.tp1.utils.Converter;
import ggcd.tp1.utils.SchemaHandler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;

@Parameters(commandDescription = "Convert title basics and ratings to parquet format")
public final class ToParquet {
    private static final ToParquet SINGLETON = new ToParquet();

    private ToParquet() {}

    public static ToParquet of() {
        return SINGLETON;
    }

    @Parameter(
            names = {"--left"},
            description = "Title basics file path")
    private String left;

    @Parameter(
            names = {"--right"},
            description = "Title ratings file path")
    private String right;

    @Parameter(
            names = {"--output"},
            description = "Output directory path")
    private String output = ggcd.tp1.config.Configuration.OUTPUT_DIR;

    public static final class TitleBasicsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(final LongWritable key, final Text value, final Context context)
                throws IOException, InterruptedException {
            if (key.get() == 0) {
                return;
            }
            String[] columns = value.toString().split("\t", 2);
            String id = columns[0];
            columns[0] = "L";

            context.write(new Text(id), new Text(String.join("\t", columns)));
        }
    }

    public static final class TitleRatingsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(final LongWritable key, final Text value, final Context context)
                throws IOException, InterruptedException {
            if (key.get() == 0) {
                return;
            }
            String[] columns = value.toString().split("\t", 2);
            String id = columns[0];
            columns[0] = "R";

            context.write(new Text(id), new Text(String.join("\t", columns)));
        }
    }

    public static final class TitleShuffleJoinToParquetReducer
            extends Reducer<Text, Text, Void, GenericRecord> {
        private Schema schema;

        @Override
        protected void setup(final Context context) throws IOException {
            schema = SchemaHandler.getOutputSchema("title.parquet");
        }

        @Override
        protected void reduce(final Text key, final Iterable<Text> values, final Context context)
                throws IOException, InterruptedException {
            String[] results = new String[2];

            for (Text value : values) {
                String result = value.toString();
                switch (result.charAt(0)) {
                    case 'L':
                        results[0] = result.substring(2);
                        break;
                    case 'R':
                        results[1] = result.substring(2);
                        break;
                    default:
                        throw new IOException("File not marked correctly");
                }
            }

            GenericRecord record = new GenericData.Record(schema);

            record.put("tconst", key.toString());

            try {
                List<String> basics =
                        Arrays.stream(results[0].split("\t"))
                                .map(column -> column.equals("\\N") ? null : column)
                                .collect(Collectors.toList());

                record.put("titleType", basics.get(0));
                record.put("primaryTitle", basics.get(1));
                record.put("originalTitle", basics.get(2));
                record.put("isAdult", !basics.get(3).equals("0"));
                record.put("startYear", basics.get(4));
                record.put("endYear", basics.get(5));
                record.put("runtimeMinutes", Converter.getInteger(basics.get(6)));

                List<String> genres = new ArrayList<>();

                try {
                    Collections.addAll(genres, basics.get(7).split(","));
                } catch (NullPointerException e) {
                    e.printStackTrace();
                }

                record.put("genres", genres);
            } catch (NullPointerException e) {
                System.out.println(record.toString());
                e.printStackTrace();
            }

            try {
                List<String> ratings =
                        Arrays.stream(results[1].split("\t"))
                                .map(column -> column.equals("\\N") ? null : column)
                                .collect(Collectors.toList());

                record.put("averageRating", Converter.getDouble(ratings.get(0)));
                record.put("numVotes", Converter.getInteger(ratings.get(1)));
            } catch (NullPointerException e) {
                System.out.println(record.toString());
                e.printStackTrace();
            }

            context.write(null, record);
        }
    }

    public void run() throws Exception {
        Job job = Job.getInstance(new Configuration(), "ToParquet");

        job.setJarByClass(ToParquet.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        MultipleInputs.addInputPath(
                job, new Path(this.left), TextInputFormat.class, TitleBasicsMapper.class);

        MultipleInputs.addInputPath(
                job, new Path(this.right), TextInputFormat.class, TitleRatingsMapper.class);

        job.setReducerClass(TitleShuffleJoinToParquetReducer.class);
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(GenericRecord.class);

        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setSchema(
                job, Objects.requireNonNull(SchemaHandler.getOutputSchema("title.parquet")));
        FileOutputFormat.setOutputPath(job, new Path(this.output));

        job.waitForCompletion(true);
    }
}
