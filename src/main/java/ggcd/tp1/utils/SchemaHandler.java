package ggcd.tp1.utils;

import ggcd.tp1.ToParquet;
import ggcd.tp1.config.Configuration;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public final class SchemaHandler {
    public static Schema getSchema(String schema) throws IOException {
        InputStream is = ToParquet.class.getClassLoader().getResourceAsStream(schema);
        MessageType mt =
                MessageTypeParser.parseMessageType(
                        new String(IOUtils.toByteArray(Objects.requireNonNull(is))));
        return new AvroSchemaConverter().convert(mt);
    }

    public static Schema getInputSchema(String title) throws IOException {
        return SchemaHandler.getSchema(Configuration.INPUT_SCHEMAS_DIR + title);
    }

    public static Schema getOutputSchema(String title) throws IOException {
        return SchemaHandler.getSchema(Configuration.OUTPUT_SCHEMAS_DIR + title);
    }
}
