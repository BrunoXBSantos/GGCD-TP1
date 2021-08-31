package ggcd.tp1.utils;

import java.io.IOException;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.commons.io.output.ByteArrayOutputStream;

public final class Converter {
    private Converter() {}

    public static Integer getInteger(final String value) {
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return null;
        }
    }

    public static Double getDouble(final String value) {
        try {
            return Double.parseDouble(value);
        } catch (Exception e) {
            return null;
        }
    }

    public static String avroToJson(GenericRecord record) throws IOException {
        DatumWriter<Object> writer = new GenericDatumWriter<>(record.getSchema());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(record.getSchema(), out);
        writer.write(record, jsonEncoder);
        jsonEncoder.flush();
        return out.toString();
    }
}
