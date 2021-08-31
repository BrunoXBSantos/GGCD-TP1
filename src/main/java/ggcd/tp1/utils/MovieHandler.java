package ggcd.tp1.utils;

import org.apache.avro.generic.GenericRecord;

public final class MovieHandler {
    private static Long getLongField(GenericRecord record, String field) {
        Object o = record.get(field);
        return o == null ? null : Long.parseLong(o.toString());
    }

    private static Double getDoubleField(GenericRecord record, String field) {
        Object o = record.get(field);
        return o == null ? null : Double.parseDouble(o.toString());
    }

    private static String getStringField(GenericRecord record, String field) {
        Object o = record.get(field);
        return o == null ? null : o.toString();
    }

    private static boolean hasMatchingField(GenericRecord record, String field, String match) {
        Object titleType = record.get(field);
        return titleType != null && titleType.toString().equalsIgnoreCase(match);
    }

    public static boolean hasTitleType(GenericRecord record, String type) {
        return MovieHandler.hasMatchingField(record, "titleType", type);
    }

    public static Long getStartYear(GenericRecord record) {
        return MovieHandler.getLongField(record, "startYear");
    }

    public static Long getNumVotes(GenericRecord record) {
        return MovieHandler.getLongField(record, "numVotes");
    }

    public static Double getRating(GenericRecord record) {
        return MovieHandler.getDoubleField(record, "averageRating");
    }

    public static String getPrimaryTittle(GenericRecord record) {
        return MovieHandler.getStringField(record, "primaryTitle");
    }
}
