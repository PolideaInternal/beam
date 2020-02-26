package org.apache.beam.sdk.io.snowflake.test.tpch;

import java.io.Serializable;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

public class TpchTestUtils implements Serializable {

  static SnowflakeIO.CsvMapper<GenericRecord> getCsvMapper() {
    return (SnowflakeIO.CsvMapper<GenericRecord>)
        parts ->
            new GenericRecordBuilder(getSchema())
                .set("orderKey", Long.valueOf(parts[0]))
                .set("partKey", Long.valueOf(parts[1]))
                .set("suppKey", Long.valueOf(parts[2]))
                .set("lineNumber", Long.valueOf(parts[3]))
                .set("quantity", Double.valueOf(parts[4]))
                .set("extendedPrice", Double.valueOf(parts[5]))
                .set("discount", Double.valueOf(parts[6]))
                .set("tax", Double.valueOf(parts[7]))
                .set("returnFlag", parts[8])
                .set("lineStatus", parts[9])
                .set("shipDate", parts[10])
                .set("commitDate", parts[11])
                .set("receiptDate", parts[12])
                .set("shipInstruct", parts[13])
                .set("shipMode", parts[14])
                .set("comment", parts[15])
                .build();
  }

  static Schema getSchema() {
    return new Schema.Parser()
        .parse(
            "{\n"
                + " \"namespace\": \"snowflakeioexample\",\n"
                + " \"type\": \"record\",\n"
                + " \"name\": \"LineItem\",\n"
                + " \"fields\": [\n"
                + "     {\"name\": \"orderKey\", \"type\": \"long\"},\n"
                + "     {\"name\": \"partKey\", \"type\": \"long\"},\n"
                + "     {\"name\": \"suppKey\", \"type\": \"long\"},\n"
                + "     {\"name\": \"lineNumber\", \"type\": \"long\"},\n"
                + "     {\"name\": \"quantity\", \"type\": \"double\"},\n"
                + "     {\"name\": \"extendedPrice\", \"type\": \"double\"},\n"
                + "     {\"name\": \"discount\", \"type\": \"double\"},\n"
                + "     {\"name\": \"tax\", \"type\": \"double\"},\n"
                + "     {\"name\": \"returnFlag\", \"type\": \"string\"},\n"
                + "     {\"name\": \"lineStatus\", \"type\": \"string\"},\n"
                + "     {\"name\": \"shipDate\", \"type\": \"string\"},\n"
                + "     {\"name\": \"commitDate\", \"type\": \"string\"},\n"
                + "     {\"name\": \"receiptDate\", \"type\": \"string\"},\n"
                + "     {\"name\": \"shipInstruct\", \"type\": \"string\"},\n"
                + "     {\"name\": \"shipMode\", \"type\": \"string\"},\n"
                + "     {\"name\": \"comment\", \"type\": \"string\"}\n"
                + " ]\n"
                + "}");
  }

  static SnowflakeIO.UserDataMapper getUserDataMapper() {
    return (SnowflakeIO.UserDataMapper<GenericRecord>)
        element ->
            new Object[] {
              Long.valueOf(String.valueOf(element.get("orderKey"))),
              Long.valueOf(String.valueOf(element.get("partKey"))),
              Long.valueOf(String.valueOf(element.get("suppKey"))),
              Long.valueOf(String.valueOf(element.get("lineNumber"))),
              Double.valueOf(String.valueOf(element.get("quantity"))),
              Double.valueOf(String.valueOf(element.get("extendedPrice"))),
              Double.valueOf(String.valueOf(element.get("discount"))),
              Double.valueOf(String.valueOf(element.get("tax"))),
              String.valueOf(element.get("returnFlag")),
              String.valueOf(element.get("lineStatus")),
              String.valueOf(element.get("shipDate")),
              String.valueOf(element.get("commitDate")),
              String.valueOf(element.get("receiptDate")),
              String.valueOf(element.get("shipInstruct")),
              String.valueOf(element.get("shipMode")),
              String.valueOf(element.get("comment"))
            };
  }
}
