package com.genericRecord;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class GenericRecordExample {

    public static void main(String[] args) {

        // Step 0: define schema.
        Schema.Parser parserSchema = new Schema.Parser();
        Schema schema = parserSchema.parse("{\n" +
                "     \"type\": \"record\",\n" +
                "     \"namespace\": \"com.example\",\n" +
                "     \"name\": \"Customer\",\n" +
                "     \"fields\": [\n" +
                "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
                "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n" +
                "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
                "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
                "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
                "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" }\n" +
                "     ]\n" +
                "}");



        // Step 1: Create a generic record using the schema created above.
        GenericRecordBuilder customerBuilder1 = new GenericRecordBuilder(schema);
        customerBuilder1.set("first_name", "John");
        customerBuilder1.set("last_name", "Doe");
        customerBuilder1.set("age", 26);
        customerBuilder1.set("height", 175f);
        customerBuilder1.set("weight", 70.5f);
        customerBuilder1.set("automated_email", false);

        GenericData.Record myCustomer1 = customerBuilder1.build();

        GenericRecordBuilder customerBuilder2 = new GenericRecordBuilder(schema);
        customerBuilder2.set("first_name", "Johnny");
        customerBuilder2.set("last_name", "Walker");
        customerBuilder2.set("age", 35);
        customerBuilder2.set("height", 200f);
        customerBuilder2.set("weight", 85.5f);

        GenericData.Record myCustomer2 = customerBuilder2.build();

        System.out.println(myCustomer1);
        System.out.println(myCustomer2);


        // Step 2: Write, on Intellij(project) root, that generic record to a file
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);

        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {

            dataFileWriter.create(myCustomer1.getSchema(), new File("customer-generic.avro"));
            //or this: dataFileWriter.create(schema, new File("customer-generic.avro"));

            dataFileWriter.append(myCustomer1);
            dataFileWriter.append(myCustomer2);

            System.out.println("Written customer-generic.avro");

        } catch (IOException e) {

            System.out.println("Couldn't write file");
            e.printStackTrace();
        }


        // Step 3: Read a generic record from a file
        final File file = new File("customer-generic.avro");

        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();

        GenericRecord customerRead;

        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)) {


        // Step 4: Interpret as a generic record
            while (dataFileReader.hasNext()) {
                customerRead = dataFileReader.next();

                System.out.println("Successfully read avro file");
                System.out.println(customerRead.toString());


                // get the data from the generic record
                System.out.println("First name: " + customerRead.get("first_name"));

                // read a non existent field
                System.out.println("Non existent field: " + customerRead.get("not_here"));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
