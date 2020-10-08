package com.schemaEvolution;

import com.example.CustomerV1;
import com.example.CustomerV2;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SchemaEvolutionExample {
    public static void main(String[] args) throws IOException {

        // vamos testar uma leitura compatível com versões anteriores

        // lidamos com o V1 do nosso customer
        CustomerV1 customerV1 = CustomerV1.newBuilder()
                .setAge(34)
                .setAutomatedEmail(false)
                .setFirstName("John")
                .setLastName("Doe")
                .setHeight(178f)
                .setWeight(75f)
                .build();

        System.out.println("Customer V1 = " + customerV1.toString());


        // escreva para um arquivo
        final DatumWriter<CustomerV1> datumWriter = new SpecificDatumWriter<>(CustomerV1.class);
        final DataFileWriter<CustomerV1> dataFileWriter = new DataFileWriter<>(datumWriter);

        dataFileWriter.create(customerV1.getSchema(), new File("customerV1.avro"));
        dataFileWriter.append(customerV1);
        dataFileWriter.close();

        System.out.println("successfully wrote customerV1.avro");

        // lemos usando o esquema v2
        System.out.println("Reading our customerV1.avro with v2 schema");

        final File file = new File("customerv1.avro");
        final DatumReader<CustomerV2> datumReaderV2 = new SpecificDatumReader<>(CustomerV2.class);
        final DataFileReader<CustomerV2> dataFileReaderV2 = new DataFileReader<>(file, datumReaderV2);

        while (dataFileReaderV2.hasNext()) {
            CustomerV2 customerV2read = dataFileReaderV2.next();
            System.out.println("Customer V2 = " + customerV2read.toString());
        }

        System.out.println("Backward schema evolution successful\n\n\n");


        // vamos testar uma leitura compatível com versões futuras

        // lidamos com o V1 do nosso customer
        CustomerV2 customerv2 = CustomerV2.newBuilder()
                .setAge(25)
                .setFirstName("Mark")
                .setLastName("Simpson")
                .setEmail("mark.simpson@gmail.com")
                .setHeight(160f)
                .setWeight(65f)
                .setPhoneNumber("123-456-7890")
                .build();

        System.out.println("Customer V2 = " + customerv2.toString());

        // escreva para um arquivo usando o esquema V2
        final DatumWriter<CustomerV2> datumWriterV2 = new SpecificDatumWriter<>(CustomerV2.class);
        final DataFileWriter<CustomerV2> dataFileWriterV2 = new DataFileWriter<>(datumWriterV2);

        dataFileWriterV2.create(customerv2.getSchema(), new File("customerV2.avro"));
        dataFileWriterV2.append(customerv2);
        dataFileWriterV2.close();

        System.out.println("successfully wrote customerV2.avro");


        // lemos usando o esquema v2
        System.out.println("Reading our customerV2.avro with v1 schema");

        final File file2 = new File("customerV2.avro");
        final DatumReader<CustomerV1> datumReader = new SpecificDatumReader<>(CustomerV1.class);
        final DataFileReader<CustomerV1> dataFileReader = new DataFileReader<>(file2, datumReader);

        while (dataFileReader.hasNext()) {
            CustomerV1 customerV1Read = dataFileReader.next();
            System.out.println("Customer V1 = " + customerV1Read.toString());
        }

        System.out.println("Forward schema evolution successful");

    }
}