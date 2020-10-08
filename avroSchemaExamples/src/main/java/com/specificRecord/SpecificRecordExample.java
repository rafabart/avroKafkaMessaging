package com.specificRecord;

import com.example.Customer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SpecificRecordExample {

    public static void main(String[] args) {

        // Step 1: Create a specific record.
        Customer.Builder customerbuilder = Customer.newBuilder();
        customerbuilder.setAge(25);
        customerbuilder.setFirstName("Johnny");
        customerbuilder.setLastName("Bravo");
        customerbuilder.setHeight(175.5f);
        customerbuilder.setWeight(80.6F);
        customerbuilder.setAutomatedEmail(false);

        Customer customer = customerbuilder.build();

        System.out.println(customer);


        // Step 2: Write, on Intellij(project) root, that specific record to a file
        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);

        try (DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter)) {

            dataFileWriter.create(customer.getSchema(), new File("customer-specific.avro"));
            dataFileWriter.append(customer);
            System.out.println("successfully wrote customer-specific.avro");

        } catch (IOException e) {
            e.printStackTrace();
        }


        // Step 3: Read from file
        final File file = new File("customer-specific.avro");
        final DatumReader<Customer> datumReader = new SpecificDatumReader<>(Customer.class);
        final DataFileReader<Customer> dataFileReader;

        try {
            System.out.println("Reading our specific record");

            dataFileReader = new DataFileReader<>(file, datumReader);

            // Step 4: Interpret as a specific record
            while (dataFileReader.hasNext()) {
                Customer readCustomer = dataFileReader.next();
                System.out.println(readCustomer.toString());
                System.out.println("First name: " + readCustomer.getFirstName());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
