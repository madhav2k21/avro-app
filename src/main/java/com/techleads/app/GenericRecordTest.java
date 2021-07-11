package com.techleads.app;

import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class GenericRecordTest implements CommandLineRunner {

	@Override
	public void run(String... args) throws Exception {
		// step0: crate a generic record
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(
				"{ \"type\": \"record\", \"namespace\": \"com.techleads.app.avro\", \"name\": \"Customer\", \"doc\": \"Avro Schema for our Customer\",     \"fields\": [ { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" }, { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" }, { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" }, { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" }, { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" }, { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" } ] }");

		// step1: create a generic record
		GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
		customerBuilder.set("first_name", "Madhav");
		customerBuilder.set("last_name", "Anupoju");
		customerBuilder.set("age", 25);
		customerBuilder.set("height", 5.3f);
		customerBuilder.set("weight", 52.5f);
//		customerBuilder.set("automated_email", false);
		GenericData.Record customer = customerBuilder.build();

		System.out.println(customer);
		
		
		// step2: write that generic record to a file

		// writing to a file
		final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
		try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
			dataFileWriter.create(customer.getSchema(), new File("customer-generic.avsc"));
			dataFileWriter.append(customer);
			System.out.println("Written customer-generic.avsc");
		} catch (Exception e) {
			System.out.println("exception while writing to a file " + e.getMessage());
		}
		
		// step3: read a generic record from a file
		final File file = new File("customer-generic.avsc");
		final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
		GenericRecord customerRead;
		try (DataFileReader<GenericRecord> dataFileWriter = new DataFileReader<>(file, datumReader)) {
			customerRead = dataFileWriter.next();
			System.out.println("successfully read customer-generic.avsc");
			System.out.println(customerRead.toString());
			// get the data from the generic record
			System.out.println("FirstName: " + customerRead.get("first_name"));
			System.out.println("Last Name: " + customerRead.get("last_name"));
			System.out.println("Age: " + customerRead.get("age"));
			System.out.println("Height: " + customerRead.get("height"));
			System.out.println("Weight: " + customerRead.get("weight"));
			System.out.println("Automated Email: " + customerRead.get("automated_email"));

		} catch (Exception e) {
			System.out.println("exception while writing to a file " + e.getMessage());
		}
		System.exit(0);
		// step4: interpret as a generic record

	}

}
