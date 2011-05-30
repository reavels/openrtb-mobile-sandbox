package com.reavely.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
/**
 * 
 * @author Simon Reavely
 *
 */
public class AvroUtil {

		/**
	 * Load the schema file from the classpath
	 * 
	 * @param schemaFileName
	 *            - schema file name on the classpath
	 * @return
	 * @throws IOException
	 */
	public static Schema loadSchemaFromClasspath(String schemaPath) throws IOException {
		InputStream schemaStream = AvroUtil.class
				.getResourceAsStream(schemaPath);
		if (schemaStream == null) {
			throw new IOException("Could not get resource from classpath:"
					+ schemaPath);
		}

		Schema schema = Schema.parse(schemaStream);
		schemaStream.close();
		return schema;
	}

	/**
	 * Load the schema from a File handle
	 * This helper is so minimal its almost not worth it
	 * @param schemaFile
	 * @return
	 * @throws IOException
	 */
	public static Schema loadSchemaFromFile(File schemaFile) throws IOException {
		Schema schema = Schema.parse(schemaFile);
		return schema;
	}

	
	/**
	 * Reads in binary Avro-encoded entities using a schema that is different
	 * from the writer's schema.
	 * 
	 * @param file
	 * @throws IOException
	 */
	public static GenericRecord readAvroFromJsonFileOnClasspath(String fileName,
			Schema schema) throws IOException {
		GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(
				schema);

		InputStream is = null;
		try {
			is = AvroUtil.class.getResourceAsStream(fileName);
			if (is == null) {
				throw new IOException("Could not get resource from classpath:"
						+ fileName);
			}
			Decoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, is);

			GenericRecord record = new GenericData.Record(schema);
			record = datumReader.read(record, jsonDecoder);
			return record;
		} finally {
			if (is != null)
				is.close();
		}
	}
	
	public static GenericRecord readAvroFromJsonFile(File file,
			Schema schema) throws IOException {
		GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(
				schema);

		FileInputStream fis = null;
		try {
			fis = new FileInputStream(file);
			if (fis == null) {
				throw new IOException("Could not open stream for file:"
						+ file.getPath());
			}
			Decoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, fis);

			GenericRecord record = new GenericData.Record(schema);
			record = datumReader.read(record, jsonDecoder);
			return record;
		} finally {
			if (fis != null)
				fis.close();
		}
	}

	public static void writeGenericRecordToFileAsJson(Schema schema,
			GenericRecord recordToWrite, FileOutputStream fos,
			boolean doPrettyPrint) throws IOException {
		ObjectMapper om = new ObjectMapper();
		JsonFactory factory = new JsonFactory(om);
		JsonGenerator g = factory.createJsonGenerator(fos, JsonEncoding.UTF8);
		GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(
				schema);
		if (doPrettyPrint) {
			g.useDefaultPrettyPrinter();
		}
		Encoder e = EncoderFactory.get().jsonEncoder(schema, g);

		writer.write(recordToWrite, e);
		e.flush(); // Don't forget to flush...otherwise you might see

	}

	public static void writeGenericRecordToStreamAsBinary(Schema schema,
			GenericRecord recordToWrite, OutputStream os) throws IOException {
		GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(
				schema);
		Encoder e = EncoderFactory.get().binaryEncoder(os, null);
		writer.write(recordToWrite, e);
		e.flush(); // Don't forget to flush...otherwise you might not see it all
	}

	public static GenericRecord readGenericRecordFromStreamAsBinary(
			Schema schema, InputStream is) throws IOException {
		GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(
				schema);

		try {
			if (is == null) {
				throw new IOException("Null inputstream");
			}
			Decoder binaryDecoder = DecoderFactory.get()
					.binaryDecoder(is, null);

			GenericRecord record = new GenericData.Record(schema);
			record = datumReader.read(record, binaryDecoder);
			return record;
		} finally {
			if (is != null)
				is.close();
		}
	}

	public static void writeGenericRecordToFileAsJson(Schema schema,
			GenericRecord recordToWrite, File file, boolean doPrettyPrint)
			throws IOException {
		FileOutputStream fos = new FileOutputStream(file);
		try {
			writeGenericRecordToFileAsJson(schema, recordToWrite, fos,
					doPrettyPrint);
		} finally {
			if (fos != null)
				fos.close();
		}
	}



	public static void invokeHttpEndpoint(String endpointUrl,
			HttpUtil.HttpResponseHandler respHandler, Schema schema,
			GenericRecord recordToSend) throws Exception {
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		writeGenericRecordToStreamAsBinary(schema, recordToSend, os);
		byte[] bytes = os.toByteArray();
		ByteArrayInputStream is = new ByteArrayInputStream(bytes);
		HttpUtil.doHttpPost(endpointUrl, respHandler, is);
	}

	
}