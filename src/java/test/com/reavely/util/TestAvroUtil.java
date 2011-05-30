package com.reavely.util;

import static com.reavely.util.AvroUtil.loadSchemaFromClasspath;
import static com.reavely.util.AvroUtil.readAvroFromJsonFileOnClasspath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
/**
 * 
 * @author Simon Reavely
 *
 */
public class TestAvroUtil {

	public GenericRecord createExpectedAvroUtilObj1(Schema schema) {

		GenericRecord expected = new GenericData.Record(schema);

		expected.put("field1", "TestReadFromJsonFile");
		expected.put("field2", 1);
		Schema impArraySchema = schema.getField("array1").schema();

		GenericData.Array<GenericRecord> imps = new GenericData.Array<GenericRecord>(
				1, impArraySchema);
		Schema impSchema = impArraySchema.getElementType();
		GenericRecord imp1 = new GenericData.Record(impSchema);
		imp1.put("subfield1", "hello");
		imps.add(imp1);
		expected.put("array1", imps);
		return expected;
	}

	public GenericRecord createExpectedAvroUtilWithNull(Schema schema) {

		GenericRecord expected = new GenericData.Record(schema);

		expected.put("field1", "TestReadFromJsonFile");
		expected.put("field2", null);
		Schema impArraySchema = schema.getField("array1").schema();

		GenericData.Array<GenericRecord> imps = new GenericData.Array<GenericRecord>(
				1, impArraySchema);
		Schema impSchema = impArraySchema.getElementType();
		GenericRecord imp1 = new GenericData.Record(impSchema);
		imp1.put("subfield1", "hello");
		imps.add(imp1);
		expected.put("array1", imps);
		return expected;
	}

	/**
	 * Test that the utility method to read a Json object into a Generic Avro
	 * Record works
	 * 
	 * @throws Exception
	 */
	@Test
	public void testReadFromJsonFile() throws Exception {
		Schema schema = loadSchemaFromClasspath("/util/avro-schema/AvroUtil.avsc");

		
		// Read the request from the input file
		GenericRecord actual = readAvroFromJsonFileOnClasspath(
				"/util/input/TestReadFromJsonFile.json", schema);
		
		GenericRecord expected = createExpectedAvroUtilObj1(schema);
		
		// Validate it against a previously serialized object
		assertEquals(
				"GenericRecord read from JSON file does not match expected: ",
				expected, actual);
	}

	@Test
	public void testReadFromJsonFileWithNull() throws Exception {
		Schema schema = loadSchemaFromClasspath("/util/avro-schema/AvroUtil.avsc");

		// Read the request from the input file
		GenericRecord actual = readAvroFromJsonFileOnClasspath(
				"/util/input/TestReadFromJsonFileWithNull.json", schema);

		GenericRecord expected = createExpectedAvroUtilWithNull(schema);

		// Validate it against a previously serialized object
		assertEquals(
				"GenericRecord read from JSON file does not match expected: ",
				expected, actual);
	}

	@Test
	public void testReadFromJsonFileWithNullInUnion() throws Exception {
		Schema schema = loadSchemaFromClasspath("/util/avro-schema/AvroUtil.avsc");

		// Read the request from the input file
		GenericRecord actual = readAvroFromJsonFileOnClasspath(
				"/util/input/TestReadFromJsonFileWithNullInUnion.json",
				schema);

		GenericRecord expected = createExpectedAvroUtilWithNull(schema);

		// Validate it against a previously serialized object
		assertEquals(
				"GenericRecord read from JSON file does not match expected: ",
				expected, actual);
	}

	@Test
	public void testReadFromJsonFileWithoutUnionForOptionalField()
			throws Exception {
		Schema schema = loadSchemaFromClasspath("/util/avro-schema/AvroUtil.avsc");

		// Read the request from the input file
		try {
			GenericRecord actual = readAvroFromJsonFileOnClasspath(
					"/util/input/TestReadFromJsonFileWithoutUnionForOptionalField.json",
					schema);
			// This file does not use a union for the optional field value and
			// therefore will throw an exception
			fail("readAvroFromJsonFile() should throw an exception due to not using a union for the optional field");
		} catch (AvroTypeException e) {
			assertTrue(
					"Exception message was not expecting start-union: "
							+ e.getMessage(),
					e.getMessage().contains("Expected start-union"));
		}

	}

	//TODO: Add more tests for other AvroUtil methods e.g. writers
	
}