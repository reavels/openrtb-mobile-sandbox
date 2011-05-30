package com.reavely.openrtb;

import static com.reavely.util.AvroUtil.invokeHttpEndpoint;
import static com.reavely.util.AvroUtil.loadSchemaFromClasspath;
import static com.reavely.util.AvroUtil.readAvroFromJsonFileOnClasspath;
import static com.reavely.util.AvroUtil.readGenericRecordFromStreamAsBinary;
import static com.reavely.util.AvroUtil.writeGenericRecordToFileAsJson;
import static com.reavely.util.AvroUtil.writeGenericRecordToStreamAsBinary;
import static com.reavely.util.HttpUtil.setupJettyTestServer;
import static com.reavely.util.HttpUtil.stopServer;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.reavely.util.HttpUtil;
import com.reavely.util.HttpUtil.HttpResponseHandler;

/**
 * 
 * @author Simon Reavely
 * 
 */
public class TestBidRequestInAvro {

	@SuppressWarnings("unused")
	@Before
	public void cleanOutputFolder() throws IOException {
		File dir = lookupAbsoluteFilePathFromClasspath("/openrtb/output");
		FileUtils.cleanDirectory(dir);
	}

	private File lookupAbsoluteFilePathFromClasspath(String fileOnClasspath) {
		// TODO: 1) See if I can find equivalent in open source
		URL url = this.getClass().getResource(fileOnClasspath);
		String file = url.getFile();
		return new File(file);
	}

	private GenericRecord createAvroBidRequest1(Schema schema) {
		GenericData.Record expected = new GenericData.Record(schema);

		expected.put("id", "BidRequest1");
		expected.put("at", 1);
		Schema impArraySchema = schema.getField("imp").schema();

		GenericData.Array<GenericRecord> imps = new GenericData.Array<GenericRecord>(
				1, impArraySchema);
		Schema impSchema = impArraySchema.getElementType();
		GenericData.Record imp1 = new GenericData.Record(impSchema);
		imp1.put("impid", "BidRequest1Impression1");
		imps.add(imp1);
		expected.put("imp", imps);
		return expected;
	}

	private GenericRecord createAvroBidResponse1(Schema schema) {
		GenericData.Record bidResponse = new GenericData.Record(schema);
		bidResponse.put("id", "BidRequest1");
		bidResponse.put("bidid", "Response1-bidid");
		// Now for the Seat Bid
		Schema seatBidArraySchema = schema.getField("seatbid").schema();
		Schema seatBidSchema = schema.getField("seatbid").schema()
				.getElementType();
		GenericData.Array<GenericData.Record> seatbids = new GenericData.Array<GenericData.Record>(
				1, seatBidArraySchema);
		GenericData.Record seatBid1 = new GenericData.Record(seatBidSchema);
		seatBid1.put("seat", "BidResponse1-seat");
		// Now for the bid
		Schema bidArraySchema = seatBidSchema.getField("bid").schema();
		Schema bidSchema = seatBidSchema.getField("bid").schema()
				.getElementType();
		GenericData.Array<GenericData.Record> bids = new GenericData.Array<GenericData.Record>(
				1, bidArraySchema);
		GenericData.Record bid1 = new GenericData.Record(bidSchema);
		bid1.put("impid", "BidRequest1Impression1");
		bid1.put("price", "20 USD");
		String adid = "100";
		bid1.put("adid", "100");
		String serverURL = "http://" + HttpUtil.DEFAULT_HOSTNAME + ":"
				+ HttpUtil.DEFAULT_PORT;
		String notificationURL = serverURL + "/notify";
		bid1.put("nurl", notificationURL);
		// Its at this point I wish I was writing my testcases in Groovy ;-)
		String mobileDisplayAdMarkup = "<a href=\"" + serverURL + "/click?aid="
				+ adid + "\"><img src=\"" + serverURL + "/content/ad" + adid
				+ ".jpg\" /></a>";
		bid1.put("adm", mobileDisplayAdMarkup);

		// Add the bid to the array of bids
		bids.add(bid1);
		// Add the bids to the seat bid
		seatBid1.put("bid", bids);
		// Add the seat bid to the array of seat bids
		seatbids.add(seatBid1);
		// Add the seat bids array to the response
		bidResponse.put("seatbid", seatbids);
		return bidResponse;
	}

	@Test
	public void testReadBidRequestFromJsonFile() throws Exception {
		Schema schema = loadSchemaFromClasspath("/BidRequest.avsc");

		// Read the request from the input file
		GenericRecord actual = readAvroFromJsonFileOnClasspath(
				"/openrtb/input/requests/BidRequest1.json", schema);

		GenericRecord expected = createAvroBidRequest1(schema);

		// Validate it against a previously serialized object
		assertEquals(
				"GenericRecord read from JSON file does not match expected: ",
				expected, actual);
	}

	@Test
	public void testWriteBidRequestAsJson() throws Exception {
		Schema schema = loadSchemaFromClasspath("/BidRequest.avsc");
		FileOutputStream fos = new FileOutputStream(
				"resources/test/openrtb/output/TestWriteBidRequestAsJson.json");
		try {

			GenericRecord record = createAvroBidRequest1(schema);

			writeGenericRecordToFileAsJson(schema, record, fos, true);
			GenericRecord actual = readAvroFromJsonFileOnClasspath(
					"/openrtb/output/TestWriteBidRequestAsJson.json", schema);

			assertEquals(
					"GenericRecord read from JSON file does not match expected: ",
					record, actual);
		} finally {
			IOUtils.closeQuietly(fos);
		}

	}

	@Test
	public void testWriteBidResponseAsJson() throws Exception {
		Schema schema = loadSchemaFromClasspath("/BidResponse.avsc");
		FileOutputStream fos = new FileOutputStream(
				"resources/test/openrtb/output/TestWriteBidResponseAsJson.json");
		try {

			// TODO: Split these out
			GenericRecord bidResponse = createAvroBidResponse1(schema);
			writeGenericRecordToFileAsJson(schema, bidResponse, fos, true);

			GenericRecord actual = readAvroFromJsonFileOnClasspath(
					"/openrtb/output/TestWriteBidResponseAsJson.json", schema);

			assertEquals(
					"GenericRecord read from JSON file does not match expected: ",
					bidResponse, actual);
		} finally {
			IOUtils.closeQuietly(fos);
		}

	}

	/**
	 * Utility method to create a response object from a request object, making
	 * sure that Ids from request make it into correct places in the response.
	 * Assumes that every bid gets a response.
	 * 
	 * @throws IOException
	 */
	public static GenericRecord createTemplateResponseFromRequest(
			GenericRecord request) throws IOException {
		// Create the response from the request, filling in required fields
		// I need to split these up into individual types to allow easy reuse in
		// case where only a subset of bids are returned\
		return null;
	}

	public static void mapRequestToLPARequestParams() {
		// this is the core of the transformer
	}

	@Test
	public void testAvroRequestSend() throws Exception {

		// Server handler for responding to the request
		Handler serverHandler = new AbstractHandler() {
			@Override
			public void handle(String target, HttpServletRequest request,
					HttpServletResponse response, int arg3) throws IOException,
					ServletException {
				Schema requestSchema = loadSchemaFromClasspath("/BidRequest.avsc");
				Schema responseSchema = loadSchemaFromClasspath("/BidResponse.avsc");
				InputStream is = null;
				OutputStream os = null;
				FileOutputStream fos = null;
				try {
					is = request.getInputStream();
					GenericRecord rtbRequest = readGenericRecordFromStreamAsBinary(
							requestSchema, is);
					// Now i have the request, lets write it out just for the
					// record in case we want to look at it after the test
					fos = new FileOutputStream(
							"resources/test/openrtb/output/TestAvroRequestSend-request.json");
					writeGenericRecordToFileAsJson(requestSchema, rtbRequest,
							fos, true);

					response.setContentType("binary/octet-stream");
					response.setStatus(HttpServletResponse.SC_OK);

					// Now lets create the response message
					GenericRecord rtbResponse = readAvroFromJsonFileOnClasspath(
							"/openrtb/input/responses/BidResponse1.json",
							responseSchema);
					// Now lets send it
					os = response.getOutputStream();
					writeGenericRecordToStreamAsBinary(responseSchema,
							rtbResponse, os);
					((Request) request).setHandled(true);

				} finally {
					IOUtils.closeQuietly(fos);
					IOUtils.closeQuietly(is);
					IOUtils.closeQuietly(os);
				}

			}
		};

		HttpResponseHandler respHandler = new HttpResponseHandler() {

			@Override
			public void handleResponse(HttpResponse response) throws Exception {

				Schema responseSchema = loadSchemaFromClasspath("/BidResponse.avsc");

				// Read the expected response
				GenericRecord expected = readAvroFromJsonFileOnClasspath(
						"/openrtb/input/responses/BidResponse1.json",
						responseSchema);

				HttpEntity resEntity = response.getEntity();
				if (resEntity != null) {
					// Read response and check its as expected
					InputStream ois = null;
					FileOutputStream fos = null;
					try {
						ois = resEntity.getContent();
						GenericRecord actual = readGenericRecordFromStreamAsBinary(
								responseSchema, ois);
						fos = new FileOutputStream(
								"resources/test/openrtb/output/TestAvroRequestSend-response.json");
						writeGenericRecordToFileAsJson(responseSchema, actual,
								fos, true);
						assertEquals("Response was not equal to expected",
								expected, actual);
					} finally {
						IOUtils.closeQuietly(ois);
						IOUtils.closeQuietly(fos);
					}
				}
				EntityUtils.consume(resEntity);
			}

		};

		Server server = setupJettyTestServer(serverHandler);
		try {
			Schema schema = loadSchemaFromClasspath("/BidRequest.avsc");

			// Read request to send
			GenericRecord record = readAvroFromJsonFileOnClasspath(
					"/openrtb/input/requests/BidRequest1.json", schema);

			invokeHttpEndpoint("http://localhost:8000/", respHandler, schema,
					record);
		} finally {
			stopServer(server);
		}

	}

	@Test
	public void testAvroRequestSendWithSpringProcessor() throws Exception {

		// Server handler for responding to the request
		Handler serverHandler = new AbstractHandler() {
			@Override
			public void handle(String target, HttpServletRequest request,
					HttpServletResponse response, int arg3) throws IOException,
					ServletException {
				Schema requestSchema = loadSchemaFromClasspath("/BidRequest.avsc");
				Schema responseSchema = loadSchemaFromClasspath("/BidResponse.avsc");
				InputStream is = null;
				OutputStream os = null;
				FileOutputStream fos = null;
				try {
					is = request.getInputStream();
					GenericRecord rtbRequest = readGenericRecordFromStreamAsBinary(
							requestSchema, is);
					// Now i have the request, lets write it out just for the
					// record in case we want to look at it after the test
					fos = new FileOutputStream(
							"resources/test/openrtb/output/TestAvroRequestSend-request.json");
					writeGenericRecordToFileAsJson(requestSchema, rtbRequest,
							fos, true);

					response.setContentType("binary/octet-stream");
					response.setStatus(HttpServletResponse.SC_OK);

					Resource springConfig = new ClassPathResource("/spring-test-bid-request-in-avro.xml");
					
					BeanFactory beanFactory = new XmlBeanFactory(springConfig);
					AvroRequestProcessor arp = (AvroRequestProcessor)beanFactory.getBean("requestProcessor"); 
					GenericRecord rtbResponse = arp.processRequest(rtbRequest);
					// Now lets send it
					os = response.getOutputStream();
					writeGenericRecordToStreamAsBinary(responseSchema,
							rtbResponse, os);
					((Request) request).setHandled(true);

				} catch (Exception e) {
					e.printStackTrace();
					throw new ServletException(e); 
				} finally {
					IOUtils.closeQuietly(fos);
					IOUtils.closeQuietly(is);
					IOUtils.closeQuietly(os);
				}

			}
		};

		HttpResponseHandler respHandler = new HttpResponseHandler() {

			@Override
			public void handleResponse(HttpResponse response) throws Exception {

				Schema responseSchema = loadSchemaFromClasspath("/BidResponse.avsc");

				// Read the expected response
				GenericRecord expected = readAvroFromJsonFileOnClasspath(
						"/openrtb/input/responses/BidResponse1.json",
						responseSchema);

				HttpEntity resEntity = response.getEntity();
				if (resEntity != null) {
					// Read response and check its as expected
					InputStream ois = null;
					FileOutputStream fos = null;
					try {
						ois = resEntity.getContent();
						GenericRecord actual = readGenericRecordFromStreamAsBinary(
								responseSchema, ois);
						fos = new FileOutputStream(
								"resources/test/openrtb/output/TestAvroRequestSend-response.json");
						writeGenericRecordToFileAsJson(responseSchema, actual,
								fos, true);
						assertEquals("Response was not equal to expected",
								expected, actual);
					} finally {
						IOUtils.closeQuietly(ois);
						IOUtils.closeQuietly(fos);
					}
				}
				EntityUtils.consume(resEntity);
			}

		};

		Server server = setupJettyTestServer(serverHandler);
		try {
			Schema schema = loadSchemaFromClasspath("/BidRequest.avsc");

			// Read request to send
			GenericRecord record = readAvroFromJsonFileOnClasspath(
					"/openrtb/input/requests/BidRequest1.json", schema);

			invokeHttpEndpoint("http://localhost:8000/", respHandler, schema,
					record);
		} finally {
			stopServer(server);
		}

	}

	public static class HardCodedRequestProcessor implements AvroRequestProcessor {
		private String fileToRead = null; 
		
		public GenericRecord processRequest(GenericRecord request) throws Exception {
			// Now lets create the response message
			Schema responseSchema = loadSchemaFromClasspath("/BidResponse.avsc");
			GenericRecord rtbResponse = readAvroFromJsonFileOnClasspath(
					getFileToRead(),
					responseSchema);
			
			return rtbResponse;
		}

		public void setFileToRead(String fileToRead) {
			this.fileToRead = fileToRead;
		}

		public String getFileToRead() {
			return fileToRead;
		}

	}
}