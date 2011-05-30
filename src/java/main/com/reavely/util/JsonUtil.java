package com.reavely.util;


import java.io.File;
import java.io.IOException;
import java.io.Writer;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * 
 * @author Simon Reavely
 *
 */
public class JsonUtil {
	
	public static JsonNode readJsonNodeFromFile(File file) throws IOException {
		ObjectMapper om = new ObjectMapper();
		JsonFactory factory = new JsonFactory(om);
		JsonParser jp = factory.createJsonParser(file);
		JsonNode rootNode = jp.readValueAsTree();
		jp.close();
		return rootNode;
	}

	public static void printJson(JsonNode rootNode, Writer writer)
			throws IOException {
		ObjectMapper om = new ObjectMapper();
		JsonFactory factory = new JsonFactory(om);
		JsonGenerator g = factory.createJsonGenerator(writer);
		g.useDefaultPrettyPrinter();
		g.writeTree(rootNode);
		g.close();
	}

}
