package com.reavely.util;


import java.io.InputStream;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
/**
 * 
 * @author Simon Reavely
 *
 */
public class HttpUtil {

	public static final String DEFAULT_HOSTNAME = "localhost";
	public static final Integer DEFAULT_PORT = 8000;
	
	public static Server setupJettyTestServer(Handler handler) throws Exception {

		Server server = new Server(DEFAULT_PORT);
		server.setHandler(handler);
		server.start();
		return server;

	}

	public static void stopServer(Server server) throws Exception {
		server.stop();
		//System.out.println("checking server state");
		while (!server.isStopped()) {
			System.out.println("Going to sleep for 1 second");
			Thread.sleep(1000);
		}

	}
	
	public static void doHttpPost(String endpointUrl,
			HttpResponseHandler respHandler, InputStream is) throws Exception {
		HttpClient httpclient = new DefaultHttpClient();
		try {

			InputStreamEntity reqEntity = new InputStreamEntity(is, -1);
			reqEntity.setContentType("binary/octet-stream");
			reqEntity.setChunked(true);
			HttpPost httppost = new HttpPost(endpointUrl);
			httppost.setEntity(reqEntity);
			//System.out.println("executing request " + httppost.getRequestLine());
			HttpResponse response = httpclient.execute(httppost);
			respHandler.handleResponse(response);
		} finally {
			// When HttpClient instance is no longer needed,
			// shut down the connection manager to ensure
			// immediate deallocation of all system resources
			httpclient.getConnectionManager().shutdown();
		}

	}


	/**
	 * Handler that allows a client to program how a response is handled e.g.
	 * asserting expected results
	 */
	public interface HttpResponseHandler {
		public void handleResponse(HttpResponse response) throws Exception;
	}
}
