package com.reavely.util;


import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.junit.Test;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

import com.reavely.util.HttpUtil.HttpResponseHandler;
/**
 * 
 * @author Simon Reavely
 *
 */
public class TestHttpUtil {
	
	@Test
	public void testJettyUnitTest() throws Exception {

		final String RESPONSE_TEXT = "<h1>Hello</h1>";
			
		Handler serverHandler = new AbstractHandler() {
			@Override
			public void handle(String target, HttpServletRequest request,
					HttpServletResponse response, int arg3) throws IOException,
					ServletException {
				InputStream is = request.getInputStream();
				String requestStr = IOUtils.toString(is);
				//System.out.println("request: " + requestStr);
				response.setContentType("text/html");
				response.setStatus(HttpServletResponse.SC_OK);
				OutputStream os = response.getOutputStream();
				response.setCharacterEncoding("UTF-8");
				StringReader sr = new StringReader(RESPONSE_TEXT);
				IOUtils.copy(sr, os, "UTF-8");
			    os.close();
				((Request) request).setHandled(true);

			}
		};
		
		HttpResponseHandler respHandler = new HttpResponseHandler() {
			@Override
			public void handleResponse(HttpResponse response) throws Exception {
				HttpEntity respEntity = response.getEntity();
				InputStream is = respEntity.getContent();
				String responseStr = IOUtils.toString(is,"UTF-8");
				is.close();
				String expected = new String(RESPONSE_TEXT);
				assertEquals("Did not match:", expected, responseStr);

			}
		};
		Server server = HttpUtil.setupJettyTestServer(serverHandler);
		try {

			InputStream is = IOUtils.toInputStream("Hello");
			String postURL = "http://"+HttpUtil.DEFAULT_HOSTNAME+":"+HttpUtil.DEFAULT_PORT+"/";
			HttpUtil.doHttpPost(postURL, respHandler, is);
		} finally {
			HttpUtil.stopServer(server);
		}
	}

}
