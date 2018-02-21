package esdemo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Consts;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.params.CoreProtocolPNames;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ThreadedElasticsearchHTTPService {
	private static final Log logger = LogFactory.getLog(ThreadedElasticsearchHTTPService.class);

	static private MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();

	static private HttpClient client = new HttpClient(connectionManager);

	static private int PORT = 9200;
	static private String ADDRESS = "http://localhost";
	static private String URL = String.format("%s:%s", ADDRESS, PORT);

	static private JSONObject parseElasticsearchResponse(CloseableHttpResponse response){
		
		StringBuffer result = new StringBuffer();
		String line = "";
		try {
			BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			while ((line = rd.readLine()) != null) {
			    result.append(line);
			}
		} catch (IOException e) {
			logger.fatal(e);
			return null;
		}

		JSONParser responseParser = new JSONParser();
		try {
			JSONObject resp = (JSONObject)responseParser.parse(result.toString());
			return resp;
		} catch (ParseException e) {
			logger.fatal(e);
			return null;
		}
	}
	
	public void setElasticsearchConfig(String address, int port) {
		PORT = port;
		ADDRESS = address;
		URL = String.format("%s:%s", ADDRESS, PORT);
	}

	public static String send(String endpoint, JSONObject obj){
		HttpPost post = new HttpPost(String.format("%s/%s",URL, endpoint));
		CloseableHttpClient client = HttpClients.createDefault();
		
		String data = obj.toJSONString();
		
		//System.out.println(data);
		StringEntity entity;

		entity = new StringEntity(data, "UTF-8");    
		
		post.setEntity(entity);
		post.setHeader("Accept", "application/json");
		post.setHeader("Content-type", "application/json;charset=UTF-8");
		
		try {
			CloseableHttpResponse response = client.execute(post);
			int status = response.getStatusLine().getStatusCode();
			
			if(status != 200 && status != 201 && status != 202){
				logger.error(String.format("HTTP Post Request for document %s have failed with status code %d", obj.get("name"), status));
				post.releaseConnection();
				return null;
			}

			JSONObject resp = parseElasticsearchResponse(response);
			
			// Free the post request
			post.releaseConnection();
			String id = (String)resp.get("_id");
			
			logger.info(String.format("HTTP Post Request for document %s have succeeded with status code %d and new ID %s", obj.get("name"), status, id));
			
			return id;
			
				
		} catch (IOException  e) {
			logger.fatal(e);
		}
		
		return null;
	}
	

	public static String sendAsBulk(JSONObject obj){
		HttpPost post = new HttpPost(String.format("%s/_bulk",URL));
		CloseableHttpClient client = HttpClients.createDefault();
		
		JSONObject meta = new JSONObject();
		meta.put("_index", "library");
		meta.put("_type", "books");
		
		JSONObject root = new JSONObject();
		root.put("index", meta);
		String data = obj.toJSONString();
		
		String bulkData = root+"\n"+data+"\n";
		
		//System.out.println(data);
		StringEntity entity;
		entity = new StringEntity(bulkData, "UTF-8");    
		
		post.setEntity(entity);
		post.setHeader("Accept", "application/json");
		post.setHeader("Content-type", "application/json;charset=UTF-8");
		try {
			CloseableHttpResponse response = client.execute(post);
			int status = response.getStatusLine().getStatusCode();
			
			if(status != 200 && status != 201 && status != 202){
				logger.error(String.format("HTTP Post Request for document %s have failed with status code %d", obj.get("name"), status));
				post.releaseConnection();
				return null;
			}

			JSONObject resp = parseElasticsearchResponse(response);
			
			// Free the post request
			post.releaseConnection();
			String id = (String)((JSONObject)((JSONObject)((JSONArray)resp.get("items")).get(0)).get("index")).get("_id");
			
			logger.info(String.format("HTTP Post Request for document %s have succeeded with status code %d and new ID %s", obj.get("name"), status, id));
			
			return id;
			
				
		} catch (IOException  e) {
			logger.fatal(e);
		} 
		
		return null;
	}
}
