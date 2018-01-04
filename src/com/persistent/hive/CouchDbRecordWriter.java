package com.persistent.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONException;
import org.json.JSONObject;

public class CouchDbRecordWriter implements org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter{
	private static final Log LOG = LogFactory.getLog(CouchDbRecordreader.class.getName());
	
	 public  List<String> Couchdb_Conf_key = Arrays.asList("dbName","protocol","host","port","username","password");
	 public  List<String> Couchdb_Conf_def = Arrays.asList(null,"http","localhost","5984",null,null);
	 HashMap<String, String> Couchdb_Conf = new HashMap<String,String>();
	 List<String> Columns = new ArrayList<String>();
	 List<String> Columns_type = new ArrayList<String>();
	
	public CouchDbRecordWriter(JobConf job){
		String columnNames = job.get("couchdb.customserde.col_names");
		String columnTypes = job.get("couchdb.customserde.col_types");
		
		 Columns = Arrays.asList(columnNames.split(","));
		 Columns_type = Arrays.asList(columnTypes.split(":"));
		 LOG.error("columnNames: " + columnNames + " Columns : " + Columns  );
		 LOG.error("columnTypes: " + columnTypes + " Columns_type : " + Columns_type  );
		 for(String col: Columns){
			 String val = job.get("map.column."+ col);
			 LOG.error("val: " + val );
			 if(val != null){
				 int temp_index = Columns.indexOf(col);
				 LOG.error("found = " + val +"at =" +  temp_index + Columns);
				 Columns.set( temp_index,val);
				 LOG.error("found = " + val +"at =" +  temp_index + Columns);
			 }
		 }
		 
		 int temp_index=0;
		 for(String key: Couchdb_Conf_key){
			 String val = job.get("couchdb.conf."+ key);
			 
			 LOG.error("conf_val: " + val );
			 if(val != null){
				 Couchdb_Conf.put(key,val);
				 LOG.error("found = " + val +"at =" +  temp_index + Couchdb_Conf);
			 }
			 else{
				 Couchdb_Conf.put(key, Couchdb_Conf_def.get(temp_index));
				 LOG.error("found = " + val +"at =" +  temp_index + Couchdb_Conf);
			 }
			 temp_index++;
			}
		
	}

	@Override
	public void close(boolean arg0) throws IOException {
		// TODO Auto-generated method stub
		LOG.info("In close");
		
	}

	@Override
	public void write(Writable arg0) throws IOException {
		// TODO Auto-generated method stub
		LOG.info("In write : "+arg0);
		String str = arg0.toString();
		List<String> items = Arrays.asList(str.split("[^a-zA-Z0-9'\"]+"));
		JSONObject json = new JSONObject();
		
		for(String col: Columns){
			int temp_index = Columns.indexOf(col);
			try {
				json.put(col, items.get(temp_index));
				LOG.info("json : "+json);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		 CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		 String url = Couchdb_Conf.get("protocol") + "://" + Couchdb_Conf.get("host") + ":"+ Couchdb_Conf.get("port")
					+"/" + Couchdb_Conf.get("dbName") + "/";

		 try {
		     HttpPost request = new HttpPost(url);
		     StringEntity params = new StringEntity(json.toString());
		     request.addHeader("content-type", "application/json");
		     request.setEntity(params);
		     httpClient.execute(request);
		     System.out.println(" request " + request);
		 // handle response here...
		 } catch (Exception ex) {
		     // handle exception here
		 } finally {
		     try {
				httpClient.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 }
		
	}

	

}
