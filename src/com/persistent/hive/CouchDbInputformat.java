package com.persistent.hive;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONException;
import org.json.JSONObject;
import org.lightcouch.CouchDbClient;

import com.google.gson.JsonObject;


public class CouchDbInputformat implements InputFormat<LongWritable, Text>{
	
	//InputSplit[] inputSplits = new InputSplit[5];
	CouchDbInputsplit inputsplit;
	CouchDbClient dbClient;
	List<JsonObject> allDocs;
	
	int COUNT,SPLITS;
	 public  List<String> Couchdb_Conf_key = Arrays.asList("dbName","protocol","host","port","username","password");
	 public  List<String> Couchdb_Conf_def = Arrays.asList(null,"http","localhost","5984",null,null);
	 
	 List<String> Columns = new ArrayList<String>();
	// JobConf job;
		private static final Log LOG = LogFactory.getLog(CouchDbInputformat.class.getName());
		
		 public CouchDbInputformat() {
				// TODO Auto-generated constructor stub
			 LOG.info("In constructor");
			 
			}
		 
		 
	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
			JobConf job, Reporter arg2) throws IOException {
		JsonObject last= null;
		/*if(DOC_COUNT*i < COUNT)
		  last = allDocs.get(DOC_COUNT*i);
		if(last != null && last.has("id")){
			lastkey = last.get("id").toString();
			lastkey = lastkey.substring(1, lastkey.length()-1);
		}*/
		
		String startkey = ((com.persistent.hive.CouchDbInputsplit)split).getstartkey();
		String endkey = ((com.persistent.hive.CouchDbInputsplit)split).getendkey();
		
		CouchDbRecordreader r = new CouchDbRecordreader(startkey,endkey,job);
		
		// TODO Auto-generated method stub
		LOG.info("In RecordReader i = "  );
		return r;
	} 

	@Override
	public InputSplit[] getSplits(JobConf job, int arg1) throws IOException {
		// TODO Auto-generated method stub
		LOG.info("In getSplits");
		HashMap<String, String> Couchdb_Conf = new HashMap<String,String>();
		
	/*	DOC_COUNT = 2;		
		//this.job = job;
		
		String columnNames = job.get("couchdb.customserde.col_names");
		 Columns = Arrays.asList(columnNames.split(","));
		 LOG.error("columnNames: " + columnNames + " Columns : " + Columns  );
		 
		 for(String col: Columns){
			 String val = job.get("map.column."+ col);
			 LOG.error("val: " + val );
			 if(val != null){
				 int temp_index = Columns.indexOf(col);
				 LOG.error("found = " + val +"at =" +  temp_index + Columns);
				 Columns.set( temp_index,val);
				 LOG.error("found = " + val +"at =" +  temp_index + Columns);
			 }
			 
			}*/
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
		 
		 dbClient = new CouchDbClient(Couchdb_Conf.get("dbName"), true, 
				 Couchdb_Conf.get("protocol"), 
				 Couchdb_Conf.get("host"), 				 
				 Integer.parseInt( Couchdb_Conf.get("port")),
				 Couchdb_Conf.get("username"),
				 Couchdb_Conf.get("password"));
		 
		 allDocs = dbClient.view("_all_docs").limit(1).query(JsonObject.class);
		 JsonObject first_json = allDocs.get(0);
		 allDocs = dbClient.view("_all_docs").limit(1).descending(true).query(JsonObject.class);
		 JsonObject last_json = allDocs.get(0);
		 
		 String first = first_json.get("id").getAsString();
		 String last = last_json.get("id").getAsString();
		 
		 
		 
		 COUNT = couchdb_doc_count(Couchdb_Conf);
		 SPLITS = 1;
		/* SPLITS = COUNT/2;
		 if(COUNT % 2 != 0)
			 SPLITS = SPLITS + 1; */
		 
		BigInteger num1 = new BigInteger(first.getBytes(Charset.forName("ISO-8859-1")));
		BigInteger num2 = new BigInteger(last.getBytes(Charset.forName("ISO-8859-1")));
		num2 = num2.add(BigInteger.ONE);
			
		BigInteger step = num2.subtract(num1);
		BigInteger splt = new BigInteger(""+SPLITS);
		step = step.divide(splt);
		step = step.add(BigInteger.ONE);
		
		String startkey,endkey;
		startkey = first;
		endkey = first;
		 
		 
		 
		 
		 LOG.error("count = "+ COUNT +" SPLITS = " + SPLITS);
		 
		 
		InputSplit[] inputSplits = new InputSplit[SPLITS];
		Path path = new Path(job.get("location"));
		
				
		
		for(int temp=0;temp<SPLITS;temp++){
			
			startkey = endkey;
			endkey = getendkey(startkey,step);
			
			inputsplit = new CouchDbInputsplit(startkey,endkey, path);
			inputSplits[temp] = inputsplit;
		}
		
		return inputSplits;
	}


	private String getendkey(String startkey, BigInteger step) {
		// TODO Auto-generated method stub
		BigInteger num1 = new BigInteger(startkey.getBytes(Charset.forName("ISO-8859-1")));
		num1 = num1.add(step);		
		return new String (num1.toByteArray(),Charset.forName("ISO-8859-1"));
	}


	private int couchdb_doc_count(HashMap<String, String> couchdb_Conf) throws IOException {
		// TODO Auto-generated method stub
		int count = 0;
		String url = couchdb_Conf.get("protocol") + "://" + couchdb_Conf.get("host") + ":"+ couchdb_Conf.get("port")
				+"/" + couchdb_Conf.get("dbName") + "/_all_docs?limit=0";
		
		 LOG.error("url = "+ url );
		 
		HttpGet req = new HttpGet(url);
		HttpClient client = new DefaultHttpClient();
		HttpResponse response = null;
		try {
			response = client.execute(req);
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BufferedReader r = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
		StringBuilder sb = new StringBuilder();
		String s = null;
		while ((s = r.readLine()) != null) {
		sb.append(s);
		}
		try {
			JSONObject countjson = new JSONObject(sb.toString());
			count = countjson.optInt("total_rows");
			
			LOG.error("count = "+ count + " \n sb : " + sb);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return count;
	}
	

}

