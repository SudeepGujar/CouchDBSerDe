package com.persistent.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.lightcouch.CouchDbClient;

import parquet.hadoop.api.InitContext;

import com.google.gson.JsonObject;




public class CouchDbRecordreader implements RecordReader<LongWritable, Text>{
	private static final Log LOG = LogFactory.getLog(CouchDbRecordreader.class.getName());
	public  List<String> Couchdb_Conf_key = Arrays.asList("dbName","protocol","host","port","username","password");
	 public  List<String> Couchdb_Conf_def = Arrays.asList(null,"http","localhost","5984",null,null);
	 HashMap<String, String> Couchdb_Conf = new HashMap<String,String>();
	 List<String> Columns = new ArrayList<String>();
	 List<String> Columns_type = new ArrayList<String>();
	int i = 0; 
	char nullchar = 0;
	char c =1;
	boolean init = true;
	String  name, branch;
	int mis;
	CouchDbClient dbClient; 
	List<JsonObject> allDocs;
	
	
	public CouchDbRecordreader(String startkey, String endkey, JobConf job) {
		// TODO Auto-generated constructor stub
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
		 
		 dbClient = new CouchDbClient(Couchdb_Conf.get("dbName"), true, 
				 Couchdb_Conf.get("protocol"), 
				 Couchdb_Conf.get("host"), 				 
				 Integer.parseInt( Couchdb_Conf.get("port")),
				 Couchdb_Conf.get("username"),
				 Couchdb_Conf.get("password"));
		 
		
		 allDocs = dbClient.view("_all_docs").includeDocs(true)
				.startKey(startkey).endKey(endkey).query(JsonObject.class);
		 
		 LOG.error("allDocs = " + allDocs +"\n" );
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		LOG.info("In close");
		
	}

	@Override
	public LongWritable createKey() {
		// TODO Auto-generated method stub
		LOG.info("In create key");
		LongWritable k = new LongWritable();
		k.set(0);
		return k;
	}

	@Override
	public Text createValue() {
		// TODO Auto-generated method stub
		LOG.info("In createvalue");
		Text t = new Text();
		t.set("1");
		return t;
	}

	@Override
	public long getPos() throws IOException {
		LOG.info("In getpos");
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getProgress() throws IOException {
		LOG.info("In getProgress");
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean next(LongWritable arg0, Text arg1) throws IOException {
		// TODO Auto-generated method stub
		LOG.info("In next " + i + " allDocs.size() = " + allDocs.size() + " arg1.get = " + arg1.getLength() );
		
		//arg1.set("8012,don,batman,train");
		
		//false here		
			
		if(i <allDocs.size()){ 
			
			String temprow = "";
			JsonObject onedoc = allDocs.get(i);
			int temp_index = 0;
			for(String col: Columns){
				if(onedoc.has(col)){
					String value = "";
					if (!onedoc.get(col).isJsonNull())
						value = onedoc.get(col).getAsString();	
					else 
					LOG.info("init val = " + value );
					/*if( value == null || value.equals("null")){
						value = "";
						LOG.info("val = " + value );
					}else if(Columns_type.get(temp_index).equals("string") && value.charAt(0) == '"'){
						value = value.substring(1, value.length()-1);
						LOG.info("val = " + value );
					}*/
					temprow = temprow + value + c;
					LOG.info("temprow = " + temprow );	
					//LOG.info("temprow = " + temprow );	
				}
				else{
					temprow = temprow+c;
				}
				temp_index++;	
			}
			LOG.info("temprow = " + temprow );			
			arg1.set(temprow);
			i++;
			return true;
		}
		else
			return false;
	}
}




/*
 * if(onedoc.get("mis") != null){	//onedoc.has("mis")
				mis = Integer.parseInt(onedoc.get("mis").toString());
				//System.out.println("NAME : "+d);				
			}
			if(onedoc.has("name")){	
				name= onedoc.get("name").toString();
				//System.out.println("MARK : "+s);				
			}
			
			if(onedoc.has("branch")){		
				branch = onedoc.get("branch").toString();
				//System.out.println("ROLL : "+branch);
				
			}
			arg1.set(""+mis+c+name+c+branch+c+branch);
			//arg1.set(""+mis+","+name+","+branch+","+branch);
			 * 
			 * 
			 */
