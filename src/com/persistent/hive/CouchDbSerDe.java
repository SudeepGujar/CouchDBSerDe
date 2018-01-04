package com.persistent.hive;
import java.util.Arrays;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;


public class CouchDbSerDe extends LazySimpleSerDe{
	 public CouchDbSerDe() throws SerDeException {
		super();
		//TODO Auto-generated constructor stub
	}
	 
	 @Override
	public void initialize(Configuration job, Properties tblProps)
			throws SerDeException {
		// TODO Auto-generated method stub
		super.initialize(job, tblProps);
		char nullchar = 0;
		job.set("couchdb.customserde.col_names", tblProps.getProperty(Constants.LIST_COLUMNS));
		job.set("couchdb.customserde.col_types", tblProps.getProperty(Constants.LIST_COLUMN_TYPES));
		
		tblProps.put("serialization.null.format", nullchar);
		
		
		//job.set("serialization.null.format", nullstr);
		
		for(String key : tblProps.stringPropertyNames()) {
			 if(key.startsWith("map.column.")) {
				 String value = tblProps.getProperty(key);
				 job.set(key, value);				 
				 LOG.error("key: " + key + " value : " + value   );
				 
			 }
			 if(key.startsWith("couchdb.conf.")){
				 String value = tblProps.getProperty(key);
				 job.set(key, value);
				 LOG.error("key: " + key + " value : " + value  );
			 }
		 }
	}

}