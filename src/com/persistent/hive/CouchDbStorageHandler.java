package com.persistent.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;

@SuppressWarnings("deprecation")
public class CouchDbStorageHandler implements HiveStorageHandler {

	 public static List<String> Columns = new ArrayList<String>();
	 public static List<String> Couchdb_Conf = Arrays.asList("dbName","protocol","host","port","username","password");
	 public static List<String> Couchdb_Conf_val = Arrays.asList(null,"http","localhost","5984",null,null);
	 private static final Log LOG = LogFactory.getLog(CouchDbStorageHandler.class.getName());
	 private Configuration conf;
	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		LOG.info("In getconf");
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub
		LOG.info("In setconf");
		this.conf = conf;
	}

	@Override
	public void configureInputJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		// TODO Auto-generated method stub
		LOG.info("In configureInputJobProperties");
		configureJobProperties(tableDesc, jobProperties);
		
	}

	private void configureJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		// TODO Auto-generated method stub
		/*
		 if(LOG.isDebugEnabled()) {
			 LOG.debug("tabelDesc: " + tableDesc);
			 LOG.debug("jobProperties: " + jobProperties);
			 }
		 
		 LOG.error("tabelDesc: " + tableDesc);
		 LOG.error("jobProperties: " + jobProperties);
		 
		 String tblName = tableDesc.getTableName();
		 Properties tblProps = tableDesc.getProperties();
		 String columnNames = tblProps.getProperty(Constants.LIST_COLUMNS);
		 Columns = Arrays.asList(columnNames.split(","));
		// LOG.error("columnNames: " + columnNames + " tblName : " + tblName  );
		
		 
		 for(String key : tblProps.stringPropertyNames()) {
			 if(key.startsWith("map.column.")) {
				 String value = tblProps.getProperty(key);
				 jobProperties.put(key, value);
				 key = key.substring(11);
				 if(Columns.contains(key)){
					 int temp_index = Columns.indexOf(key);
					 Columns.set( temp_index,value);
					 LOG.debug("found = " + key +"at =" +  temp_index + Columns);
				 }
				 LOG.error("key: " + key + " value : " + value + " contains "+ Columns.contains(key) );
				 
			 }
			 if(key.startsWith("couchdb.conf.")){
				 String value = tblProps.getProperty(key);
				 jobProperties.put(key, value);
				 key = key.substring(13);
				 if(Couchdb_Conf.contains(key)){
					 Couchdb_Conf_val.set( Couchdb_Conf.indexOf(key),value);
				 }
				 LOG.error("key: " + key + " value : " + value  );
			 }
		 }
		 LOG.error("Column names = " + Columns);
		 LOG.error("conf val = " + Couchdb_Conf_val);
		 
		 if(Couchdb_Conf_val.get(0) == null){
			try {
				throw new Exception("Database name may not be specified for Couchdb \nExpected couchdb.conf.dbName = Database_Name");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 }
		 */
	}

	@Override
	public void configureJobConf(TableDesc arg0, JobConf arg1) {
		// TODO Auto-generated method stub
		LOG.info("In configureJobConf");
		//configureJobProperties(tableDesc, jobProperties);
	}

	@Override
	public void configureOutputJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		// TODO Auto-generated method stub
		LOG.info("In configureOutputJobProperties");
		configureJobProperties(tableDesc, jobProperties);
	}

	@Override
	@Deprecated
	public void configureTableJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		// TODO Auto-generated method stub
		LOG.info("In configureTableJobProperties");
		configureJobProperties(tableDesc, jobProperties);
	}

	@Override
	public HiveAuthorizationProvider getAuthorizationProvider()
			throws HiveException {
		// TODO Auto-generated method stub
		LOG.info("In HiveAuthorizationProvider");
		return new DefaultHiveAuthorizationProvider();
		
	}

	@Override
	public Class<? extends InputFormat> getInputFormatClass() {
		// TODO Auto-generated method stub
		LOG.info("In InputFormat");
		return CouchDbInputformat.class;
	}

	@Override
	public HiveMetaHook getMetaHook() {
		// TODO Auto-generated method stub
		LOG.info("In getMetaHook");
		CouchDbHivemetahook hivemetahook = new CouchDbHivemetahook();
		return hivemetahook;
	}

	@Override
	public Class<? extends OutputFormat> getOutputFormatClass() {
		// TODO Auto-generated method stub
		LOG.info("In OutputFormat");
		return CouchDbOutputformat.class;
	}

	@Override
	public Class<? extends SerDe> getSerDeClass() {
		// TODO Auto-generated method stub
		LOG.info("In SerDe");
		return CouchDbSerDe.class;
	}
	
	

}
