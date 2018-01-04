package com.persistent.hive;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;



public class CouchDbOutputformat implements HiveOutputFormat<LongWritable, Text>{
	//OutputFormat outputforamt;
	private static final Log LOG = LogFactory.getLog(CouchDbOutputformat.class.getName());
	
	CouchDbOutputformat(){
		//this.outputforamt= new OutputFormat();
	}
	@Override
	public void checkOutputSpecs(FileSystem arg0, JobConf arg1)
			throws IOException {
		LOG.info("In checkopspec");
		// TODO Auto-generated method stub
		
	}

	@Override
	public RecordWriter<LongWritable, Text> getRecordWriter(FileSystem arg0,
			JobConf arg1, String arg2, Progressable arg3) throws IOException {
		// TODO Auto-generated method stub
		LOG.info("In recordwrite trying");
		
		return getRecordWriter(arg0, arg1, arg2, arg3);
	}

	@Override
	public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(
			JobConf job, Path arg1, Class<? extends Writable> arg2,
			boolean arg3, Properties arg4, Progressable arg5)
			throws IOException {
		LOG.info("In hive record writer");
		// TODO Auto-generated method stub
		CouchDbRecordWriter w = new CouchDbRecordWriter(job);
		return w;
	}

}
