package com.persistent.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.mapred.FileSplit;




public class CouchDbInputsplit extends FileSplit{
	int SPLITS;
	String regionLocation;
	String Startkey, Endkey;
	private static final Log LOG = LogFactory.getLog(CouchDbInputformat.class.getName());
	
	public String getstartkey(){
		return this.Startkey;
	}
	
	public String getendkey(){
		return this.Endkey;
	}
	
	public CouchDbInputsplit() {
		// TODO Auto-generated constructor stub
		
		this("","", new Path("/"));
		LOG.info("in hiveinputsplit ");
	}

	public CouchDbInputsplit(String startkey, String endkey, Path path) {
		// TODO Auto-generated constructor stub
		super(path, 0, 0, (String[]) null);
		this.Startkey = startkey;
		this.Endkey = endkey;		
	}
	

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
		
		 Startkey = UTF8.readString(in);
		 Endkey =  UTF8.readString(in);
		 super.readFields(in);
		 LOG.info("Startkey " + Startkey + "\t Endkey :" + Endkey);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		/*out.writeChars(Startkey);
		out.writeChars(Endkey)*/;
		
		UTF8.writeString(out, Startkey);
		UTF8.writeString(out, Endkey);
		super.write(out);
	}
/*
	@Override
	public long getLength() {
		// TODO Auto-generated method stub
		return SPLITS;
	}*/

/*	
	@Override
	public String[] getLocations() throws IOException {
		// TODO Auto-generated method stub
		LOG.info("in readFields " + regionLocation);
		String[] strarry = new String[1];
		strarry[0] = "localhost";
		return new String[]{};
		
	}*/
	
	


}
