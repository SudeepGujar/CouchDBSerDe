package com.persistent.hive;

import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

public class CouchDbHivemetahook implements HiveMetaHook{

	@Override
	public void commitCreateTable(Table arg0) throws MetaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commitDropTable(Table arg0, boolean arg1) throws MetaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void preCreateTable(Table arg0) throws MetaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void preDropTable(Table arg0) throws MetaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rollbackCreateTable(Table arg0) throws MetaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rollbackDropTable(Table arg0) throws MetaException {
		// TODO Auto-generated method stub
		
	}

}
