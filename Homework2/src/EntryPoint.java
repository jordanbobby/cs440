
import java.io.File;
import java.io.FileNotFoundException;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.db.*;

public class EntryPoint
{
	
	private static EnvironmentConfig envConfig = new EnvironmentConfig();
	
	private static File envHome_ = new File("/nfs/stak/students/m/mcconnjo/cs440/Homework2/resource/");
	
	private static Environment env_;
	
 
    public static void main(String[] args) throws FileNotFoundException, DatabaseException{
    	createEnv();
    	System.out.println(env_.getHome());
    	envConfig = new EnvironmentConfig();
		envConfig.setErrorStream(System.err);
		envConfig.setErrorPrefix("cs440_hw2");
		envConfig.setAllowCreate(true);
		envConfig.setInitializeCache(true);
		envConfig.setTransactional(false);
		envConfig.setPrivate(true);
		envConfig.setInitializeLocking(false);
		envConfig.setCacheSize(1024 * 1024);
		
    	System.out.println("hello world");
    	
    	DatabaseConfig dbConfig = new DatabaseConfig();
    	
		dbConfig.setType(DatabaseType.BTREE);
		dbConfig.setCacheSize(4 * 1024 * 1024);
		dbConfig.setCreateDir(envHome_);
		
		Database db; 
		dbConfig.setAllowCreate(true);
		dbConfig.setTransactional(true);
		db = env_.openDatabase(null,"resource", null, dbConfig);
			
		
		ImdbData entry = new ImdbData();
		entry.setFileName("45.xml");
		entry.setFileSize(1000);
		entry.setContent("this is some content");
		
		
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		String keyvalue  = "45.xml";
		
		StringBinding.stringToEntry(keyvalue, key);
		ImdbDataTupleBinding binding = new ImdbDataTupleBinding();
		binding.objectToEntry(entry, data);
		
		db.put(null, key, data);
		
		
		//Retrieve the data.
		key = new DatabaseEntry();
		data = new DatabaseEntry();
		StringBinding.stringToEntry(keyvalue, key);
		
		while(db.get(null, key, data, null) == OperationStatus.SUCCESS){
			
			ImdbData x = (ImdbData)binding.entryToObject(data);
			
			System.out.println(x.toString());
		}
    	
    }
	private static void createEnv()
	{
		try
	    {
			envConfig = new EnvironmentConfig();
			envConfig.setErrorStream(System.err);
			envConfig.setErrorPrefix("cs440_hw2");
			envConfig.setAllowCreate(true);
			envConfig.setInitializeCache(true);
			envConfig.setTransactional(false);
			envConfig.setPrivate(true);
			envConfig.setInitializeLocking(false);
			envConfig.setCacheSize(1024 * 1024);
			env_ = new Environment(envHome_, envConfig);
		}
		catch(DatabaseException e)
		{
			System.err.println("createEnv: Database Exception caught " +
	                    e.toString());
		}
		catch(Exception e)
		{
			System.err.println("createEnv: " + e.toString());
		}
	}
}