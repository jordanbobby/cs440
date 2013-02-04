import java.io.File;
import java.io.FileNotFoundException;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.db.*;

public class BdbAssignment2 {

	public static String databaseFileHome = "./berkdb/";
	
	public static void main(String[] args) throws FileNotFoundException, DatabaseException{
		//initialize database
		System.out.println("Hello world");
		
		EnvironmentConfig envConfig_ = new EnvironmentConfig();
		
		envConfig_.setAllowCreate(true);
		envConfig_.setInitializeCache(true);
		envConfig_.setTransactional(false);
		envConfig_.setInitializeLocking(false);
		envConfig_.setPrivate(true);
		envConfig_.setCacheSize(1024 * 1024);
		
		File envHome_ = new File(databaseFileHome);
		Environment env_ = new Environment(envHome_, envConfig_);
		
		
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setType(DatabaseType.HASH);
		dbConfig.setCacheSize(4 * 1024 * 1024);
		
		Database db; 
		
		db = env_.openDatabase(null, "imdb.db", "imdb", dbConfig);
		
		ImdbData entry = new ImdbData();
		entry.setFileName(45);
		entry.setFileSize(1000);
		entry.setContent("this is some content");
		
		
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		int keyvalue  = 45;
		
		IntegerBinding.intToEntry(keyvalue, key);
		ImdbDataTupleBinding binding = new ImdbDataTupleBinding();
		binding.objectToEntry(entry, data);
		
		db.put(null, key, data);
		
		
		//Retrieve the data.
		key = new DatabaseEntry();
		data = new DatabaseEntry();
		IntegerBinding.intToEntry(keyvalue, key);
		
		while(db.get(null, key, data, null) == OperationStatus.SUCCESS){
			
			ImdbData x = (ImdbData)binding.entryToObject(data);
			
			System.out.println(x.toString());
		}
		
		
	}
}
