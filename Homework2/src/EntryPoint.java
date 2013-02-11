import java.io.*;
import java.util.*;

import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.serial.TupleSerialKeyCreator;
import com.sleepycat.bind.tuple.*;
import com.sleepycat.db.*;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.collections.StoredValueSet;

public class EntryPoint 
{
	private static EnvironmentConfig envConfig_ = null;
    private static Environment env_ = null;
    private static File envHome_ = new File("./cs440_hw2_env");
    
    //private static DatabaseConfig dbConfig_ = null;
    private static Database imdbDataDb_ = null;
    
    /*
    // Hard coded data for now
    static Random rand = new Random();
    private static ImdbData file1 = new ImdbData(new Long(rand.nextLong()), "name1", 10, "hola");
    private static ImdbData file2 = new ImdbData(new Long(rand.nextLong()), "name2", 10, "mundo");
    private static ImdbData file3 = new ImdbData(new Long(rand.nextLong()), "name3", 10, "hello");
    */
    
    public static void main (String []args) throws Exception{
    	
    	int taskArg = 0;
    	if (args.length > 0) {
    	    try {
    	        taskArg = Integer.parseInt(args[0]);
    	        
    	    } catch (NumberFormatException e) {
    	        System.err.println("First Argument must be an integer");
    	        System.exit(1);
    	    }
    	} else {
    		System.err.println("Need an argument");
    		System.exit(1);
    	}
    	
    	taskArg = 1; // for debugging only
        switch (taskArg) {
        	//1- [20 points] Inserting the information about all files in the data set into a Berkeley DB table.
            case 1:
            	createEnv();
            	createDbHandle(DatabaseType.HASH, true);	
            	insertEntries();
            	retrieveEntries();
            	break;
                     
            //2- [10 points] Answering point queries for the file name over the table. 
            //For example, finding a file whose name is “282441.xml”. 
            case 2:
            	System.out.println("Query1: Answering point queries for the file name over the table.");
            	createEnv();
            	createDbHandle(DatabaseType.HASH, false);
            	queryFileName();
            	break;
                     
            //3- [15 points] Answering range queries over file name. 
            //For example, finding all files whose names are from “20000.xml” to “30000.xml”.
            case 3:
            	System.out.println("Query2: Answering range queries over file name.");
            	
            	break;
                     
            //4- [10 points] Answering point queries over file size. 
            //For example, finding all files with size equal to 2000.
            case 4:
            	System.out.println("Query3: Answering point queries over file size. ");
                break;
                     
            //5- [15 points] Answering range queries over file size. 
            //For example, finding all files whose sizes are from 2000 to 3000. 
            case 5:
            	System.out.println("Query4: Answering range queries over file size. ");
                break;
                     
            //6- [20 points] Answering range queries over both file name and file size. 
            //For example, finding all files whose names are from “20000.xml” to “30000.xml”
            //and their sizes range from 2000 to 3000. 
            case 6:
            	System.out.println("Query1: Answering range queries over both file name and file size.");
                break;
                     
            //7- [10 points] Finding all files whose contents contain an input string value. 
            //For example, finding all files that contain a specific string such as “Arnold”.
            case 7:
            	System.out.println("Query1: Finding all files whose contents contain an input string value. ");
                break;
        }
        imdbDataDb_.close();
        env_.close();
        System.out.println("Done");
    }
    
    
	private static void createEnv() throws Exception
	{
		System.out.println("Creating Enviroment...");
		try
	    {
			envConfig_ = new EnvironmentConfig();
			envConfig_.setErrorStream(System.err);
			envConfig_.setErrorPrefix("cs440_hw2");
			envConfig_.setAllowCreate(true);
			envConfig_.setInitializeCache(true);
			envConfig_.setTransactional(false);
			envConfig_.setPrivate(true);
			envConfig_.setInitializeLocking(false);
			envConfig_.setCacheSize(1024 * 1024);
			env_ = new Environment(envHome_, envConfig_);
		}
		catch(DatabaseException e)
		{
			System.err.println("createEnv: Database Exception caught " +
	                    e.toString());
			throw e;
		}
		catch(Exception e)
		{
			System.err.println("createEnv: " + e.toString());
			throw e;
		}
	}
	
	private static void createDbHandle(DatabaseType type, Boolean AllowCreate) throws Exception
    {
		System.out.println("Creating Db handle...");
        try
        {
        	DatabaseConfig dbConfig_ = new DatabaseConfig();
            dbConfig_.setErrorStream(System.err);
            dbConfig_.setType(type);
            dbConfig_.setCacheSize(4 * 1024 * 1024);
            dbConfig_.setAllowCreate(true);
            dbConfig_.setTransactional(false);
            dbConfig_.setErrorPrefix("cs440:ImdbDb");
            imdbDataDb_ = env_.openDatabase(null, "ImdbFile_Db",
                    "imdb", dbConfig_);
        }
        catch(DatabaseException e)
        {
            System.err.println(
                    "createDbHandle: Database Exception caught " +
                    e.toString());
            throw e;
        }
        catch(Exception e)
        {
            System.err.println("createDbHandle: " + e.toString());
            throw e;
        }
    }
	private static void insertEntries() throws Exception 
	{
		System.out.println("Inserting Entries...");
		//EntryBinding binding = null;
		ImdbDataTupleBinding binding = new ImdbDataTupleBinding();
		addImdbDataEntries(binding);
		
	}
	private static void queryFileName() throws Exception
	{
		
		//This block would print the entire table or database
		/*
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		ImdbDataTupleBinding dataBinding = new ImdbDataTupleBinding();
		TupleBinding<Long> keyBinding = ImdbDataTupleBinding.getPrimitiveBinding(Long.class);
		ImdbData fileData = null;
		Long fileKey = null;
		Cursor cursor = imdbDataDb_.openCursor(null, null);
		while(cursor.getNext(key, data, LockMode.DEFAULT)
		                           == OperationStatus.SUCCESS)
		{
			fileKey = (Long)(keyBinding.entryToObject(key));
			fileData = (ImdbData)(dataBinding.entryToObject(data));
		    System.out.println("key: " + fileKey + " value " + fileData);
		}
		*/
		
		
		//new database
		SecondaryConfig sIndexConfig = new SecondaryConfig();
		sIndexConfig.setType(DatabaseType.HASH);
		sIndexConfig.setTransactional(false); 
		// Duplicates are frequently required for secondary databases.
		sIndexConfig.setSortedDuplicates(true); 
		sKeyCreator keyCreator = new sKeyCreator();  
		sIndexConfig.setKeyCreator(keyCreator);
		// Perform the actual open 
		 
		//Not sure why this gives me unexpected file type or format ??
		SecondaryDatabase sIndex = env_.openSecondaryDatabase(null, "ImdbFile_Db", null, imdbDataDb_, sIndexConfig);
         
		//Find a file with name 7000.xml
		String name = "7000.xml";
		DatabaseEntry secKey = new DatabaseEntry(name.getBytes("UTF-8"));
		DatabaseEntry primaryKey = new DatabaseEntry();
		DatabaseEntry value = new DatabaseEntry();
		OperationStatus retVal = sIndex.get(null, secKey, 
				primaryKey, 
				value, 
				LockMode.DEFAULT); 
		System.out.println(retVal);
		
	}
	private static void retrieveEntries() throws Exception
	{
		System.out.println("Retrieving Entries...");
		ImdbDataTupleBinding binding = new ImdbDataTupleBinding();
		
		Cursor cursor = null;
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        
		try
        {
            
            ImdbData imdb = new ImdbData();
            cursor = imdbDataDb_.openCursor(null, null);
            while(cursor.getNext(key, data, null)
                    == OperationStatus.SUCCESS)
            {
                imdb = (ImdbData)(binding.entryToObject(data));
                System.out.println("retrieveEntries: " + imdb);
            }
            cursor.close();
            cursor = null;
            //txn.commit();
        }
        catch (DatabaseException e)
        {
            if(cursor != null)
                cursor.close();
            
            //txn.abort();
            System.err.println("addImdbDataEntries: " + e.toString());
            throw e;
        }
	}
	
	private static void addImdbDataEntries(ImdbDataTupleBinding binding) throws Exception 
	{
		//Transaction txn = null;
        try
        {
            //txn = env_.beginTransaction(null, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            
            /*
            LongBinding.longToEntry(file1.getFileKey(), key);
            binding.objectToEntry(file1, data);
            imdbDataDb_.put(null, key, data);
            
            LongBinding.longToEntry(file2.getFileKey(), key);
            binding.objectToEntry(file2, data);
            imdbDataDb_.put(null, key, data);
            
            LongBinding.longToEntry(file3.getFileKey(), key);
            binding.objectToEntry(file3, data);
            imdbDataDb_.put(null, key, data);
            */
            
            
        	File currentDir = new File("./imdb");
        	scanFiles(currentDir, binding, key, data);
        	
            
            //txn.commit();
        }
        catch (DatabaseException e)
        {
            //txn.abort();
            System.err.println("addPersonEntries: " + e.toString());
            throw e;
        }
	}
	/*
	static Random rand = new Random();
    private static ImdbData file1 = new ImdbData(new Long(rand.nextLong()), "name1", 10, "hola");
    private static ImdbData file2 = new ImdbData(new Long(rand.nextLong()), "name2", 10, "mundo");
    private static ImdbData file3 = new ImdbData(new Long(rand.nextLong()), "name3", 10, "yeah");
	 */
	
	
	private static void scanFiles(File dir, 
			ImdbDataTupleBinding binding, 
			DatabaseEntry key, 
			DatabaseEntry data) throws DatabaseException
    {
    	try {
			File[] files = dir.listFiles();
			for (File file : files) {
				if (file.isDirectory()) {
					System.out.println("scanning directory:" + file.getCanonicalPath());
					scanFiles(file, binding, key, data);
				} else {
					//System.out.println("     file:" + file.getCanonicalPath());
					Random rand = new Random();
					StringBuilder fileContent = getFileContent(file);
					
					ImdbData imdbRecord = new ImdbData(new Long(rand.nextLong()), 
							file.getName(), 
							(int)file.length(), 
							fileContent.toString());
					
					LongBinding.longToEntry(imdbRecord.getFileKey(), key);
		            binding.objectToEntry(imdbRecord, data);
		            imdbDataDb_.put(null, key, data);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (DatabaseException e) {
            System.err.println("scanFiles: " + e.toString());
            throw e;
        }
    }
    
	private static StringBuilder getFileContent(File file)
	{
		StringBuilder content = new StringBuilder();
		try {
            Scanner scanner = new Scanner(file);
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                content.append(line);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return content;
	}
}
class sKeyCreator implements SecondaryKeyCreator {
	  public boolean createSecondaryKey (SecondaryDatabase  secDb, DatabaseEntry keyEntry, DatabaseEntry dataEntry, DatabaseEntry resultEntry)
	  {
	      //set resultEntry to the secondary key value
		  resultEntry = keyEntry;
		  return true;
	  }
}
