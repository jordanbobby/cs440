import java.io.*;
import java.util.*;

import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.serial.TupleSerialKeyCreator;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.tuple.*;
import com.sleepycat.db.*;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.collections.StoredValueSet;

public class EntryPoint 
{
	private static EnvironmentConfig envConfig_ = null;
    private static Environment env_ = null;
    //private static File envHome_ = new File("./cs440_hw2_env");
    private static String strEnvPath = "./";
    // DatabaseEntries used for loading records
    private static DatabaseEntry theKey = new DatabaseEntry();
    private static DatabaseEntry theData = new DatabaseEntry();
    //The file to locate if for task # 1
    private static String locateFile = "561004.xml";
    
    // Encapsulates the databases.
    private static MyDbs myDbs = new MyDbs();
    
    // Usage Message
    private static void usage() {
        System.out.println("EntryPoint [-t <task number>]");
        System.out.println("           [-h <database home>]");
        System.out.println("           [-i <imdb input directory>]");
        System.exit(-1);
    }
    
    //private static DatabaseConfig dbConfig_ = null;
    //private static Database imdbDataDb_ = null;
    
    public static void main (String []args) throws Exception{
    	//Enable when finished, or maybe add a new function to parse args
    	/*
    	
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
    	*/
    	int taskArg = 2; // for debugging only, this value will come from args
        switch (taskArg) {
        	//1- [20 points] Inserting the information about all files in the data set into a Berkeley DB table.
            case 1:
            	//createEnv();
            	//createDbHandle(DatabaseType.HASH);
            	strEnvPath = "./cs440_hw2_env"; // for debugging only, disable when finished
            	myDbs.setup(strEnvPath);
            	insertEntries();
            	//retrieveEntries();
            	break;
                     
            //2- [10 points] Answering point queries for the file name over the table. 
            //For example, finding a file whose name is “282441.xml”. 
            case 2:
            	System.out.println("Query1: Answering point queries for the file name over the table.");
            	strEnvPath = "./cs440_hw2_env"; // for debugging only, disable when finished
            	myDbs.setup(strEnvPath);
            	searchEntry();
            	//createEnv();
            	//createDbHandle(DatabaseType.HASH);
            	//queryFileName();
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
        //imdbDataDb_.close();
        //env_.close();
        myDbs.close();
        System.out.println("Done");
    }
    
    /*
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
	
	private static void createDbHandle(DatabaseType type) throws Exception
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
    */
	private static void insertEntries() throws Exception 
	{
		System.out.println("Inserting Entries...");
		//EntryBinding binding = null;
		// Now load the data into the database. The item's fileName is the
        // key, and the data is an ImdbData class object.

        // Need a tuple binding for the Inventory class.
        TupleBinding imdbBinding = new ImdbDataTupleBinding();
		//ImdbDataTupleBinding binding = new ImdbDataTupleBinding();
		addImdbDataEntries(imdbBinding);
		
	}
	private static void searchEntry() throws Exception 
	{
		System.out.println("Searching for file " + locateFile + "...");
		TupleBinding imdbBinding = new ImdbDataTupleBinding();
		if (locateFile != null) {
            showFile(imdbBinding);
        } else {
            showAllFiles(imdbBinding);
        }
	}
	private static void showFile(TupleBinding binding) throws DatabaseException 
	{
        SecondaryCursor secCursor = null;
        try {
            // searchKey is the key that we want to find in the
            // secondary db.
            DatabaseEntry searchKey =
                new DatabaseEntry(locateFile.getBytes("UTF-8"));

            // foundKey and foundData are populated from the primary
            // entry that is associated with the secondary db key.
            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();

            // open a secondary cursor
            secCursor =
                myDbs.getNameIndexDB().openSecondaryCursor(null, null);

            // Search for the secondary database entry.
            OperationStatus retVal =
                secCursor.getSearchKey(searchKey, foundKey,
                    foundData, LockMode.DEFAULT);

            // Display the entry, if one is found. Repeat until no more
            // secondary duplicate entries are found
            while(retVal == OperationStatus.SUCCESS) {
            	ImdbData imdbRecord = (ImdbData)binding.entryToObject(foundData);
                //Inventory theInventory =
                //    (Inventory)inventoryBinding.entryToObject(foundData);
                displayImdbRecord(foundKey, imdbRecord);
                retVal = secCursor.getNextDup(searchKey, foundKey,
                    foundData, LockMode.DEFAULT);
            }
        } catch (Exception e) {
            System.err.println("Error on inventory secondary cursor:");
            System.err.println(e.toString());
            e.printStackTrace();
        } finally {
            if (secCursor != null) {
                secCursor.close();
            }
        }
    }
	private static void displayImdbRecord(DatabaseEntry theKey,
            ImdbData imdbRecord) throws DatabaseException {
		String fileName = new String(theKey.getData());
        System.out.println(fileName + "(" + imdbRecord.getFileSize() + ")");
        System.out.println("Content:\n");
        System.out.println(imdbRecord.getContent());
        System.out.println("\n");
	}
	private static void showAllFiles(TupleBinding binding) throws DatabaseException {
	    // Get a cursor
	    //Cursor cursor = myDbs.getInventoryDB().openCursor(null, null);
		Cursor cursor = myDbs.getImdbDataDB().openCursor(null, null);
	
	    // DatabaseEntry objects used for reading records
	    DatabaseEntry foundKey = new DatabaseEntry();
	    DatabaseEntry foundData = new DatabaseEntry();
	
	    try { // always want to make sure the cursor gets closed
	        while (cursor.getNext(foundKey, foundData,
	                    LockMode.DEFAULT) == OperationStatus.SUCCESS) {
	        	ImdbData imdbRecord =
	                (ImdbData)binding.entryToObject(foundData);
	        	displayImdbRecord(foundKey, imdbRecord);
	        }
	    } catch (Exception e) {
	        System.err.println("Error on inventory cursor:");
	        System.err.println(e.toString());
	        e.printStackTrace();
	    } finally {
	        cursor.close();
	    }
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
		
		/*
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
		*/
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
            //cursor = imdbDataDb_.openCursor(null, null);
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
	
	private static void addImdbDataEntries(TupleBinding binding) throws Exception 
	{   
        try {
        	File currentDir = new File("./imdb"); // debugging only, it will come from args later
        	scanFiles(currentDir, binding);
        }
        catch (DatabaseException e)
        {
            System.err.println("addPersonEntries: " + e.toString());
            throw e;
        }
	}
	
	private static void scanFiles(File dir, 
			TupleBinding binding) throws DatabaseException
    {
    	try {
			File[] files = dir.listFiles();
			for (File file : files) {
				if (file.isDirectory()) {
					System.out.println("scanning directory:" + file.getCanonicalPath());
					scanFiles(file, binding);
				} else {
					StringBuilder fileContent = getFileContent(file);
					theKey = new DatabaseEntry(file.getName().getBytes("UTF-8"));
					ImdbData imdbRecord = new ImdbData();
					imdbRecord.setFileName(file.getName());
					imdbRecord.setFileSize((int)file.length());
					imdbRecord.setContent(fileContent.toString());
					binding.objectToEntry(imdbRecord, theData);
					myDbs.getImdbDataDB().put(null, theKey, theData);
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
/*
class sKeyCreator implements SecondaryKeyCreator {
	  public boolean createSecondaryKey (SecondaryDatabase  secDb, DatabaseEntry keyEntry, DatabaseEntry dataEntry, DatabaseEntry resultEntry)
	  {
	      //set resultEntry to the secondary key value
		  resultEntry = keyEntry;
		  return true;
	  }
}
*/