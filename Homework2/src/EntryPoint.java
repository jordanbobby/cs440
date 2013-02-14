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
	private static final String strEnvPath = "/scratch/cs440g1";
	// DatabaseEntries used for loading records
	private static DatabaseEntry theKey = new DatabaseEntry();
	private static DatabaseEntry theData = new DatabaseEntry();
	private static ImdbDataTupleBinding binding = new ImdbDataTupleBinding();
	//The file to locate if for task # 1
	private static String ContactPerson = "BenLambert";
	// Encapsulates the databases.
	private static MyDbs myDbs = new MyDbs();

	// Usage Message
	private static void usage(String commandName, int task) {
		switch (task){
		case 0:
			System.out.println(commandName + " 1-7 arg1 arg2 arg3 arg4");
			break;
		case 1:
			System.out.println(commandName + " 1");
			break;
		case 2:
			System.out.println(commandName + " 2 XXXX.xml");
			break;

		case 3:
			System.out.println(commandName + " 3 XXXX.xml XXXX.xml");
			break;
		case 4:
			System.out.println(commandName + " 4 ##");
			break;

		case 5:
			System.out.println(commandName + " 5 #### ####");
			break;
		case 6:
			System.out.println(commandName + " 6 XXXX.xml XXXX.xml #### ####");
			break;
		case 7:
			System.out.println(commandName + " 7 keyword");
		}

		System.exit(-1);
	}

	public static void main (String []args) throws Exception{
		//Enable when finished, or maybe add a new function to parse args
		String argumentKey;
		String lowerKeyName;
		String upperKeyName;
		String contentString;
		String programFileName = "EntryPoint";
		String outputText = "";
		int fileSize;
		int lowerFileSize;
		int upperFileSize;
		FileWriter fStream;
		if(args.length < 1){
			//exit
			usage(programFileName, 0);
		}

		int taskArg = Integer.parseInt(args[0]);
		fStream = new FileWriter(ContactPerson + "_" + taskArg);
		BufferedWriter out = new BufferedWriter(fStream);

		switch (taskArg) {
		//1- [20 points] Inserting the information about all files in the data set into a Berkeley DB table.
		case 1:
			//strEnvPath = "./cs440_hw2_env"; // for debugging only, disable when finished
			//@todo, if folder doesn't exist, create it.


			myDbs.setup(strEnvPath);
			insertEntries();
			outputText = "Files Inserted";
			break;

			//2- [10 points] Answering point queries for the file name over the table. 
			//For example, finding a file whose name is “282441.xml”. 
		case 2:
			if(args.length != 2){
				usage(programFileName, 2);
			}
			argumentKey = args[1];
			System.out.println("Query1: Answering point queries for the file name over the table.");

			myDbs.setup(strEnvPath);
			outputText = searchFileName(argumentKey);
			break;

			//3- [15 points] Answering range queries over file name. 
			//For example, finding all files whose names are from “20000.xml” to “30000.xml”.
		case 3:
			if(args.length != 3){
				usage(programFileName, 3);
			}
			lowerKeyName = args[1];
			upperKeyName = args[2];
			System.out.println("Query2: Answering range queries over file name.");

			myDbs.setup(strEnvPath);
			outputText = searchFileNameRange(lowerKeyName, upperKeyName);
			break;

			//4- [10 points] Answering point queries over file size. 
			//For example, finding all files with size equal to 2000.
		case 4:
			if(args.length != 2){
				usage(programFileName, 4);
			}
			fileSize = Integer.parseInt(args[1]);

			System.out.println("Query3: Answering point queries over file size. ");

			myDbs.setup(strEnvPath);
			outputText = searchFileSize(fileSize);

			break;

			//5- [15 points] Answering range queries over file size. 
			//For example, finding all files whose sizes are from 2000 to 3000. 
		case 5:
			if(args.length != 3){
				usage(programFileName, 5);
			}
			lowerFileSize = Integer.parseInt(args[1]);
			upperFileSize = Integer.parseInt(args[2]);
			System.out.println("Query4: Answering range queries over file size. ");
			myDbs.setup(strEnvPath);
			outputText = searchFileSizeRange(lowerFileSize, upperFileSize);
			break;

			//6- [20 points] Answering range queries over both file name and file size. 
			//For example, finding all files whose names are from “20000.xml” to “30000.xml”
			//and their sizes range from 2000 to 3000. 
		case 6:
			if(args.length != 5){
				usage(programFileName, 6);
			}
			lowerKeyName = args[1];
			upperKeyName = args[2];
			lowerFileSize = Integer.parseInt(args[3]);
			upperFileSize = Integer.parseInt(args[4]);
			myDbs.setup(strEnvPath);
			System.out.println("Query5: Answering range queries over both file name and file size.");
			outputText = searchFileNameAndSizeRange(lowerKeyName, upperKeyName, lowerFileSize, upperFileSize);

			break;

			//7- [10 points] Finding all files whose contents contain an input string value. 
			//For example, finding all files that contain a specific string such as “Arnold”.
		case 7:
			if(args.length != 2){
				usage(programFileName, 7);
			}
			contentString = args[1];
			System.out.println("Query6: Finding all files whose contents contain an input string value. ");
			myDbs.setup(strEnvPath);
			outputText = searchFileContent(contentString);
			break;
		default:
			usage(programFileName, 0);
			break;
		}

		myDbs.close();
		out.write(outputText);
		out.close();
	}

	private static String insertEntries() throws Exception 
	{
		System.out.println("Inserting Entries...");

		TupleBinding imdbBinding = new ImdbDataTupleBinding();
		//ImdbDataTupleBinding binding = new ImdbDataTupleBinding();
		addImdbDataEntries(imdbBinding);

		return "Insertion Complete";

	}
	private static String searchFileName(String locateFile) throws Exception 
	{

		TupleBinding imdbBinding = new ImdbDataTupleBinding();
		System.out.println("Searching for file " + locateFile + "...");
		return showFile(imdbBinding, locateFile);
	}

	private static String searchFileContent(String locateContent) throws Exception 
	{

		TupleBinding imdbBinding = new ImdbDataTupleBinding();

		return showAllFiles(imdbBinding, locateContent);

	}

	private static String searchFileNameRange(String lowerKey, String upperKey) throws Exception{

		System.out.println("Searching for files between " + lowerKey + " and " + upperKey);
		TupleBinding imdbBinding = new ImdbDataTupleBinding();
		return showFileRange(imdbBinding, lowerKey, upperKey);
	}

	private static String searchFileSize(Integer locateFileSize) throws Exception
	{
		System.out.println("Searching for files with size " + locateFileSize + "...");
		TupleBinding imdbBinding = new ImdbDataTupleBinding();
		SecondaryCursor secCursor = null;
		StringBuilder returnText = new StringBuilder();

		try {
			// searchKey is the key that we want to find in the
			// secondary db.
			DatabaseEntry searchKey = new DatabaseEntry();

			IntegerBinding.intToEntry(locateFileSize, searchKey);

			// foundKey and foundData are populated from the primary
			// entry that is associated with the secondary db key.
			DatabaseEntry pfoundKey = new DatabaseEntry();
			DatabaseEntry foundData = new DatabaseEntry();


			secCursor =
				myDbs.getSizeIndexDB().openSecondaryCursor(null, null);


			// Search for the secondary database entry.
			OperationStatus retVal =
				secCursor.getSearchKey(searchKey, pfoundKey,
						foundData, LockMode.DEFAULT);


			while(retVal == OperationStatus.SUCCESS) {
				ImdbData imdbRecord = (ImdbData)imdbBinding.entryToObject(foundData);
				//Inventory theInventory =
				//    (Inventory)inventoryBinding.entryToObject(foundData);
				returnText.append(displayImdbRecord(pfoundKey, imdbRecord));
				retVal = secCursor.getNextDup(searchKey, pfoundKey,
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
		return returnText.toString();

	}

	private static String searchFileNameAndSizeRange(String lowerNameKey, String upperNameKey, int lowerSizeKey, int upperSizeKey) throws DatabaseException{
		//Cursor cursor = null;
		Cursor cursor = null;
		StringBuilder returnText = new StringBuilder();

		try {
			// searchKey is the key that we want to find in the
			// secondary db.
			DatabaseEntry searchKey =
				new DatabaseEntry(lowerNameKey.getBytes("UTF-8"));

			// foundKey and foundData are populated from the primary
			// entry that is associated with the secondary db key.
			DatabaseEntry foundKey = new DatabaseEntry();
			DatabaseEntry foundData = new DatabaseEntry();

			//DatabaseEntry secondaryKey = new DatabaseEntry();

			cursor = myDbs.getImdbDataDB().openCursor(null, null);

			// Search for the secondary database entry.
			OperationStatus retVal = cursor.getSearchKey(searchKey,
					foundData, LockMode.DEFAULT);


			while(retVal == OperationStatus.SUCCESS) {

				ImdbData imdbRecord = (ImdbData)binding.entryToObject(foundData);


				if(imdbRecord.getFileSize() >= lowerSizeKey && imdbRecord.getFileSize() <= upperSizeKey){
					returnText.append(displayImdbRecord(searchKey, imdbRecord));
				}

				if(imdbRecord.getFileName().equals(upperNameKey)){
					break;
				}

				retVal = cursor.getNext(searchKey, foundData, LockMode.DEFAULT);
			}
		} catch (Exception e) {
			System.err.println("Error on inventory secondary cursor:");
			System.err.println(e.toString());
			e.printStackTrace();
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}

		return returnText.toString();

	}

	private static String searchFileSizeRange(int lowerKey, int upperKey) throws DatabaseException
	{

		SecondaryCursor secCursor = null;
		StringBuilder returnText = new StringBuilder();
		try {
			// searchKey is the key that we want to find in the
			// secondary db.
			DatabaseEntry searchKey = new DatabaseEntry();

			IntegerBinding.intToEntry(lowerKey, searchKey);

			// foundKey and foundData are populated from the primary
			// entry that is associated with the secondary db key.
			DatabaseEntry pfoundKey = new DatabaseEntry();
			DatabaseEntry foundData = new DatabaseEntry();


			secCursor =
				myDbs.getSizeIndexDB().openSecondaryCursor(null, null);


			// Search for the secondary database entry.
			OperationStatus retVal =
				secCursor.getSearchKey(searchKey, pfoundKey,
						foundData, LockMode.DEFAULT);

			// Display the entry, if one is found. Repeat until no more
			// secondary duplicate entries are found
			while(retVal == OperationStatus.SUCCESS) {
				ImdbData imdbRecord = (ImdbData)binding.entryToObject(foundData);
				//Inventory theInventory =
				//    (Inventory)inventoryBinding.entryToObject(foundData);
				if(imdbRecord.getFileSize() > upperKey){
					break;	
				}

				returnText.append(displayImdbRecord(pfoundKey, imdbRecord));

				retVal = secCursor.getNext(searchKey, pfoundKey,
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

		return returnText.toString();
	}

	private static String showFile(TupleBinding binding, String locateFile) throws DatabaseException 
	{
		StringBuilder returnText = new StringBuilder();
		try {
			// searchKey is the key that we want to find in the
			// secondary db.
			DatabaseEntry searchKey =
				new DatabaseEntry(locateFile.getBytes("UTF-8"));

			DatabaseEntry foundData = new DatabaseEntry();


			if(myDbs.getImdbDataDB().get(null, searchKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS){
				ImdbData imdb = (ImdbData) binding.entryToObject(foundData);
				returnText.append(displayImdbRecord(searchKey, imdb));
			}
			else{
				throw new Exception();
			}
		} catch (Exception e) {
			System.err.println("Error on inventory secondary cursor:");
			System.err.println(e.toString());
			e.printStackTrace();
		} finally {
		}

		return returnText.toString();
	}
	private static String showFileRange(TupleBinding binding, String lowerKey, String upperKey) throws DatabaseException{

		//Cursor cursor = null;
		Cursor cursor = null;
		StringBuilder returnText = new StringBuilder();
		try {
			// searchKey is the key that we want to find in the
			// secondary db.
			DatabaseEntry searchKey =
				new DatabaseEntry(lowerKey.getBytes("UTF-8"));

			// foundKey and foundData are populated from the primary
			// entry that is associated with the secondary db key.
			DatabaseEntry foundKey = new DatabaseEntry();
			DatabaseEntry foundData = new DatabaseEntry();


			DatabaseEntry upperKey_ = new DatabaseEntry();
			upperKey_ = new DatabaseEntry(upperKey.getBytes("UTF-8"));

			cursor = myDbs.getImdbDataDB().openCursor(null, null);

			// Search for the secondary database entry.
			OperationStatus retVal = cursor.getSearchKey(searchKey,
					foundData, LockMode.DEFAULT);

			while(retVal == OperationStatus.SUCCESS) {

				ImdbData imdbRecord = (ImdbData)binding.entryToObject(foundData);

				returnText.append(displayImdbRecord(searchKey, imdbRecord));

				if(upperKey.compareTo(imdbRecord.getFileName()) < 0){
					break;
				}
				else{
					retVal = cursor.getNextNoDup(searchKey, foundData, LockMode.DEFAULT);
				}
			}
		} catch (Exception e) {
			System.err.println("Error on inventory secondary cursor:");
			System.err.println(e.toString());
			e.printStackTrace();
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
		return returnText.toString();
	}

	private static String displayImdbRecord(DatabaseEntry theKey,
			ImdbData imdbRecord) throws DatabaseException {

		System.out.println(imdbRecord.getFileName() + "(" + imdbRecord.getFileSize() + ")");
		return imdbRecord.getFileName() + "(" + imdbRecord.getFileSize() + ")\n";

	}

	private static String displayImdbFilter(DatabaseEntry theKey,
			ImdbData imdbRecord, String key) throws DatabaseException {

		if(imdbRecord.getContent().contains(key)) {
			System.out.println(imdbRecord.getFileName() + "(" + imdbRecord.getFileSize() + ")");
			return imdbRecord.getFileName() + "(" + imdbRecord.getFileSize() + ")\n";
		}
		return "";

	}

	private static String showAllFiles(TupleBinding binding, String key) throws DatabaseException {
		// Get a cursor

		Cursor cursor = myDbs.getImdbDataDB().openCursor(null, null);
		StringBuilder returnText = new StringBuilder();
		// DatabaseEntry objects used for reading records
		DatabaseEntry foundKey = new DatabaseEntry();
		DatabaseEntry foundData = new DatabaseEntry();

		try { // always want to make sure the cursor gets closed
			while (cursor.getNext(foundKey, foundData,
					LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				ImdbData imdbRecord =
					(ImdbData)binding.entryToObject(foundData);

				returnText.append(displayImdbFilter(foundKey, imdbRecord, key));
			}
		} catch (Exception e) {
			System.err.println("Error on inventory cursor:");
			System.err.println(e.toString());
			e.printStackTrace();
		} finally {
			cursor.close();
		}

		return returnText.toString();
	}

	private static void addImdbDataEntries(TupleBinding binding) throws Exception 
	{   
		try {

			File currentDir = new File("/scratch/cs440/imdb/"); //TODO: change to something else
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
		} finally {

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
			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return content;
	}
}
