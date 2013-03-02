import java.io.*;

import java.util.*;
import java.util.regex.Pattern;

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
	//private static final String strEnvPath = "/scratch/cs440g1"; // this path was used for testing
	private static String strEnvPath = "./cs440_hw2_env";
	private static HashSet<String> StopWords;
	// DatabaseEntries used for loading records
	private static DatabaseEntry theKey = new DatabaseEntry();
	private static DatabaseEntry theData = new DatabaseEntry();
	private static ImdbDataTupleBinding binding = new ImdbDataTupleBinding();
	//The file to locate if for task # 1
	
	private static String ContactPerson = "GregorioLuisramirez";
	// Encapsulates the databases.
	private static MyDbs myDbs = new MyDbs();

	private static final String Delimiter =  " \t\n\r\f,.;:/\\\'\"`-_(){}[]?!<>|";
	// Usage Message
	private static void usage(String commandName, int task) {
		switch (task){
		case 0:
			System.out.println(commandName + " i|q arg1 arg2 arg3");
			break;
		case 1:
			System.out.println(commandName + " i env_full_path");
			break;
		case 2:
			System.out.println(commandName + " q env_full_path file_path");
			break;
		}

		System.exit(-1);
	}

	public static void main (String args[]) throws Exception{
		
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

		getStopWords();
		
		String strArg = args[0];
		
		if (strArg.equals("i")) {
			// Arguments count
			if(args.length != 2){
				System.out.println("wrong length: " + args.length);
				usage(programFileName, 1);
			}
			
			// Environment path
			strEnvPath = args[1];
			
			File theDir = new File(strEnvPath);
			if (!theDir.exists()) {
				System.out.println("Environment path does not exist");
				theDir.mkdir();
				//usage(programFileName, 1);
			}
			
			myDbs.setup(strEnvPath);
			
			insertEntries();
			outputText = "Files Inserted";

		} else if (strArg.equals("q")) {
			// Argument count
			if(args.length != 3){
				usage(programFileName, 2);
			}
			
			// Environment path
			
			strEnvPath = args[1]; 
			//argumentKey = args[1];
			System.out.println("Query");
			myDbs.setup(strEnvPath);
			
			// Input file path
			String inputPath = args[2];
			
			File theDir = new File(inputPath);
			if (!theDir.exists()) {
				System.out.println("Input file does not exist");
				usage(programFileName, 1);
			}	
			System.out.println("queryKeywords");
			queryKeywords(inputPath);
			

		} else {
			//System.out.println("case not found");
			//usage(programFileName, 0);
			
			strEnvPath = args[1]; 
			//argumentKey = args[1];
			System.out.println("Show all ");
			myDbs.setup(strEnvPath);
			showAll();
		}


		myDbs.close();

	}

	private static ArrayList<String> scanKeywords(String[] input) {
		
		ArrayList<String> adjustedKeywords = new ArrayList<String>();
		for (String keyword: input) {
			if (!StopWords.contains(keyword)) {
				adjustedKeywords.add(keyword.toLowerCase());
				
			}
		}
		adjustedKeywords.trimToSize();
		return adjustedKeywords;
	}
	private static void queryKeywords(String inputPath) throws IOException, DatabaseException {
		File input = new File(inputPath);
		Scanner scanner = new Scanner(input);
		StringBuilder outputText = new StringBuilder();
		
		while (scanner.hasNextLine()) {
			
			long startTime = System.nanoTime();
			
			String line = scanner.nextLine();
			ArrayList<String> keywords = scanKeywords(line.split(" "));
			Map<String, HashSet<String>> results = new HashMap<String, HashSet<String>>();
			
			for (String strKey: keywords) {
				results.put(strKey, getFilesByKeyword(strKey));
				System.out.println("out key = " + strKey);
			}
			
			System.out.println("size = " + results.size());
			boolean hasResults = true; 
			
			for (int i = 1; i < keywords.size(); i++) {
				results.get(keywords.get(i)).retainAll(results.get(keywords.get(i-1)));
				
				System.out.println("intersection of " + keywords.get(i) + " and " + keywords.get(i-1));
				for (String var : results.get(keywords.get(i))) {
					System.out.println(var);
				}
				
				if (results.get(keywords.get(i)).size() == 0) {
					System.out.println("hasResults = false");
					hasResults = false;
					break;
				}
			}
			
			if (hasResults) {
				System.out.println("has results");
				//for (String fileName : results.get(keywords[keywords.length-1])){
				for (String fileName : results.get(keywords.get(keywords.size()-1))){
					outputText.append(fileName + "; ");
					
				}
			}
			outputText.append("\n");
			
			long endTime = System.nanoTime();
			long duration = endTime - startTime;
			double seconds = (double) duration / (1.0E-9);
			System.out.println("Total time per line = \"" + line + " \"  Time = " + String.valueOf(seconds) + " seconds");
			
						
		}
		scanner.close();
		FileWriter fStream;
		BufferedWriter out = null;
		System.out.println(outputText.toString());
		try {
			fStream = new FileWriter(ContactPerson + "_Project3");
			out = new BufferedWriter(fStream);
			out.write(outputText.toString());
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			out.close();
		}
		
		
	}

	private static HashSet<String> getFilesByKeyword(String strKey) throws DatabaseException {
		HashSet<String> fileNames = new HashSet<String>();
		System.out.println("strKey = " + strKey);
		TupleBinding imdbBinding = new ImdbDataTupleBinding();
		Cursor cursor = null;
		try {
			DatabaseEntry searchKey;
			searchKey = new DatabaseEntry(strKey.getBytes("UTF-8"));
			
			DatabaseEntry pfoundKey = new DatabaseEntry();
			DatabaseEntry foundData = new DatabaseEntry();
			cursor = myDbs.getImdbDataDB().openCursor(null, null);
			
			OperationStatus retVal =
				cursor.getSearchKey(searchKey, foundData, LockMode.DEFAULT);

			while(retVal == OperationStatus.SUCCESS) {
				ImdbData imdbRecord = (ImdbData)imdbBinding.entryToObject(foundData);
				fileNames.add(imdbRecord.getFileName());
				//System.out.println(imdbRecord.getFileName());
				//returnText.append(displayImdbRecord(pfoundKey, imdbRecord));
				retVal = cursor.getNextDup(searchKey, foundData, LockMode.DEFAULT);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
		
		return fileNames;
	}

	private static String insertEntries() throws Exception 
	{
		
		System.out.println("Inserting Entries...");
		TupleBinding imdbBinding = new ImdbDataTupleBinding();
		
		long startTime = System.nanoTime();
		addImdbDataEntries(imdbBinding);
		long endTime = System.nanoTime();
		
		long duration = endTime - startTime;
		double seconds = (double) duration / (1.0E-9);
		System.out.println("Total time = " + String.valueOf(seconds) + " seconds");
		return "Insertion Complete";

	}

	/*
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
				//returnText.append(displayImdbRecord(pfoundKey, imdbRecord));
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

*/

	private static void addImdbDataEntries(TupleBinding binding) throws Exception 
	{   
		try {

			File currentDir = new File("/scratch/cs440/imdb/868");
			//File currentDir = new File("/scratch/cs440/imdb/000/");
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
			
					Set<String> keywordSet = tokenizeFile(file);
					System.out.println("scanning file: " + file.getName());
					insertInvertedIndex(keywordSet, file.getName());

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
	
	private static void insertInvertedIndex(Set<String> keywordSet, String name) throws DatabaseException, UnsupportedEncodingException {
		
		TupleBinding binding = new ImdbDataTupleBinding();
		ImdbData imdbRecord = new ImdbData();
		DatabaseEntry entry = new DatabaseEntry();
		for(String keyword : keywordSet){
			
			imdbRecord.setFileName(name);
			imdbRecord.setInvIndex(keyword);
			binding.objectToEntry(imdbRecord, theData);
			theKey = new DatabaseEntry(keyword.getBytes("UTF-8"));
			myDbs.getImdbDataDB().put(null, theKey, theData);
			//System.out.println(keyword + " : " + name);
		}
		
	}
	
	private static void getStopWords(){
		 
		File stopWordFile = new File("./stopwords.txt");
		StopWords = new HashSet<String>();
		try {
			Scanner scanner = new Scanner(stopWordFile);
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				StopWords.add(line.trim());
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	private static Set<String> tokenizeFile(File file) {
		Set<String> keywordSet = new HashSet<String>();
		try {
			Scanner scanner = new Scanner(file);
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				String lineNoXml = line.replaceAll("\\<.*?>", "").replaceAll("&.*?;","");
				String[] tokenizedLine = lineNoXml.split("["+Pattern.quote(Delimiter)+"]");
				for(String word : tokenizedLine){
					String lowerWord = word.toLowerCase().trim();
					if(!StopWords.contains(lowerWord) && lowerWord.length() > 0){
						keywordSet.add(lowerWord);
					}
				}
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		return keywordSet;
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
	
	private static void showAll() throws DatabaseException 
	{
		// Get a cursor
		TupleBinding imdbBinding = new ImdbDataTupleBinding();
		
		Cursor cursor = myDbs.getImdbDataDB().openCursor(null, null);
		
		// DatabaseEntry objects used for reading records
		DatabaseEntry foundKey = new DatabaseEntry();
		DatabaseEntry foundData = new DatabaseEntry();
		int count = 0;
		try { // always want to make sure the cursor gets closed
			while (cursor.getNext(foundKey, foundData,
					LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				ImdbData imdbRecord =
					(ImdbData)imdbBinding.entryToObject(foundData);
				System.out.println("index=" + imdbRecord.getInvIndex() + ", filename=" + imdbRecord.getFileName());
				count++;
			}
		} catch (Exception e) {
			System.err.println("Error on inventory cursor:");
			System.err.println(e.toString());
			e.printStackTrace();
		} finally {
			cursor.close();
			System.out.println("count = "+ count);
		}

		
	}
}
