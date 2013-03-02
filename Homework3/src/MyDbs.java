/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2004, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$ 
 */

// File: MyDbs.java

//package db.GettingStarted;

import java.io.FileNotFoundException;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.SecondaryConfig;
import com.sleepycat.db.SecondaryDatabase;


public class MyDbs {

    // The databases that our application uses

	
	private Database imdbDataDb = null;
    
    private Database classCatalogDb = null;
    private SecondaryDatabase itemNameIndexDb = null;
    

    private String imdbdatadb = "ImdbDataDB.db";
    private String classcatalogdb = "ClassCatalogDB.db";
    private String itemnameindexdb = "ItemNameIndexDB.db";
    
    private SecondaryDatabase itemSizeIndexDb = null;
    private String itemsizeindexdb = "ItemSizeIndexDB.db";
    


    // Our constructor does nothing
    public MyDbs() {}

    // The setup() method opens all our databases
    // for us.
    public void setup(String databasesHome)
        throws DatabaseException {

        DatabaseConfig myDbConfig = new DatabaseConfig();
        
        
        myDbConfig.setErrorStream(System.err);
        
        myDbConfig.setErrorPrefix("MyDbs");
        
        myDbConfig.setType(DatabaseType.HASH);
        
        myDbConfig.setAllowCreate(true);

        myDbConfig.setSortedDuplicates(true);

        
        // Now open, or create and open, our databases
        // Open the vendors and inventory databases
        try {
        	
        	imdbdatadb = databasesHome + "/" + imdbdatadb;
        	imdbDataDb = new Database(imdbdatadb,
                                    null,
                                    myDbConfig);
        	
        	
            
        } catch(FileNotFoundException fnfe) {
            System.err.println("MyDbs: " + fnfe.toString());
            System.exit(-1);
        }

        
        // Open the secondary database. We use this to create a
        // secondary index for the inventory database

        // We want to maintain an index for the inventory entries based
        // on the item name. So, instantiate the appropriate key creator
        // and open a secondary database.
        
        
    }

   // getter methods

    public Database getImdbDataDB() {
        return imdbDataDb;
    }



    // Close the databases
    public void close() {
        try {

            
            if (imdbDataDb != null) {
            	imdbDataDb.close();
            }

        } catch(DatabaseException dbe) {
            System.err.println("Error closing MyDbs: " +
                                dbe.toString());
            System.exit(-1);
        }
    }
}
