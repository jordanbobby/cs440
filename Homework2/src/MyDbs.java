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
        SecondaryConfig mySecConfigSize = new SecondaryConfig();
        
        myDbConfig.setErrorStream(System.err);
        mySecConfigSize.setErrorStream(System.err);
        
        myDbConfig.setErrorPrefix("MyDbs");
        mySecConfigSize.setErrorPrefix("MyDbs");
        
        myDbConfig.setType(DatabaseType.BTREE);
        mySecConfigSize.setType(DatabaseType.BTREE);
        
        myDbConfig.setAllowCreate(true);
        mySecConfigSize.setAllowCreate(true);

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

        
        TupleBinding imdbBinding = new ImdbDataTupleBinding();
        // Open the secondary database. We use this to create a
        // secondary index for the inventory database

        // We want to maintain an index for the inventory entries based
        // on the item name. So, instantiate the appropriate key creator
        // and open a secondary database.
        
        ItemNameKeyCreator keyCreator =
            new ItemNameKeyCreator(new ImdbDataTupleBinding());

        ItemSizeKeyCreator keyCreatorSize = 
        	new ItemSizeKeyCreator(new ImdbDataTupleBinding());
        
        //ItemNameKeyCreator keyCreator =
        //    new ItemNameKeyCreator(new InventoryBinding());

        
        mySecConfigSize.setSortedDuplicates(true);
        mySecConfigSize.setAllowPopulate(true); // Allow autopopulate
        mySecConfigSize.setKeyCreator(keyCreatorSize);

        // Now open it
        try {
            itemsizeindexdb = databasesHome + "/" + itemsizeindexdb;
            //itemNameIndexDb = new SecondaryDatabase(itemnameindexdb,
            //                                        null,
            //                                        inventoryDb,
            //                                        mySecConfig);
            itemSizeIndexDb = new SecondaryDatabase(itemsizeindexdb,
                                                    null,
                                                    imdbDataDb,
                                                    mySecConfigSize);            
            
        } catch(FileNotFoundException fnfe) {
            System.err.println("MyDbs: " + fnfe.toString());
            System.exit(-1);
        }
    }

   // getter methods

    public Database getImdbDataDB() {
        return imdbDataDb;
    }

    public SecondaryDatabase getSizeIndexDB() {
        return itemSizeIndexDb;
    }

    // Close the databases
    public void close() {
        try {

            if (itemSizeIndexDb != null) {
                itemSizeIndexDb.close();
            }

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
