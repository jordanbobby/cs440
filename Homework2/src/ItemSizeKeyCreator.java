/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2004, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$ 
 */

// File: ItemNameKeyCreator.java

//package db.GettingStarted;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.db.SecondaryKeyCreator;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.SecondaryDatabase;
import com.sleepycat.bind.tuple.*;

public class ItemSizeKeyCreator implements SecondaryKeyCreator {

    private TupleBinding theBinding;

    // Use the constructor to set the tuple binding
    ItemSizeKeyCreator(TupleBinding binding) {
        theBinding = binding;
    }

    // Abstract method that we must implement
    public boolean createSecondaryKey(SecondaryDatabase secDb,
             DatabaseEntry keyEntry,    // From the primary
             DatabaseEntry dataEntry,   // From the primary
             DatabaseEntry resultEntry) // set the key data on this.
         throws DatabaseException {

        if (dataEntry != null) {
            // Convert dataEntry to an Inventory object
            //Inventory inventoryItem =
            //      (Inventory)theBinding.entryToObject(dataEntry);
            
            ImdbData imdbItem =
                (ImdbData)theBinding.entryToObject(dataEntry);
            
            IntegerBinding.intToEntry(imdbItem.getFileSize(), resultEntry);
           
            
        }
        return true;
    }
}