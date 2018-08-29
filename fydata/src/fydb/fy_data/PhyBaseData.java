/**
 * @(#)PhyBaseData.java	0.01 11/04/19
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_data;

import fydb.fy_comm.FyMetaData;
//import fydb.fy_main.BP;

import fydb.fy_comm.FyDataEntry;

//import fydb.fy_main.CommunicationClient;

import java.sql.Connection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

public interface PhyBaseData {
    public FyMetaData init(); //initialize

    public FyMetaData init(Connection dbconn, String dbTable, ArrayList keyColumns, String where); //for db initialize
    
    public FyMetaData init(String fileDir, String fileName, int workMode); // for file initialize

    public FyDataEntry next(BP bp); //get next entry from physical data source
    
    //private boolean implementLog(FyDataLogEntry.LogContent logData); // update data to physical data source

    public int implementLog(TreeMap logs, boolean ignoreFails);  // batch update data to physical data source
    
    public boolean insertData(HashMap key, HashMap value); // insert data to data source
    
    public int insertData(ArrayList datas, boolean ignoreFails); // batch insert data to data source (db), input a Array of FyDataEntry, return fails number

    public boolean deleteData(HashMap key); // delete data from data source
    
    public int deleteData(ArrayList datas, boolean ignoreFails); // batch delete data to data source (db), input a Array of key(HashMap), return fails number
    
    public long getCount(); // get data count
    
    public HashMap getDataProps(); // get data properties
    
    public void assignMetaData(FyMetaData metaData); // for push data from remote server
    
    public void assignDataProps(HashMap dataProps); // for push data from remote server

    public boolean release(); // release resource
}
