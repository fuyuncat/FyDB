/*
* @(#)FyDataSet.java   0.01 11/04/19
*
* Copyright 2011 Fuyuncat. All rights reserved.
* FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
* Email: fuyucat@gmail.com
* WebSite: www.HelloDBA.com
*/
package fydb.fy_data;

import fydb.fy_comm.Tracer;
import fydb.fy_comm.InitParas;
//import fydb.fy_comm.InitParas.*;
//import fydb.fy_comm.Consts;
import fydb.fy_comm.CommUtility;
import fydb.fy_comm.Consts;
import fydb.fy_comm.Debuger;
import fydb.fy_comm.FyDataEntry;
import fydb.fy_comm.FyMetaData;
import fydb.fy_comm.Prediction;
import fydb.fy_data.Optimizer.SearchMethod;

//import java.util.HashMap;
//import java.util.Map;
//import java.io.BufferedWriter;

//import fydb.fy_main.Beeper;
//import fydb.fy_main.BP;
//import fydb.fy_main.CommunicationClient;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
//import java.io.ObjectInputStream;
import java.io.InputStreamReader;
//import java.io.ObjectOutputStream;
//import java.io.OutputStreamWriter;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;

//import java.net.InetAddress;

import java.util.ArrayList;
import java.sql.*;

//import java.util.Collections;
//import java.util.Date;
import java.util.Comparator;
import java.util.HashMap;
//import java.util.Iterator;
import java.util.HashSet;
import java.util.Iterator;
//import java.util.Map;
//import java.util.Set;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class FyDataSet {
    protected InitParas paras;
    protected Tracer dtrace;
    protected Debuger debuger;
    protected Beeper beeper;

    //private Map metaCols = new HashMap();
    private int locker = 0;
    private boolean quiesced = false;
    private boolean loaded = false;
    private int accesserNum = 0;
    private int maxAccesserNum = 255; // max concurrent access number
    private boolean bufferLog = true; // buffer the change logs or not
    private boolean persistent = true; // implement persistent actions or not. true will append the log into disk.
    private boolean initialized = false; 
    //private boolean keepLogInMemory = false; // if keep log in memory. For some senarios, e.g. building index, implemented logs should be release after building index completed.
                                               // It seems this flag is unnecessary, because we've kept active BPs, which will not be implemented & released

    private byte[] lock = new byte[0]; // used for synchronize
    private byte[] logLock = new byte[0]; // used for synchronize

    private HashMap clients;
    private boolean isMaster = true;    // only master should implement logs to DB physical source, it's just meaningful for DB physical source
    private String masterAddr;

    //private ObjectOutputStream logWriter;
    private String baseLogName;
    private int curLogSeq = 1;
    private LogWriter logWriter;

    private ArrayList activeBPs;
    private FyLogData logs;
    private BP lastAppliedBp;

    private int phyType;
    private int memType;
    private FyMetaData metaData;

    private String schema;
    private String tableName;
    private PhyBaseData phyData;
    private MemBaseData memData;
    private HashMap indexes = new HashMap();        // entry of an index -- key(ArrayList):col1,col2..;value:indexData(MemBaseData)
    private int lastIndexId = 0;                   // unique id for an index
    private ArrayList keyColumns = new ArrayList();
    private String guid;

    // option for db dataset
    private Connection dbconn; 
    private int dbid;
    private String where;
    
    // option for file dataset
    private boolean storeLocal=false;   // if data should be store in localFile

    private HashMap fetchTokens;  // fetch Tokens. clientAddr:SynchronizeToken
    private TreeMap pendingLogs;  // store the logs recieved from net when local data set is quieced

    // Comparator of column data comparing
    public static class ColumnsComparator implements Comparator <ArrayList> {
        private ArrayList indColumns; // column_ids; position is physical position
        private FyMetaData metaData;

        public ColumnsComparator(ArrayList indColumns, FyMetaData metaData){
            this.indColumns = indColumns;
            this.metaData = metaData;
        }
        public int compare(ArrayList b1, ArrayList b2) {
            if (b1 == null && b2 == null)
                return 0;
            else if (b1 == null)
                return -1;
            else if (b2 == null)
                return 1;
            else if (b1.size() != indColumns.size())
                return -1;
            else if (b2.size() != indColumns.size())
                return 1;

            for (int i=0;i<indColumns.size();i++){
                Integer colID = (Integer)indColumns.get(i);
                int rslt = CommUtility.anyDataCompare((String)b1.get(i), (String)b2.get(i), metaData.getColumnType(colID));
                if (rslt == 0)
                    continue;
                else
                    return rslt;
            }
            return 0;
        }
        /*public int compare(HashMap b1, HashMap b2) {
            if (indColumns == null)
                return 0;
            for (int i=0;i<indColumns.size();i++){
                Integer colID = (Integer)indColumns.get(i);
                int rslt = CommUtility.anyDataCompare((String)b1.get(colID), (String)b2.get(colID), metaData.getColumnType(colID));
                if (rslt == 0)
                    continue;
                else
                    return rslt;
            }
            return 0;
        }//*/
    }
    
    private class SynchronizeToken{
        private BP bp;
        private ObjectInputStream in;    // data snapshot when start synchronize
        private ObjectOutputStream out;  
        private String tmpFileName;     // name of file to store temp data
        private HashMap buffData;         // temporary buffer data to be fetched
        private Iterator pointer;        // iterator for fetching

        public SynchronizeToken(BP bp){
            this.bp = bp;
        }

        public BP getBP(){
            return bp;
        }

        public boolean initialize(String clientAddr, String tableName){
            try{
                tmpFileName = System.getProperty("java.io.tmpdir")+File.separator+tableName+"_"+clientAddr+".tmp";
                File f = new File(tmpFileName);
                if (f.exists())
                    f.delete();
                f.createNewFile();
                out = new ObjectOutputStream(new FileOutputStream(tmpFileName, true));
            }catch(IOException e){
                return false;
            }
            return true;
        }

        public boolean storeData(HashMap datas){
            // store first data part in buffer
            if (buffData == null){
                buffData = new HashMap(datas);
                pointer = buffData.keySet().iterator();
            }
            try{
                out.writeObject(datas);
            }catch(IOException e){
                return false;
            }
            return true;
        }

        public boolean dataReady(){
            try{
                out.close();
                in = new ObjectInputStream(new FileInputStream(tmpFileName));
            }catch(IOException e){
                return false;
            }
            return true;
        }

        public HashMap fetchData(int batchSize){
            int fetchNum = 0;
            HashMap retData = new HashMap();
            if (buffData == null || buffData.size() == 0)
                return retData;
            // fetch data from buffer
            while (pointer.hasNext() && fetchNum<batchSize){
                HashMap key = (HashMap)pointer.next();
                HashMap value = (HashMap)buffData.get(key);
                retData.put(new HashMap(key), new HashMap(value));
                fetchNum++;
            }
            // if all buffer fetched, read next from disk to buffer
            if (!pointer.hasNext())
                try{
                    buffData = (HashMap)in.readObject();
                    pointer = buffData.keySet().iterator();
                }catch(ClassNotFoundException e){
                    return retData;
                }catch(IOException e){
                    return retData;
                }
            // if not reach batch size limitation, try to read next buffer
            if (fetchNum<batchSize)
                retData.putAll(fetchData(batchSize-fetchNum));

            return retData;
        }

        public void release(){
            try{
                in.close();
            }catch(IOException e){
            }
            buffData = null;
        }
    }

    // db dataset constructor
    public FyDataSet(int phyType, int memType, Connection dbconn, int dbid, String schema, String tableName, ArrayList keyColumns, HashMap indexes, String guid,
        InitParas paras, Tracer dtrace, Debuger debuger, Beeper beeper, HashMap clients, int maxAccesserNum, boolean bufferLog, boolean persistent) {
        if (phyType < 1 && phyType > 20){ // detect if physical data source is rdbms/file
            dtrace.trace(2);
            return;
        }

        this.phyType = phyType;
        this.memType = memType;
        this.dbconn = dbconn;
        this.dbid = dbid;
        this.tableName = tableName;
        this.schema = schema;
        this.keyColumns = keyColumns;
        this.indexes = indexes;
        if (indexes!=null){
           Iterator it = indexes.keySet().iterator();
           while (it.hasNext()){
               Integer indexID = (Integer)it.next();
               if (indexID.intValue() > this.lastIndexId)
                   this.lastIndexId = indexID.intValue();
           }
        }
        this.guid = guid;
        if (maxAccesserNum > 0)
            this.maxAccesserNum = maxAccesserNum;
        this.bufferLog = bufferLog;
        this.persistent = persistent;

        this.paras = paras;
        this.dtrace = dtrace;
        this.debuger = debuger;
        this.beeper = beeper;
        this.clients = clients;
        //beeper = new Beeper();
        lastAppliedBp = new BP();

        if (activeBPs == null)
            activeBPs = new ArrayList();
        if (logs == null)
            logs = new FyLogData(Beeper.getReverseComparator()); // build a reversed tree.

        //locker = 0;
        //quiesce = false;
    }

    // re-construct function to use default values
    public FyDataSet(int phyType, int memType, Connection dbconn, int dbid, String schema, String tableName, ArrayList keyColumns, HashMap indexes, String guid,
        InitParas paras, Tracer dtrace, Debuger debuger, Beeper beeper) {
        this(phyType, memType, dbconn, dbid, schema, tableName, keyColumns, indexes, guid, paras, dtrace, debuger, beeper, null, 255, true, true);
    }

    // re-construct function to use default values
    public FyDataSet(int phyType, int memType, Connection dbconn, int dbid, String schema, String tableName, ArrayList keyColumns, HashMap indexes, String guid,
        InitParas paras, Tracer dtrace, Debuger debuger, Beeper beeper, HashMap clients) {
        this(phyType, memType, dbconn, dbid, schema, tableName, keyColumns, indexes, guid, paras, dtrace, debuger, beeper, clients, 255, true, true);
    }
    
    private boolean awake(){
        if (!initialized){
            dtrace.trace(104);
            return false;
        }
        while (quiesced){
            try{
                Thread.sleep((Integer)paras.getParameter("_spinTime"));
            }
            catch (InterruptedException e) {
                dtrace.trace(e.getMessage());
                dtrace.trace(103);
                return false;
            }
        }
        return true;
    }
    
    //private long convertToLong(String strValue){
    //}
    
    // require access to this dataset
    public boolean requireAccess(){
        synchronized(lock){
            if (accesserNum >= maxAccesserNum-1){
                return false;
            }
            else{
                accesserNum++;
                return true;
            }
        }
    }

    public void releaseAccess(){
        synchronized(lock){
            accesserNum--;
            if (accesserNum < 0){
                dtrace.trace(11);
                accesserNum = 0;
            }
        }
        return;
    }

    // generate a new index id
    public long generateIndexId(){
       return lastIndexId++;
    }

    // load log control data from file
     /*  1~16(1): cur log seq
      * 17~32(2): lastAppliedBp.level0
      * 33~48(3): lastAppliedBp.level1
      * 49~64(4): lastAppliedBp.level2
      * 4 sections so far
      */
    private boolean loadLogControl(){
        String logControlFile = baseLogName+".sys";
        int bufLen = 16;
        char dataEntry[] = new char[bufLen];

        // set default values
        curLogSeq = 1;

        try{ // read metadata from file.
            int readSection = 0;
            int maxSection = 5;
            File dummmyfile = new File(logControlFile);
            if (!dummmyfile.exists()){
                dtrace.trace(logControlFile);
                dtrace.trace(21);
                return true;
            }
            BufferedReader dataReader = new BufferedReader(new InputStreamReader(new FileInputStream(dummmyfile.getCanonicalPath())));
            while (dataReader.read(dataEntry, 0 , bufLen) > -1 && readSection<maxSection){
                readSection++;
                switch (readSection){
                case 1: // version
                    curLogSeq = Integer.parseInt(String.valueOf(dataEntry).trim());
                    break;
                case 2: // lastBP.level0
                    lastAppliedBp.level0 = Long.parseLong(String.valueOf(dataEntry).trim());
                    break;
                case 3: // lastBP.level1
                    lastAppliedBp.level1 = Long.parseLong(String.valueOf(dataEntry).trim());
                    break;
                case 4: // lastBP.level2
                    lastAppliedBp.level2 = Long.parseLong(String.valueOf(dataEntry).trim());
                    break;
                }
            }
            dataReader.close();
        }catch (Exception e){
            dtrace.trace(22);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return true;
        }
        return true;
    }
    
    public boolean isMaster(boolean justCheckMyself){
        if (!justCheckMyself)
            checkMasterInNet();
        return isMaster;
    }

    public String getMasterAddr(){
        return masterAddr;
    }

    public void setMaster(boolean isMaster){
        this.isMaster= isMaster;
    }

    private void checkMasterInNet(){
        if (clients == null){
            isMaster= true;
        }else{
            Iterator it = clients.keySet().iterator();
            masterAddr = null;
            boolean foundMaster = false;
            while (it.hasNext()){
                String hostAddr = (String)it.next();
                CommunicationClient client = (CommunicationClient)clients.get(hostAddr);
                if (client.isMaster(guid)){
                    if (!foundMaster){
                        masterAddr = hostAddr;
                        foundMaster = true;
                    }else { // duplicated beeper
                        client.handoverMaster(guid);
                    }
                }
            }
            isMaster= masterAddr==null;
        }
    }

    // only meaningful for local file data source
    public void setStoreLocal(boolean storeLocal){
        this.storeLocal = storeLocal;
    }

    // only meaningful for db data source
    public void setWhere(String where){
        this.where = where;
    }
    // initialize a dataset
    private boolean init(){
        switch (phyType){
            case Consts.DB_ORACLE: //DB_Oracle:
            case Consts.DB_MYSQL: //DB_Mysql:
            case Consts.DB_MSSQL:
            case Consts.DB_DB2:
            case Consts.DB_SYBASE:
                baseLogName = new String((String)paras.getParameter("logDir")+File.separator+String.valueOf(dbid)+File.separator+schema+File.separator+tableName);
                checkMasterInNet();
                break;
            case Consts.HASHMAP: //HashMap:
            case Consts.SORTEDMAP: //SortedMap:
            case Consts.TREEMAP: //TreeMap:
                baseLogName = new String((String)paras.getParameter("logDir")+File.separator+String.valueOf(0)+File.separator+tableName);
                break;
            default:
                dtrace.trace(202);
                return false;
        }
        loadLogControl();
        logWriter = new LogWriter(paras, dtrace, debuger);
        if (!logWriter.openWriter(baseLogName+"_"+String.valueOf(curLogSeq)+".log"))
            return false;

        initialized = true;
        return true;
    }

    // update logs to memory data, including indexes
    private int updateLogsToMem(TreeMap curLogs){
        int updNum = 0;
        Iterator it = curLogs.keySet().iterator();
        while (it.hasNext()) {
            BP logBP = (BP)it.next();
            if (logBP == null)
                continue;
            FyBaseLogEntry log = (FyBaseLogEntry)curLogs.get(logBP);
            if (log.logType == Consts.INDEX){
                FyIndexLogEntry indLog = (FyIndexLogEntry)log;
                // update indexes
                updateRowInIndex(indLog.indId, indLog.datas, indLog.key, indLog.op); 
            }else if (log.logType == Consts.DATA){
                FyDataLogEntry dataLog = (FyDataLogEntry)log;
                switch (dataLog.logData.op) {
                case Consts.INSERT:
                    memData.set(dataLog.logData.key, dataLog.logData.value);
                    break;
                case Consts.MODIFY:
                    if (dataLog.logData.value != null)
                    {
                        HashMap oldVal = (HashMap)memData.get(dataLog.logData.key);
                        HashMap newVal = new HashMap();
                        Iterator chgItem = dataLog.logData.value.keySet().iterator();
                        while (chgItem.hasNext()){
                            Integer colID = (Integer)chgItem.next();
                            FyDataLogEntry.valueChangeEntry valChg = (FyDataLogEntry.valueChangeEntry)dataLog.logData.value.get(colID);
                            newVal.put(colID, valChg.newValue);
                        }
                        newVal = CommUtility.mergeValues(oldVal, newVal);
                        memData.set(dataLog.logData.key, dataLog.logData.value);
                    }
                    break;
                case Consts.DELETE:
                    memData.remove(dataLog.logData.key);
                    break;
                default:
                    continue;
                }
            }
            updNum++;
        }
        return updNum;
    }

    // write logs to file
    private boolean writeLogs(TreeMap curLogs){
        synchronized(logLock){
            String strLogs =new String();
            Iterator it = curLogs.keySet().iterator();
            while (it.hasNext()){
                BP logBP = (BP)it.next();
                FyBaseLogEntry baseLog = (FyBaseLogEntry)curLogs.get(logBP);
                if (baseLog.logType != Consts.DATA)
                    continue;
                FyDataLogEntry log = (FyDataLogEntry)baseLog;
   
                try {
                    String strLog = logBP.encodeString()+"|"+log.encodeString();
                    strLog += String.valueOf(strLog.length())+"|"+strLog;
                    strLogs += strLog;
                    if (strLogs.length() + strLog.length() > 8192 || !it.hasNext()) {
                        //debuger.printMsg("begin write inserting log",true);
                        logWriter.write(String.valueOf(strLog.length())+"|"+strLog);
                        if (!it.hasNext()) {
                            //debuger.printMsg("begin flush inserting log",true);
                            logWriter.flush();
                            //debuger.printMsg("end write inserting log",true);
                        }
                    }
                } catch (IOException e) {
                    dtrace.trace(211);
                    //dtrace.trace(curLogName);
                    return false;
                }
            }
        }
        return true;
    }

    // read the logs to be recovered into a treemap
    private int readLogs(BufferedReader logReader, TreeMap redoLogs){
        char buf[];
        int logNum = 0;
        try{ // read log from file.
            // read length
            String tmpStr = new String();
            while (logNum < 1024){
                buf = new char[1];
                tmpStr = "";
                if (logReader.read(buf,0,1) < 0) // get first char and check if reach tail
                    return logNum;
                tmpStr += String.valueOf(buf);
                while(logReader.read(buf,0,1)>-1 && buf[0] != '|'){
                    tmpStr += String.valueOf(buf);
                }
                int len = Integer.parseInt(tmpStr.trim());

                buf = new char[len];
                if (logReader.read(buf,0,len) < 0)
                    return logNum;
                int off = 0;
                BP logBp = new BP();
                int bpLen = logBp.decodeString(buf,off); // decode buffer stream to BP
                if (bpLen < 0){
                    dtrace.trace(26);
                    return -1;
                }
                off += bpLen;
                if (beeper.compare(logBp, lastAppliedBp) <= 0) // skip those applied logs
                    continue;
                FyDataLogEntry log = new FyDataLogEntry();
                if (log.decodeString(buf, off)<0){ // decode buffer stream to a log entry
                    dtrace.trace(26);
                    return -1;
                }
                redoLogs.put(logBp,log); // attach decoded log into tree
                logNum++;
            }
            return logNum;
        }catch (Exception e){
            dtrace.trace(26);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return -1;
        }
    }

    // return Meta data
    public FyMetaData getMetaData(){
        return metaData;
    }

    // return data properties
    public HashMap getDataProps(){
        HashMap dataProps = phyData.getDataProps();
        return dataProps;
    }

    // recover data from logs
    private boolean recoverData(){
        String curLogFile = baseLogName+"_"+String.valueOf(curLogSeq)+".log";
        try{ // read log from file.
            int readSection = 0;
            int maxSection = 5;
            File dummmyfile = new File(curLogFile);
            if (!dummmyfile.exists()){
                dtrace.trace(curLogFile);
                dtrace.trace(25);
                return true;
            }
            BufferedReader logReader = new BufferedReader(new InputStreamReader(new FileInputStream(dummmyfile.getCanonicalPath())));
            int bufLen = 256;
            char dataEntry[] = new char[bufLen];
            logReader.read(dataEntry, 0 , bufLen);
            TreeMap redoLogs = new TreeMap(Beeper.getComparator());
            while (readLogs(logReader, redoLogs) > 0){
                phyData.implementLog(redoLogs,true);
                redoLogs.clear();
            }
            logReader.close();
        }catch (Exception e){
            dtrace.trace(24);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return true;
        }
        return true;
    }

    // maintain indexes. modify a row. it will also generate releated logs. a modify will generate an delete & insert
    private boolean modifyRowInIndexes(BP bp, HashMap key, HashMap chgDatas, TreeMap curLogs, boolean updateMem){
        if (curLogs == null){
            dtrace.trace(250);
            return false;
        }
        if (chgDatas == null)
            return true;
        Iterator it = indexes.values().iterator();
        while (it.hasNext()){
            FyIndex index = (FyIndex)it.next();
            if (index == null || index.getState() != Consts.VALID || index.getIndData() == null) // just update valid index
               continue;
            ArrayList indColumns = index.getIndColumns();
            MemBaseData indData = index.getIndData();
            ArrayList oldDatas = new ArrayList();
            ArrayList newDatas = new ArrayList();
            if (!getChangedColumnsData(chgDatas, indColumns, oldDatas, newDatas))
                continue;
            // an index data entry: coldata1,coldata2..(ArrayList):key1,key2...(ArrayList) 
            // complete a delete
            ArrayList oldKeys = (ArrayList)indData.get(oldDatas);
            if (oldKeys == null){ 
                if (updateMem)
                    indData.remove(oldDatas);
            }else{
                oldKeys.remove(key);
                if (updateMem)
                    if (oldKeys.size() == 0)
                        indData.remove(oldDatas);
                    else
                        indData.set(oldDatas, oldKeys);
            }
            // complete an insert
            ArrayList newKeys = (ArrayList)indData.get(newDatas);
            if (newKeys == null)
                newKeys = new ArrayList();
            newKeys.add(key);
            if (updateMem)
                indData.set(newDatas, newKeys);
            if (bp != null){ // generate delete& insert index maintain log
               FyIndexLogEntry indLogD = new FyIndexLogEntry(Consts.DELETE,oldDatas,key);
               curLogs.put(new BP(bp), indLogD);
               bp.increaseSeq();
               FyIndexLogEntry indLogI = new FyIndexLogEntry(Consts.INSERT,newDatas,key);
               curLogs.put(new BP(bp), indLogI);
               bp.increaseSeq();
            }
        }
        return true;
    }
    
    // maintain indexes. detele a row. it will also generate releated logs
    private boolean deleteRowFromIndexes(BP bp, FyDataEntry row, TreeMap curLogs, boolean updateMem){
        //if (curLogs == null){
        //    dtrace.trace(250);
        //    return false;
        //}
        // update all indexes
        HashMap wholeRow = new HashMap(row.key);
        wholeRow.putAll(row.value); // whole row should contain key as well as value
        Iterator it = indexes.values().iterator();
        while (it.hasNext()){
            FyIndex index = (FyIndex)it.next();
            if (index == null || index.getState() != Consts.VALID) // just update valid index
               continue;
            ArrayList indColumns = index.getIndColumns();
            MemBaseData indData = index.getIndData();
            if (indData == null){
                dtrace.trace(indColumns.toString());
                dtrace.trace(117);
                index.setState(Consts.INVALID);
                continue;
            }
            // content of any index are same.
            ArrayList colData = getColumnsDataByID(wholeRow, indColumns); // key data of sortedmap/treemap index is set, whose sequence is sensitive
            // an index data entry: coldata1,coldata2..(ArrayList):key1,key2...(ArrayList) 
            ArrayList keys = (ArrayList)indData.get(colData);
            if (keys == null){ 
                if (updateMem)
                    indData.remove(colData);
            }else{
                keys.remove(row.key);
                if (updateMem)
                    if (keys.size() == 0)
                        indData.remove(colData);
                    else
                        indData.set(colData, keys);
            }
            if (bp != null && curLogs != null){ // generate index maintain log
               FyIndexLogEntry indLog = new FyIndexLogEntry(Consts.DELETE,colData,row.key);
               curLogs.put(new BP(bp), indLog);
               bp.increaseSeq();
            }
        }
        return true;
    }

    // maintain an indexes. insert or delete a row. it will update the memory index directly
    private boolean updateRowInIndex(int indexId, ArrayList indexedData, HashMap key, int action){
        FyIndex index = (FyIndex)indexes.get(Integer.valueOf(indexId));
        if (index == null || index.getState() != Consts.VALID)
            return false;
        MemBaseData indData = index.getIndData();
        if (indData == null){
            dtrace.trace(117);
            index.setState(Consts.INVALID);
            return false;
        }
        ArrayList existingKeys = (ArrayList)indData.get(indexedData);
        switch (action){
        case Consts.INSERT:
            if (existingKeys == null)
                existingKeys = new ArrayList();
            existingKeys.add(key);
            break;
        case Consts.DELETE:
            existingKeys.remove(key);
            if (existingKeys.size() == 0)
                indData.remove(indexedData);
            break;
        }
        if (existingKeys.size() != 0)
            indData.set(indexedData,existingKeys);

        return true;
    }
    
    // maintain indexes. insert a row. it will also generate releated logs
    private boolean insertRowToIndexes(BP bp, FyDataEntry row, TreeMap curLogs, boolean updateMem){
        //if (curLogs == null){
        //    dtrace.trace(250);
        //    return false;
        //}
        // update all indexes
        HashMap wholeRow = new HashMap(row.key);
        wholeRow.putAll(row.value); // whole row should contain key as well as value
        Iterator it = indexes.values().iterator();
        while (it.hasNext()){
            FyIndex index = (FyIndex)it.next();
            if (index == null || index.getState() != Consts.VALID) // just update valid index
               continue;
            ArrayList indColumns = index.getIndColumns();
            MemBaseData indData = index.getIndData();
            if (indData == null || !index.hasInitialized()){
                dtrace.trace(indColumns.toString());
                dtrace.trace(117);
                index.setState(Consts.INVALID);
                continue;
            }
            // content of any index are same.
            ArrayList colData = getColumnsDataByID(wholeRow, indColumns); // key data of sortedmap/treemap index is ArrayList, whose sequence is sensitive
            // an index data entry: coldata1,coldata2..(ArrayList):key1,key2...(ArrayList) 
            ArrayList keys = (ArrayList)indData.get(colData);
            if (keys == null)
                keys = new ArrayList();
            keys.add(row.key);
            if (updateMem)
                indData.set(colData, keys);
            if (bp != null && curLogs != null){ // generate index maintain log
               FyIndexLogEntry indLog = new FyIndexLogEntry(Consts.INSERT,colData,row.key);
               curLogs.put(new BP(bp), indLog);
               bp.increaseSeq();
            }
        }
        return true;
    }
    
    // prepare to push data that got from remote server, assign the meata datas
    public boolean preparePush(FyMetaData metaData, HashMap dataProps){
        if (!initialized && !init())
            return false;

        if (!awake()) // wait quice awake by other process
            return false;

        quiesced = true;
        loaded = false;

        try{
            // wait all sessions release dataset
            while (locker > 0){
                try{
                    Thread.sleep((Integer)paras.getParameter("_spinTime"));
                }
                catch (InterruptedException e) {
                    dtrace.trace(e.getMessage());
                    dtrace.trace(101);
                    quiesced = false;
                    return false;
                }
            }

            // release data
            if (this.metaData != null)
                this.metaData.releaseData();
            if (this.memData != null)
                this.memData.releaseAll();

            this.metaData = new FyMetaData(metaData);

            switch (phyType){
            case Consts.DB_ORACLE: //DB_Oracle:
            case Consts.DB_MYSQL: //DB_Mysql:
            case Consts.DB_MSSQL:
            case Consts.DB_DB2:
            case Consts.DB_SYBASE:
                where = (String)dataProps.get("where");
                phyData = new PhyDBData(dtrace, debuger);
                FyMetaData localMeta = phyData.init(dbconn,tableName,keyColumns,where);
                if (!localMeta.getName().equals(metaData.getName()) ||
                    !localMeta.getColumns().equals(metaData.getColumns()) ||
                    !localMeta.getKeyColumns().equals(metaData.getKeyColumns())){
                    dtrace.trace(128);
                    return false;
                }
                phyData.assignMetaData(metaData);
                phyData.assignDataProps(dataProps);
                break;
            case Consts.HASHMAP: //HashMap:
            case Consts.SORTEDMAP: //SortedMap:
            case Consts.TREEMAP: //TreeMap:
                phyData = new PhyHashKVData(dtrace, debuger, paras);
                phyData.assignMetaData(metaData);
                dataProps.put("fileName", tableName);
                dataProps.put("fileDir", (String)paras.getParameter("dataDir")+File.separator+tableName);
                phyData.assignDataProps(dataProps);
                if (!((PhyHashKVData)phyData).buildNewFiles())
                    return false;
                break;
            default:
                dtrace.trace(202);
                quiesced = false;
                return false;
            }

            // initialzed the pre-defined local indexes, assign column IDs
            Iterator it = indexes.keySet().iterator();
            while (it.hasNext()){
                Integer indID = (Integer)it.next();
                FyIndex index = (FyIndex)indexes.get(indID);
                if (index != null){
                    index.initialize(metaData);
                }
            }

            lastAppliedBp = beeper.getCurBP();

            switch (memType){
            case Consts.HASHMAP: //HashMap:
                memData = new MemHashKVData(dtrace);
                break;
            case Consts.SORTEDMAP: //SortedMap:
            case Consts.TREEMAP: //TreeMap:
                memData = new MemTreeKVData(new ColumnsComparator(metaData.getKeyColumns(), metaData), dtrace);
                break;
            default:
                dtrace.trace(203);
                quiesced = false;
                return false;
            }
        }catch(Exception e){
            dtrace.trace(10);
            if (debuger.isDebugMode())
                e.printStackTrace();
            quiesced = false;
            return false;
        }


        return true;
    }
    
    public void pushData(Map datas){
        memData.putAll(datas);
        Iterator it = datas.keySet().iterator();
        while (it.hasNext()){
            HashMap key = (HashMap)it.next();
            HashMap value = (HashMap)datas.get(key);
            insertRowToIndexes(null, new FyDataEntry(key,value), null, true);
        }
        metaData.setEntryCount(memData.getSize());
    }

    // complete push. and apply the pending logs generated from other server during pushing
    public void completePush(){
        quiesced = false;
        // write data to file
        if (phyType >= 11 && phyType <= 20 && storeLocal){
            ((PhyHashKVData)phyData).fullCopyToFiles(memData);
        }
        synchronizeLogsLocal(pendingLogs);
        loaded = true;
    }

    // prepare to fetch all memory data 
    public boolean beginFetch(String clientAddr){
        if (!verifyState())
            return false;
        locker++;
        BP bp  = beeper.getBP();
        activeBPs.add(bp);
        
        if (fetchTokens == null){
            fetchTokens = new HashMap();
        }
        SynchronizeToken fetchToken = new SynchronizeToken(bp);
        if (!fetchToken.initialize(clientAddr, tableName))
            return false;
        Prediction fakeFilter = new Prediction(); // build a fake filter to full scan data
        HashSet allColumns = new HashSet(metaData.getColumns().keySet());
        ArrayList snapShot = searchDataFullC(bp,fakeFilter,fakeFilter,allColumns);
        int bufferSize = 1000;
        HashMap bufferData = new HashMap();
        // store data into to temp file
        for (int i=0;i<snapShot.size();i++){
            HashMap allColVal = (HashMap)snapShot.get(i);
            HashMap key = new HashMap();
            HashMap value = new HashMap();
            Iterator it = allColVal.keySet().iterator();
            while (it.hasNext()){
                Integer colID = (Integer)it.next();
                String colVal = (String)allColVal.get(colID);
                if (metaData.isKeyColumn(colID))
                    key.put(colID,colVal);
                else
                    value.put(colID,colVal);
            }
            bufferData.put(key,value);
            if (bufferData.size() >= bufferSize){
                fetchToken.storeData(bufferData);
                bufferData = new HashMap();
            }
        }
        if (bufferData.size() >= 0)
            fetchToken.storeData(bufferData);
        fetchToken.dataReady();
        fetchTokens.put(clientAddr, fetchToken);

        return true;
    }

    // batch Fetch data from temp file
    public HashMap batchFetch(String clientAddr, int batchSize){
        HashMap fetchData = new HashMap();
        if (clientAddr == null || fetchTokens == null)
            return fetchData;
        SynchronizeToken fetchToken = (SynchronizeToken)fetchTokens.get(clientAddr);
        if (fetchToken == null)
            return fetchData;
        fetchData = fetchToken.fetchData(batchSize);
        // reached end, release token
        if(fetchData.size() == 0){
            BP bp = fetchToken.getBP();
            locker--;
            bp.resetSeq();
            activeBPs.remove(bp);
            fetchToken.release();
            fetchTokens.remove(clientAddr);
        }

        return fetchData;
    }

    // load data from physical source to memory dataset 
    public boolean loadData(BP lastBp) {
        if (!initialized && !init())
            return false;

        if (!awake()) // wait quice awake by other process
            return false;
        // quiesce the dataset before loading data
        //if (quiesce){
        //    dtrace.trace(102);
        //    return false;
        //}
        quiesced = true;
        loaded = false;
        try{
            // wait all sessions release dataset
            while (locker > 0){
                try{
                    Thread.sleep((Integer)paras.getParameter("_spinTime"));
                }
                catch (InterruptedException e) {
                    dtrace.trace(e.getMessage());
                    dtrace.trace(101);
                    quiesced = false;
                    return false;
                }
            }

            // release data
            if (metaData != null)
                metaData.releaseData();
            if (memData != null)
                memData.releaseAll();
            //if (memData != null)
            //    try{
            //        memData.finalize();
            //    }catch (Throwable e){
            //        e.printStackTrace();
            //        dtrace.trace(6);
            //    }
    
            // load data
            switch (phyType){
            case Consts.DB_ORACLE: //DB_Oracle:
            case Consts.DB_MYSQL: //DB_Mysql:
            case Consts.DB_MSSQL:
            case Consts.DB_DB2:
            case Consts.DB_SYBASE:
                phyData = new PhyDBData(dtrace, debuger);
                metaData = phyData.init(dbconn, tableName, keyColumns, where);
                break;
            case Consts.HASHMAP: //HashMap:
            case Consts.SORTEDMAP: //SortedMap:
            case Consts.TREEMAP: //TreeMap:
                phyData = new PhyHashKVData(dtrace, debuger, paras);
                metaData = phyData.init((String)paras.getParameter("dataDir")+File.separator+tableName, tableName, Consts.DISK);
                break;
            default:
                dtrace.trace(202);
                quiesced = false;
                return false;
            }
            
            // initialzed the pre-defined local indexes, assign column IDs
            Iterator it = indexes.keySet().iterator();
            while (it.hasNext()){
                Integer indID = (Integer)it.next();
                FyIndex index = (FyIndex)indexes.get(indID);
                if (index != null){
                    index.initialize(metaData);
                }
            }

            recoverData(); // recover logs
            lastAppliedBp = lastBp; // set the lastappliedbp as a new last bp in this instance after recovered.

            switch (memType){
            case Consts.HASHMAP: //HashMap:
                memData = new MemHashKVData(dtrace);
                break;
            case Consts.SORTEDMAP: //SortedMap:
            case Consts.TREEMAP: //TreeMap:
                memData = new MemTreeKVData(new ColumnsComparator(metaData.getKeyColumns(), metaData), dtrace);
                break;
            default:
                dtrace.trace(203);
                quiesced = false;
                return false;
            }

            BP bp = beeper.getBP();
            //activeBPs.add(bp);
            FyDataEntry row = phyData.next(bp);
            while(row != null){
                if (memData.containsKey(row.key)) { // detect duplicated key
                    dtrace.trace(210);
                    memData.releaseAll();
                    metaData.releaseData();
                    quiesced = false;
                    return false;
                }
                memData.add(row);
                insertRowToIndexes(null, row, null, true);
                row = phyData.next(bp);
            }
            metaData.setEntryCount(memData.getSize());
        }catch(Exception e){
            dtrace.trace(10);
            if (debuger.isDebugMode())
                e.printStackTrace();
            quiesced = false;
            return false;
        }

        quiesced = false;
        loaded = true;
        //debuger.printMsg(metaData.getColumns().toString(), false);
        return true;
    }

    private synchronized boolean verifyState(){
        if (!loaded || memData == null || metaData == null){
            dtrace.trace(208);
            return false;
        }
        if (!awake())
            return false;
        return true;
    }

    // get old/new data of specified columns from changing data, in exactly same sequence. NULL is inclusive
    // input columns Name
    private boolean getChangedColumnsData(HashMap chgDatas, ArrayList columns, ArrayList oldDatas, ArrayList newDatas){
        if (columns == null || chgDatas == null || oldDatas == null || newDatas == null)
            return false;
        ArrayList colIDs = metaData.getColIDByName(columns);
        for (int i=0;i<columns.size();i++){
            Integer colID = (Integer)colIDs.get(i);
            FyDataLogEntry.valueChangeEntry valChange = (FyDataLogEntry.valueChangeEntry)chgDatas.get(colID);
            if (valChange == null)
                continue;
            oldDatas.add(valChange.oldValue);
            newDatas.add(valChange.newValue);
        }
        if (oldDatas.size() == 0 || newDatas.size() == 0)
            return false;
        else
            return true;
    }

    // get data of specified columns from whole row, in exactly same sequence. NULL is inclusive
    // input columns Name
    private ArrayList getColumnsData(HashMap wholeRow, ArrayList columns){
        if (columns == null)
            return null;
        ArrayList data = new ArrayList();
        if (wholeRow == null){
            for (int i=0;i<columns.size();i++)
                data.add(null);
        }else{
            ArrayList colIDs = metaData.getColIDByName(columns);
            for (int i=0;i<columns.size();i++){
                data.add(wholeRow.get(colIDs.get(i)));
            }
        }
        return data;
    }

    // get data of specified columns from whole row, in exactly same sequence. NULL is inclusive
    // input columns Name. NULL will be contained
    private ArrayList getColumnsDataByID(HashMap wholeRow, ArrayList columns){
        if (columns == null)
            return null;
        ArrayList data = new ArrayList();
        if (wholeRow == null){
            for (int i=0;i<columns.size();i++)
                data.add(null);
        }else{
            for (int i=0;i<columns.size();i++){
                data.add(wholeRow.get(columns.get(i)));
            }
        }
        return data;
    }

    // build an index
    public boolean buildIndex(ArrayList indColumns, int indexType){
        if (!verifyState())
            return false;
        dtrace.trace(116);
        return false;
    }

    //  build a new key, if columns not contain a key column with different data, it will return a key set equal to the old one.
    private HashMap generateNewKey(HashMap oldKey, ArrayList<Integer> colIDs, ArrayList newValues){
        HashMap newKey = new HashMap(oldKey);
        if (colIDs == null || newValues == null || colIDs.size() != newValues.size()){
            dtrace.trace(213);
            return newKey;
        }
        for (int i=0; i<colIDs.size(); i++){
            Integer colID = colIDs.get(i);
            //HashMap colProperties = (HashMap)metaData.getColumns().get(colID);
            // ???????consistent get the Key!!!
            //if (colProperties != null && ((Integer)colProperties.get("K")).intValue() >= 0 && !((String)oldKey.get(colID)).equals((String)newValues.get(i))) { // the column set contains a key column
            if (metaData.isKeyColumn(colID)) { // the column set contains a key column
                newKey.remove(colID);
                newKey.put(colID, (String)newValues.get(i));
            }
        }
        return newKey;
    }

    //  build a new key, if columns not contain a key column with different data, it will return a key set equal to the old one.
    private HashSet generateNewKeys(HashSet oldKeys, ArrayList<Integer> colIDs, ArrayList newValues){
        if (!verifyState())
            return null;
        HashSet newKeys = new HashSet();
        Iterator it = oldKeys.iterator();
        while (it.hasNext()){
            HashMap oldKey = (HashMap)it.next();
            HashMap newKey = generateNewKey(oldKey, colIDs, newValues);
            newKeys.add(newKey);
        }
        return newKeys;
    }

    // identify key from a set of column, then generate a key
    private HashMap identifyKeyFromColumns(ArrayList<Integer> colIDs, ArrayList<String> values){
        if (colIDs == null){
            dtrace.trace(236);
            return null;
        }
        if (values == null){
            dtrace.trace(217);
            return null;
        }
        if (values.size() != values.size()){
            dtrace.trace(213);
            return null;
        }
        HashMap key = new HashMap();
        Iterator it = metaData.getKeyColumns().iterator();
        while (it.hasNext()){
            Integer colID = (Integer)it.next();
            int index = colIDs.indexOf(colID);
            if (index >= 0)
                key.put(colID, values.get(index));
        }
        return key;
    }

    // merge 2 data entry, the items in 2nd one will replace items in the 1st one; and new ones in 2nd one will insert into 1st one
    private FyDataEntry mergeData(BP bp, HashMap key, ArrayList colIDs, ArrayList newValues){
        if (key == null)
            return null;
        if (colIDs == null){
            dtrace.trace(214);
            return null;
        }
        if (newValues == null){
            dtrace.trace(215);
            return null;
        }
        if (colIDs.size() != newValues.size()){
            dtrace.trace(213);
            return null;
        }
        
        HashMap newKey = new HashMap(key);
        HashMap newValue = new HashMap(getMemDataByKeyC(bp,key));
        for (int i=0;i<colIDs.size();i++){
            Integer colID = (Integer)colIDs.get(i);
            if (newKey.containsKey(colID))
                newKey.put(colID, newValues.get(i));
            else if (newValue.containsKey(colID))
                newValue.put(colID, newValues.get(i));
        }
        
        return new FyDataEntry(newKey, newValue);
    }

    // generate a data entry with inputted columns&values
    private FyDataEntry generateDataEntry(BP bp, ArrayList<Integer> colIDs, ArrayList<String> values){
        if (colIDs == null){
            dtrace.trace(236);
            return null;
        }
        if (values == null){
            dtrace.trace(217);
            return null;
        }
        if (colIDs.size() != values.size()){
            dtrace.trace(213);
            return null;
        }
        HashMap key = new HashMap();
        HashMap value = new HashMap();
        HashMap columns = metaData.getColumns(); // full column list, to be used for nullable test
        for (int i=0; i<colIDs.size(); i++){
            Integer colID = colIDs.get(i);
            String strVal = values.get(i);
            columns.remove(colID);
            if (strVal == null)
                if (metaData.isNullable(colID) != 1){
                    dtrace.trace(metaData.getColumnName(colID));
                    dtrace.trace(248);
                    return null;
                }else
                    continue;
            if (metaData.isKeyColumn(colID))
                key.put(colID,strVal);
            else
                value.put(colID,strVal);
        }
        Iterator it = columns.keySet().iterator();
        while (it.hasNext()){
            Integer colID = (Integer)it.next();
            if (metaData.isNullable(colID) != 1){
                dtrace.trace(metaData.getColumnName(colID));
                dtrace.trace(248);
                return null;
            }
        }
        FyDataEntry data = new FyDataEntry(key, value);
        return data;
    }

    //private void terminalBP(BP bp){
    //    activeBPs.remove(bp);
    //    locker--;
    //}
    
    // consistent get. called in internal. Implement mvcc
    private HashMap getMemDataByKeyC(BP bp, HashMap key){
        HashMap val;
        val = (HashMap)memData.get(key);
        SortedMap consistentLogs = logs.headMap(bp);
        //Iterator it = consistentLogs.values().iterator(); 
        Iterator it = consistentLogs.keySet().iterator(); 
        while(it.hasNext()) {
            BP logBp = (BP)it.next();
            // ignore the logs with same BP except seq.
            if (beeper.compareBP(logBp, bp,false) == 0)
                continue;
            FyBaseLogEntry baseLog = (FyBaseLogEntry)consistentLogs.get(logBp);
            // also ignore INDEX logs
            if (baseLog.logType != Consts.DATA) 
                continue;
            FyDataLogEntry log = (FyDataLogEntry)baseLog; // convert base log to data log
            if (key.equals(log.logData.key)){
                if (log.logData.op == Consts.INSERT) // consistent with insert operations. means this key must not exist when consistent get begin
                    val = null;
                else if (log.logData.op == Consts.MODIFY){ // consistent with modify operations. rollback the changes
                    if (log.logData.value == null) {
                        dtrace.trace(12);
                        continue; // log value is null, we should ignore this log
                    }
                    // apply the old values for consistents
                    Iterator itValue = log.logData.value.keySet().iterator(); 
                    while(itValue.hasNext()) {
                        Integer colID = (Integer)itValue.next();
                        FyDataLogEntry.valueChangeEntry colChgs = (FyDataLogEntry.valueChangeEntry)log.logData.value.get(colID);
                        if (colChgs != null){
                            if (val == null) 
                                val = new HashMap();
                            val.remove(colID);
                            val.put(colID, colChgs.oldValue);
                        }
                    }
                }
                else if (log.logData.op == Consts.DELETE) // consistent with delete operations. means this key must exist when consistent get begin
                {
                    val = log.logData.value;
                }
            }
        }
        return val;
    }

    public HashMap getMemDataByKey(HashMap key){
        if (!verifyState())
            return null;
        locker++;
        HashMap val;
        BP bp = beeper.getBP();
        activeBPs.add(bp);
        try{
            val = getMemDataByKeyC(bp, key);
            locker--;
            activeBPs.remove(bp);
        }catch(Exception e){
            locker--;
            activeBPs.remove(bp);
            dtrace.trace(10);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return null;
        }
        return val;
    }
    
    // get a set of data from memory, identifying by a set of keys
    public ArrayList getMemDatasByKeys(BP bp, ArrayList keys){
        if (!verifyState())
            return null;
        locker++;
        activeBPs.add(bp);
        ArrayList datas = new ArrayList();
        try{
            for (int i=0;i<keys.size();i++){
                datas.add(getMemDataByKeyC(bp, (HashMap)keys.get(i)));
            }
        }catch(Exception e){
            locker--;
            activeBPs.remove(bp);
            dtrace.trace(10);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return null;
        }
        return datas;
    }
    
    // get all columns&data in prediction and decide if all operators are = and all junctions are "and"
    // get all potentioal accessable prediction columns, who fulfill all of below criterias
    // 1: who stand on the top level;
    // 2: OR junction will change the level;
    // 3: whose operators are = ;
    // 4: no dupluicated preduction in the same level.
    // inAccessColumns store those who can not be accessable prediction column (duplicated in top level)
    // example (col1 = A and col2 =B ) or col3 = C => allColDatas: (col1;col2;col3), equalColData: () -- because top junction is OR
    // example (col1 = A and col2 =B ) and col1 = C => allColDatas: (col1;col2), equalColData: (col2:B) -- col2 should match 2 datas, is not absolute EQUAL
    // example (col1 = A or col2 =B ) and col1 = C => allColDatas: (col1;col2), equalColData: (col1:C) -- (col1 = A or col2 =B ) should be filters
    private boolean predictColumns(Prediction filter, HashSet allColumns, HashMap accessColData, HashSet inAccessColumns, int level){
        if (inAccessColumns == null)
            inAccessColumns = new HashSet();
        boolean allMatch = true; //Only if all predicting columns are hash index columns, vice versa, and all operators are = and all junctions are "and", the hash index has the highest priority
        if (filter.type == Consts.BRANCH){
            if (filter.leftNode == null || filter.rightNode == null){
                dtrace.trace(119);
                return false;
            }else{
                if (filter.junction != Consts.AND) // OR will change the level
                    level++;
                allMatch &= (predictColumns(filter.leftNode, allColumns, accessColData, inAccessColumns, level) && 
                             predictColumns(filter.rightNode, allColumns, accessColData, inAccessColumns, level));
                allMatch &= (filter.junction == Consts.AND);  // junction is and
            }
        }else if (filter.type == Consts.LEAF){
            Integer colID = new Integer(filter.leftColId);
            if (level == 0) {// if is top level and EQUAL comparator
                if (filter.comparator == Consts.EQ){
                    String data = (String)accessColData.get(colID);
                    // delete if is an inaccesable column already
                    // if not, then detect if duplicated. if duplicated, detect if equal to same value, NULL value should be considered
                    // ???only detect top level columns???
                    if (!inAccessColumns.contains(colID))
                        if (!accessColData.containsKey(colID) || data == filter.rightExpression || data.equals(filter.rightExpression))
                            accessColData.put(colID, filter.rightExpression);
                        else{
                            accessColData.remove(colID);
                            inAccessColumns.add(colID);
                        }
                }else{
                    accessColData.remove(colID);
                    inAccessColumns.add(colID);
                }
            }else
                inAccessColumns.add(colID);
            allColumns.add(colID);
            allMatch &= (filter.comparator == Consts.EQ); // operator is ==
        }else{
            dtrace.trace(121);
            return false;
        }
        return allMatch;
    }

    // get all columns in prediction and decide if all operators are = and all junctions are "and"
    private boolean predictionColumns(Prediction filter, ArrayList columns, ArrayList datas){
        boolean allMatch = true; //Only if all predicting columns are hash index columns, vice versa, and all operators are = and all junctions are "and", the hash index has the highest priority
        if (filter.type == Consts.BRANCH){
            if (filter.leftNode == null || filter.rightNode == null){
                dtrace.trace(119);
                return false;
            }else{
                allMatch &= (predictionColumns(filter.leftNode, columns, datas) && predictionColumns(filter.rightNode, columns, datas));
                allMatch &= (filter.junction == Consts.AND);  // junction is and
            }
        }else if (filter.type == Consts.LEAF){
            columns.add(new Integer(filter.leftColId));
            datas.add(filter.rightExpression);
            allMatch &= (filter.comparator == Consts.EQ); // operator is ==
        }else{
            dtrace.trace(121);
            return false;
        }
        return allMatch;
    }
    
    // detect if data of key&vale match all filters
    private boolean matchFilter(HashMap allValue, Prediction prediction){
        boolean matched = false; 
        if (prediction.type == Consts.BRANCH){
            if (prediction.leftNode == null || prediction.rightNode == null){
               dtrace.trace(119);
               return false;
            }
            if (prediction.junction == Consts.AND)  // and
               matched = matchFilter(allValue, prediction.leftNode) && matchFilter(allValue, prediction.rightNode);
            else // or
               matched = matchFilter(allValue, prediction.leftNode) || matchFilter(allValue, prediction.rightNode);
        }else if (prediction.type == Consts.LEAF){
            if (prediction.leftColId < 0) // filter is expression
                return prediction.leftExpression==null?prediction.rightExpression==null:prediction.leftExpression.equals(prediction.rightExpression);
            if (allValue == null)
                return false;
            String data = (String)allValue.get(Integer.valueOf(prediction.leftColId));
            if (data == null)
                return false;
            return CommUtility.anyDataCompare(data, prediction.comparator, prediction.rightExpression, metaData.getColumnType(Integer.valueOf(prediction.leftColId))) == 1;
        }else{ // no predication means alway true
            return true;
        }
        return matched;
    }
    
    // search data from dataset directly, filtered by conditions
    // consistent get required
    private ArrayList searchDataFullC(BP bp, Prediction expressionAccesser, Prediction dataFilter, HashSet selectColumns){
        //debuger.printMsg(Debuger.getMethodName(), true);
        ArrayList datas = new ArrayList();
        if (expressionAccesser.calculateExpression()){
            Iterator it = memData.keySet().iterator();
            while (it.hasNext()){
                HashMap key = (HashMap)it.next();
                HashMap value = getMemDataByKeyC(bp, key);
                if (value == null)
                    continue;
                HashMap allValue = new HashMap(value);
                allValue.putAll(key); // involve key to be tested matching
                if (matchFilter(allValue, dataFilter))
                    //datas.add(new FyDataEntry(bp, key, value));
                    datas.add(selectColumns==null?allValue:CommUtility.subMap(allValue,selectColumns)); // bp is meanningless for a result
            }

            // retrieve all deleted keys & datas
            // for consistent get, we should get all logs would be used for rollback
            // identify all deleted keys
            SortedMap consistentLogs =  mergeDataLogs(bp,beeper.getCurBP(),new FyLogData(logs.headMap(bp)));
            if (consistentLogs != null){
                it = consistentLogs.keySet().iterator(); 
                while(it.hasNext()) {
                    BP logBp = (BP)it.next();
                    // ignore the logs with same BP except seq.
                    if (beeper.compareBP(logBp, bp,false) == 0)
                        continue;
                    FyBaseLogEntry baseLog = (FyBaseLogEntry)consistentLogs.get(logBp);
                    // also ignore INDEX logs
                    if (baseLog.logType != Consts.DATA) 
                        continue;
                    FyDataLogEntry log = (FyDataLogEntry)baseLog; // convert base log to data log
                    if (log.logData.op == Consts.DELETE){
                        HashMap allValue = new HashMap(log.logData.value);
                        allValue.putAll(log.logData.key);
                        //datas.add(new FyDataEntry(bp, log.logData.key, log.logData.value));
                        datas.add(selectColumns==null?allValue:CommUtility.subMap(allValue,selectColumns)); // bp is meanningless for a result
                    }
                }
            }
       }
       return datas;
    }
    
    // consistent get. called in internal. Implement mvcc
    private ArrayList getKeyFromIndexC(BP bp, int indId, MemBaseData indData, ArrayList indexedData){
        ArrayList keys = null;
        keys = (ArrayList)indData.get(indexedData);
        /*switch (indData.getMemType()){
            case Consts.HASHMAP: //HashMap
                keys = (ArrayList)indData.get(indexedData);
                break;
            case Consts.TREEMAP: //TreeMap:
            case Consts.SORTEDMAP: //SortedMap:
                keys = (ArrayList)indData.get(indexedData);
                break;
        }//*/
        if (keys == null)
            keys = new ArrayList();
        SortedMap consistentLogs = logs.headMap(bp);
        //Iterator it = consistentLogs.values().iterator(); 
        Iterator it = consistentLogs.keySet().iterator(); 
        while(it.hasNext()) {
            BP logBp = (BP)it.next();
            // ignore the logs with same BP except seq.
            if (beeper.compareBP(logBp, bp,false) == 0)
                continue;
            FyBaseLogEntry baseLog = (FyBaseLogEntry)consistentLogs.get(logBp);
            // also ignore non-INDEX logs
            if (baseLog.logType != Consts.INDEX) 
                continue;
            FyIndexLogEntry log = (FyIndexLogEntry)baseLog; // convert base log to data log
            if (log.indId != indId)
                continue;
            if (indexedData.equals(log.datas)){
                if (log.op == Consts.INSERT) // consistent with insert operations. means this key must not exist when consistent get begin
                    keys.remove(log.key);
                else if (log.op == Consts.DELETE){ // consistent with delete operations. means this key must exist when consistent get begin
                    keys.add(log.key);
                }
            }
        }
        return keys;
    }

    // get data by index. call get getKeyFromIndexC get key list first, then filter keys, get data by keys, and filter data
    private ArrayList getDataByIndex(BP bp, int indId, Prediction expressionAccesser, Prediction indexAccesser, Prediction indexFilter, Prediction dataFilter, HashSet selectColumns){
        //debuger.printMsg(Debuger.getMethodName(), true);
        ArrayList datas = new ArrayList();
        FyIndex index = (FyIndex)indexes.get(Integer.valueOf(indId));
        if (index == null || index.getState() != Consts.VALID || index.getIndData() == null)
            return datas;
        if (expressionAccesser.calculateExpression()){
            HashMap accessMapData = indexAccesser.buildMap();
            ArrayList accessColData = new ArrayList();
            // build indexed data
            for (int i=0;i<index.getIndColumns().size();i++){
                Integer colID = (Integer)index.getIndColumns().get(i);
                if (colID == null)
                    continue;
                // each indexed columns should be assinged value, null could be contained.
                String accessData = (String)accessMapData.get(colID);
                accessColData.add(accessData);
            }

            HashSet keyCols = new HashSet(metaData.getKeyColumns());
            HashSet indCols = new HashSet(metaData.getColumns().keySet());
            HashSet avaCols = new HashSet(keyCols);
            avaCols.addAll(indCols);
            boolean searchIndexOnly = avaCols.containsAll(selectColumns)&&(dataFilter == null || dataFilter.type == Consts.UNKNOWN);

            ArrayList keys = getKeyFromIndexC(bp, indId, (MemBaseData)index.getIndData(), accessColData);

            if (searchIndexOnly){
                HashMap indColData = new HashMap();
                // add selected column from indexed columns
                for (int i=0;i<index.getIndColumns().size();i++){
                    Integer colID = (Integer)index.getIndColumns().get(i);
                    if (selectColumns.contains(colID))
                        indColData.put(colID, accessColData.get(i));
                }
                // add selected column from key columns
                for (int i=0;i<keys.size();i++){
                    HashMap retData = new HashMap(indColData);
                    retData.putAll((HashMap)keys.get(i));
                    datas.add(selectColumns==null?retData:CommUtility.subMap(retData,selectColumns));
                }
            }else{
                if (keys.size() == 0)
                    return datas;
                for (int i=keys.size()-1;i>=0;i--){ // reversed sequence, remove will not affect sequence id
                    HashMap key = (HashMap)keys.get(i);
                    // filter keys
                    if (matchFilter(key, indexFilter)){
                        HashMap value = getMemDataByKeyC(bp, key);
                        if (value == null)
                            continue;
                        HashMap allValue = new HashMap(value);
                        allValue.putAll(key); // involve key to be tested matching
                        // filter data
                        if (matchFilter(allValue, dataFilter)){
                            //datas.add(new FyDataEntry(bp, key, value));
                            datas.add(selectColumns==null?allValue:CommUtility.subMap(allValue,selectColumns)); // bp is meanningless for a result
                        }else
                            continue;
                    }
                }
            }
        }
        return datas;
    }

    // get data by key. call get method
    private ArrayList getDataByKey(BP bp, Prediction expressionAccesser, Prediction indexAccesser, Prediction indexFilter, Prediction dataFilter, HashSet selectColumns){
        //debuger.printMsg(Debuger.getMethodName(), true);
        ArrayList datas = new ArrayList();
        if (expressionAccesser.calculateExpression()){
            HashMap key = indexAccesser.buildMap();
            HashMap value = getMemDataByKeyC(bp, key);
            if (value == null)
                return datas;
            if (matchFilter(key, indexFilter)){
                HashMap allValue = new HashMap(value);
                allValue.putAll(key); // involve key to be tested matching
                // filter data
                if (matchFilter(allValue, dataFilter)){
                    //datas.add(new FyDataEntry(bp, key, value));
                    datas.add(selectColumns==null?allValue:CommUtility.subMap(allValue,selectColumns)); // bp is meanningless for a result
                }else
                    return datas;
            }else
                return datas;
        }
        return datas;
    }

    // retrieve all deleted & inserted keys & datas
    // for consistent get, we should get all logs would be used for rollback
    // identify all deleted & inserted keys
    private void consistentMatchDatas(BP bp, Prediction prediction, ArrayList datas, HashSet selectColumns){
        SortedMap consistentLogs =  mergeDataLogs(bp,beeper.getCurBP(),new FyLogData(logs.headMap(bp)));
        if (consistentLogs != null){
            Iterator it = consistentLogs.keySet().iterator(); 
            while(it.hasNext()) {
                BP logBp = (BP)it.next();
                // ignore the logs with same BP except seq.
                if (beeper.compareBP(logBp, bp,false) == 0)
                    continue;
                FyBaseLogEntry baseLog = (FyBaseLogEntry)consistentLogs.get(logBp);
                // also ignore INDEX logs
                if (baseLog.logType != Consts.DATA) 
                    continue;
                FyDataLogEntry log = (FyDataLogEntry)baseLog; // convert base log to data log
                HashMap data = new HashMap(log.logData.key);
                data.putAll(log.logData.value);
                if (log.logData.op == Consts.DELETE){
                    HashMap allValue = new HashMap(log.logData.value);
                    allValue.putAll(log.logData.key); // involve key to be tested matching
                    // filter data
                    if (matchFilter(allValue, prediction))
                        //datas.add(new FyDataEntry(bp, log.logData.key, log.logData.value));
                        //datas.add(new FyDataEntry(null, log.logData.key, log.logData.value));
                        datas.add(selectColumns==null?data:CommUtility.subMap(data,selectColumns)); 
                }else if (log.logData.op == Consts.INSERT){
                    //datas.remove(new FyDataEntry(bp, log.logData.key, log.logData.value));
                    //datas.remove(new FyDataEntry(null, log.logData.key, log.logData.value));
                    datas.remove(selectColumns==null?data:CommUtility.subMap(data,selectColumns)); 
                }
            }
        }
    }

    // search data by full-scan key. consistent get required
    private ArrayList searchDataFullScanKeyC(BP bp, Prediction expressionAccesser, Prediction indexFilter, Prediction dataFilter, HashSet selectColumns){
        //debuger.printMsg(Debuger.getMethodName(), true);
        ArrayList datas = new ArrayList();
        if (expressionAccesser.calculateExpression()){
            Iterator it = memData.keySet().iterator();
            while (it.hasNext()){
                HashMap key = (HashMap)it.next();
                HashMap value = getMemDataByKeyC(bp, key);
                if (value == null)
                    continue;
                if (matchFilter(key, indexFilter)){
                    HashMap allValue = new HashMap(value);
                    allValue.putAll(key); // involve key to be tested matching
                    // filter data
                    if (matchFilter(allValue, dataFilter)){
                        //datas.add(new FyDataEntry(bp, key, value));
                        datas.add(selectColumns==null?allValue:CommUtility.subMap(allValue,selectColumns)); // bp is meanningless for a result
                    }else
                        continue;
                }else
                    continue;
            }

            // retrieve all deleted & inserted keys & datas
            // for consistent get, we should get all logs would be used for rollback
            // identify all deleted & inserted keys
            consistentMatchDatas(bp, dataFilter, datas,selectColumns);
        }
        return datas;
    }
    
    // retrieve all deleted & inserted keys & datas
    // for consistent get, we should get all logs would be used for rollback
    // identify all deleted & inserted keys
    private void consistentMatchIndexes(BP bp, int indId, Prediction prediction, ArrayList keys, SortedMap consistentLogs){
        ArrayList indColumns = ((FyIndex)indexes.get(Integer.valueOf(indId))).getIndColumns();
        if (consistentLogs != null){
            Iterator it = consistentLogs.keySet().iterator(); 
            while(it.hasNext()) {
                BP logBp = (BP)it.next();
                // ignore the logs with same BP except seq.
                if (beeper.compareBP(logBp, bp,false) == 0)
                    continue;
                FyBaseLogEntry baseLog = (FyBaseLogEntry)consistentLogs.get(logBp);
                // also ignore INDEX logs
                if (baseLog.logType != Consts.INDEX) 
                    continue;
                FyIndexLogEntry log = (FyIndexLogEntry)baseLog; // convert base log to data log
                if (log.op == Consts.DELETE){
                    HashMap indexedDataMap = CommUtility.arrayDataToMap(indColumns, log.datas);
                    // filter data
                    if (matchFilter(indexedDataMap, prediction))
                        keys.add(new HashMap(log.key));
                }else if (log.op == Consts.INSERT){
                    keys.remove(log.key);
                }
            }
        }
    }

    // full scan index, get all matched keys. consistent get required
    // if selectColumns is null, just return keys, if it's not null, will return the list of selected columns
    private ArrayList searchKeysFullScanIndexC(BP bp, int indId, MemBaseData indData, ArrayList indColumns, Prediction indexFilter, HashSet selectColumns){
       ArrayList retDatas = new ArrayList();
       if (indData == null)
            return retDatas;

        SortedMap consistentLogs =  mergeIndexLogs(bp,beeper.getCurBP(),indId,new FyLogData(logs.headMap(bp)));
        Iterator it = indData.keySet().iterator();
        while (it.hasNext()){
            ArrayList indexedData = (ArrayList)it.next();
            if (indexedData == null)
                continue;
            HashMap indexedDataMap = CommUtility.arrayDataToMap(indColumns, indexedData);
            if (matchFilter(indexedDataMap, indexFilter)){
                ArrayList moreKeys = (ArrayList)indData.get(indexedData);
                // retrieve all deleted & inserted keys & datas
                // for consistent get, we should get all logs would be used for rollback
                // identify all deleted & inserted keys
                consistentMatchIndexes(bp, indId, indexFilter, retDatas, consistentLogs);

                if (moreKeys != null){
                    if (selectColumns == null)
                        retDatas.addAll(new ArrayList(moreKeys));
                    else{
                        HashMap indColData = new HashMap();
                        // add selected column from indexed columns
                        for (int i=0;i<indColumns.size();i++){
                            Integer colID = (Integer)indColumns.get(i);
                            if (selectColumns.contains(colID))
                                indColData.put(colID, indexedData.get(i));
                        }
                        // add selected column from key columns
                        for (int i=0;i<moreKeys.size();i++){
                            HashMap retData = new HashMap(indColData);
                            retData.putAll((HashMap)moreKeys.get(i));
                            retDatas.add(retData);
                        }
                    }
                }
            }else
                continue;
        }

        return retDatas;
    }

    // search data by full-scan index. consistent get required
    private ArrayList searchDataFullScanIndexC(BP bp, int indId, Prediction expressionAccesser, Prediction indexFilter, Prediction dataFilter, HashSet selectColumns){
        //debuger.printMsg(Debuger.getMethodName(), true);
        ArrayList datas = new ArrayList();
        FyIndex index = (FyIndex)indexes.get(Integer.valueOf(indId));
        if (index == null || index.getState() != Consts.VALID || index.getIndData() == null)
            return datas;
        if (expressionAccesser.calculateExpression()){
            HashSet keyCols = new HashSet(metaData.getKeyColumns());
            HashSet indCols = new HashSet(index.getIndColumns());
            HashSet avaCols = new HashSet(keyCols);
            avaCols.addAll(indCols);
            boolean searchIndexOnly = avaCols.containsAll(selectColumns)&&(dataFilter == null || dataFilter.type == Consts.UNKNOWN);

            ArrayList keys = searchKeysFullScanIndexC(bp, indId, index.getIndData(), index.getIndColumns(), indexFilter, searchIndexOnly?selectColumns:null);

            Iterator it = keys.iterator();
            while (it.hasNext()){
                HashMap key = metaData.identifyKey((HashMap)it.next());
                HashMap value = getMemDataByKeyC(bp, key);
                if (value == null)
                    continue;
                HashMap allValue = new HashMap(value);
                allValue.putAll(key); // involve key to be tested matching
                // filter data
                if (matchFilter(allValue, dataFilter))
                    //datas.add(new FyDataEntry(bp, key, value));
                    datas.add(selectColumns==null?allValue:CommUtility.subMap(allValue,selectColumns)); // bp is meanningless for a result
                else
                    continue;
            }

            // retrieve all deleted & inserted keys & datas
            // for consistent get, we should get all logs would be used for rollback
            // identify all deleted & inserted keys
            consistentMatchDatas(bp, dataFilter, datas, selectColumns);
        }

        return datas;
    }
    
    // build an indexed data from an accesser, complete the absent column with NULL
    private ArrayList buildIndexedDataFromAccesser(ArrayList indColumns, Prediction indexAccesser){
        ArrayList indexedData =  new ArrayList();
        if (indColumns == null)
            return indexedData;
        // build a NULL list first
        for (int i=0;i<indColumns.size();i++)
            indexedData.add(null);

        indexAccesser.fillDataForColumns(indexedData, indColumns);
        return indexedData;
    }

    // part(range) scan index, get all matched keys. consistent get required
    // just for SortedMap/TreeMap index
    // if selectColumns is null, just return keys, if it's not null, will return the list of selected columns
    private ArrayList searchKeysPartScanIndexC(BP bp, int indId, MemBaseData indData, ArrayList indColumns, Prediction indexAccesser, Prediction indexFilter, HashSet selectColumns){
        ArrayList retDatas = new ArrayList();
        if (indData == null)
            return retDatas;
        if (indData.getMemType() != Consts.SORTEDMAP && indData.getMemType() != Consts.TREEMAP){
            dtrace.trace(122);
            return retDatas;
        }

        ArrayList accessData = buildIndexedDataFromAccesser(indColumns, indexAccesser);
        
        // get the first non-EQ compartor, decide we should get tail or head of B-Tree index
        boolean getTail = true;
        for (int i=0;i<indColumns.size();i++){
            Integer colID = (Integer)indColumns.get(i);
            Prediction node = indexAccesser.getFirstPredByColId(colID.intValue(),true);
            if (node == null || node.type != Consts.LEAF)
                break;
            if (node.comparator != Consts.EQ){
                if (node.comparator == Consts.ST){
                    getTail = false;
                // for SE, we should set the endpoint as successor of accessData.
                }else if (node.comparator == Consts.SE){
                    getTail = false;
                    int dataId = indColumns.indexOf(colID);
                    String accessVale = (String)accessData.get(dataId);
                    accessVale = CommUtility.successor(accessVale, metaData.getColumnType(colID));
                    accessData.set(dataId,accessVale);
                }
                break;
            }
        }

        SortedMap consistentLogs =  mergeIndexLogs(bp,beeper.getCurBP(),indId,new FyLogData(logs.headMap(bp)));
        //MemTreeKVData accessTree = new MemTreeKVData(new FyDataSet.ColumnsComparator(indColumns, metaData), dtrace);
        // get start of accesser matched set
        Iterator it = getTail?((MemTreeKVData)indData).tailMap(accessData).keySet().iterator():((MemTreeKVData)indData).headMap(accessData).keySet().iterator();
        while (it.hasNext()){
            ArrayList indexedData = (ArrayList)it.next();
            if (indexedData == null)
                continue;
            HashMap indexedDataMap = CommUtility.arrayDataToMap(indColumns, indexedData);
            // check the end of accesser matched set
            if (matchFilter(indexedDataMap, indexAccesser)){
                ArrayList moreKeys = (ArrayList)indData.get(indexedData);;
                // retrieve all deleted & inserted keys & datas
                // for consistent get, we should get all logs would be used for rollback
                // identify all deleted & inserted keys
                consistentMatchIndexes(bp, indId, indexFilter, moreKeys, consistentLogs);

                if (moreKeys != null){
                    // filter indexedData
                    if (matchFilter(indexedDataMap, indexFilter)){
                        if (selectColumns == null)
                            retDatas.addAll(new ArrayList(moreKeys));
                        else{
                            HashMap indColData = new HashMap();
                            // add selected column from indexed columns
                            for (int i=0;i<indColumns.size();i++){
                                Integer colID = (Integer)indColumns.get(i);
                                if (selectColumns.contains(colID))
                                    indColData.put(colID, indexedData.get(i));
                            }
                            // add selected column from key columns
                            for (int i=0;i<moreKeys.size();i++){
                                HashMap retData = new HashMap(indColData);
                                retData.putAll((HashMap)moreKeys.get(i));
                                retDatas.add(retData);
                            }
                        }
                    }
                }
            }
            //else
            //    break;
        }

        return retDatas;
    }

    // search data by full-scan index. consistent get required
    private ArrayList searchDataPartScanIndexC(BP bp, int indId, Prediction expressionAccesser, Prediction indexAccesser, Prediction indexFilter, Prediction dataFilter, HashSet selectColumns){
        //debuger.printMsg(Debuger.getMethodName(), true);
        ArrayList datas = new ArrayList();
        FyIndex index = (FyIndex)indexes.get(Integer.valueOf(indId));
        if (index == null || index.getState() != Consts.VALID || index.getIndData() == null)
            return datas;
        if (index.getIndType() != Consts.SORTEDMAP && index.getIndType() != Consts.TREEMAP){
            dtrace.trace(122);
            return datas;
        }
        
        if (expressionAccesser.calculateExpression()){
            HashSet keyCols = new HashSet(metaData.getKeyColumns());
            HashSet indCols = new HashSet(index.getIndColumns());
            HashSet avaCols = new HashSet(keyCols);
            avaCols.addAll(indCols);
            boolean searchIndexOnly = avaCols.containsAll(selectColumns)&&(dataFilter == null || dataFilter.type == Consts.UNKNOWN);

            ArrayList keys = searchKeysPartScanIndexC(bp, indId, index.getIndData(), index.getIndColumns(), indexAccesser, indexFilter, searchIndexOnly?selectColumns:null);
            if (searchIndexOnly)
                datas.addAll(keys);
            else{
                //get data by key and filter data
                Iterator it = keys.iterator();
                while (it.hasNext()){
                    HashMap key = metaData.identifyKey((HashMap)it.next());
                    HashMap value = getMemDataByKeyC(bp, key);
    
                    if (value == null)
                        continue;
                    HashMap allValue = new HashMap(value);
                    allValue.putAll(key); // involve key to be tested matching
                    // filter data
                    if (matchFilter(allValue, dataFilter)){
                        //datas.add(new FyDataEntry(bp, key, value));
                        //datas.add(new FyDataEntry(null, key, value)); // bp is meanningless for a result
                        datas.add(selectColumns==null?allValue:CommUtility.subMap(allValue,selectColumns)); 
                    }else
                        continue;
                }
            }

            // retrieve all deleted & inserted keys & datas
            // for consistent get, we should get all logs would be used for rollback
            // identify all deleted & inserted keys
            consistentMatchDatas(bp, dataFilter, datas, selectColumns);
        }

        return datas;
    }

    // search data, filter by conditions
    // Only if the index contains all predicting columns, the index could be hitted;
    // Onlu if all predicting columns are key columns, vice versa, and all operators are = and all junctions are "and", the key has the highest priority
    // Only if all predicting columns are hash index columns, vice versa, and all operators are = and all junctions are "and", the hash index has the highest priority following key
    // Index with few columns number has higher priority then the one with more columns, key has higher priorit than other indexes
    // if no index could be hitted, access memData directly
    // 1: key(all columns matched with ==&"and")
    // 2: hash index(all columns matched with ==&"and")
    // 3: key(part columns matched)
    // 4: index(part columns matched, fewer columns in index)
    // 5: index(part columns matched, more columns in index)
    public ArrayList searchData(BP bp, Prediction prediction, HashSet selectColumns){
        if (!verifyState())
            return null;
        locker++;
        bp = bp==null?beeper.getBP():bp; 
        activeBPs.add(bp);
        ArrayList result = new ArrayList();

        if (prediction == null) // null means get full dataset
            prediction = new Prediction();
        if (!prediction.columnsAnalyzed())
            prediction.analyzeColumns(metaData,null);

        //debuger.printMsg("start optmizing", true);
        Optimizer optimizer = new Optimizer(metaData.getKeyColumns(),memType,memData.size(),indexes,dtrace);
        SearchMethod searchMethod = optimizer.newSearchMethod();
        optimizer.optimizeSearch(prediction, searchMethod, selectColumns);
        //debuger.printMsg("end optmizing", true);

        //debuger.printMsg("start searching", true);
        switch (searchMethod.method){
        case Consts.INDEXGET:
            if (searchMethod.indId == 0) // get key
                result = getDataByKey(bp, searchMethod.expressionAccesser, searchMethod.indexAccesser, searchMethod.indexFilter, searchMethod.dataFilter, selectColumns);
            else // get index
                result = getDataByIndex(bp, searchMethod.indId, searchMethod.expressionAccesser, searchMethod.indexAccesser, searchMethod.indexFilter, searchMethod.dataFilter, selectColumns);
            break;
        case Consts.FULLINDEXSCAN:
            if (searchMethod.indId == 0) // scan key
                result = searchDataFullScanKeyC(bp, searchMethod.expressionAccesser, searchMethod.indexFilter, searchMethod.dataFilter, selectColumns);
            else // get index
                result = searchDataFullScanIndexC(bp, searchMethod.indId, searchMethod.expressionAccesser, searchMethod.indexFilter, searchMethod.dataFilter, selectColumns);
            break;
        case Consts.PARTINDEXSCAN:
            //if (searchMethod.indId == 0) // scan key
            //    result = searchDataPartScanKeyC(bp, searchMethod.expressionAccesser, searchMethod.indexFilter, searchMethod.dataFilter);
            //else // get index
                result = searchDataPartScanIndexC(bp, searchMethod.indId, searchMethod.expressionAccesser, searchMethod.indexAccesser, searchMethod.indexFilter, searchMethod.dataFilter, selectColumns);
            break;
        default:
            result = searchDataFullC(bp, searchMethod.expressionAccesser, searchMethod.dataFilter, selectColumns);
        }
        /*try{     
            ArrayList columns = new ArrayList();
            ArrayList datas = new ArrayList();
            boolean allMatch = predictionColumns(prediction, columns, datas);
            boolean keyMatched = false;
            boolean hashMatched = false;
            // detect if hit key
            if (metaData.getKeyColumns().containsAll(columns))
                keyMatched = true;
            // full hitted key. get(key) directly
            if (keyMatched && allMatch && columns.size() == metaData.getKeyColumns().size()){
                FyDataEntry dataEntry = generateDataEntry(bp, columns, datas);
                result.add(getMemDataByKeyC(bp,dataEntry.key));
            }else{
                // detect if hit index. and choose the highest priority one
                Integer selID = new Integer(-1);  // selected index id
                int indSize = metaData.getColumns().size()+1; // seleted index columns number
                Iterator it = indexes.keySet().iterator();
                while (it.hasNext()){
                   Integer indID = (Integer)it.next();
                   FyIndex index = (FyIndex)indexes.get(indID);
                   if (index.getState() != Consts.VALID) // just update valid index
                      continue;
                   Set indColumns = new HashSet(index.getIndColumns());
                   if (indColumns.containsAll(columns)){ // candidate index
                       // if fulfill hash index requirements, choose it directly
                       if (allMatch && columns.size() == indColumns.size() && index.getIndType() == Consts.HASHMAP){
                           selID = indID;
                           hashMatched = true;
                           break;
                       }else if (indColumns.size() < indSize){
                           selID = indID;
                           indSize = indColumns.size();
                       }
                   }
                }
                if (!hashMatched && keyMatched){ // if hitted key and no hash index full hitted, choose key first
                    result = searchDataFull(bp, prediction); // process of search by key is same as full search
                }else if (selID.intValue() < 0)
                    result = searchDataFull(bp, prediction);
                else{
                    ArrayList keys = searchKeyFromIndex(bp, prediction, selID, allMatch&&(indSize==columns.size()));
                    result = getMemDatasByKeys(bp,keys);
                }
            }
        }catch(Exception e){
            locker--;
            activeBPs.remove(bp);
            dtrace.trace(10);
            if (debuger.isDebugMode())
               e.printStackTrace();
            return null;
        }//*/
        //debuger.printMsg("completed searching", true);

        locker--;
        activeBPs.remove(bp);
        return result;
    }

    // search data, filter by conditions
    // Only if the index contains all predicting columns, the index could be hitted;
    // Onlu if all predicting columns are key columns, vice versa, and all operators are = and all junctions are "and", the key has the highest priority
    // Only if all predicting columns are hash index columns, vice versa, and all operators are = and all junctions are "and", the hash index has the highest priority following key
    // Index with few columns number has higher priority then the one with more columns, key has higher priorit than other indexes
    // if no index could be hitted, access memData directly
    // 1: key(all columns matched with ==&"and")
    // 2: hash index(all columns matched with ==&"and")
    // 3: key(part columns matched)
    // 4: index(part columns matched, fewer columns in index)
    // 5: index(part columns matched, more columns in index)
    public ArrayList searchData(Prediction prediction, HashSet selectColumns){
        return searchData(null,prediction,selectColumns);
    }

    public ArrayList searchData(Prediction prediction){
        HashSet allColumns = new HashSet(metaData.getColumns().keySet());
        return searchData(prediction, allColumns);
    }

    // remove data, filter by conditions
    // get the kyes with filter first, then bacth delete data
    public int removeData(Prediction prediction){
        if (!verifyState())
            return -1;
        locker++;
        BP bp = beeper.getBP(); 
        activeBPs.add(bp);
        ArrayList keys = new ArrayList();

        int removedNum = 0;
        if (prediction == null) // null means get full dataset
            prediction = new Prediction();
        if (!prediction.columnsAnalyzed())
            prediction.analyzeColumns(metaData,null);

        HashSet selectColumns = new HashSet(metaData.getKeyColumns());

        //debuger.printMsg("start optmizing", true);
        Optimizer optimizer = new Optimizer(metaData.getKeyColumns(),memType,memData.size(),indexes,dtrace);
        SearchMethod searchMethod = optimizer.newSearchMethod();
        optimizer.optimizeSearch(prediction, searchMethod, selectColumns);
        //debuger.printMsg("end optmizing", true);

        //debuger.printMsg("start searching", true);
        switch (searchMethod.method){
        case Consts.INDEXGET:
            if (searchMethod.indId == 0) // get key
                keys = getDataByKey(bp, searchMethod.expressionAccesser, searchMethod.indexAccesser, searchMethod.indexFilter, searchMethod.dataFilter, selectColumns);
            else // get index
                keys = getDataByIndex(bp, searchMethod.indId, searchMethod.expressionAccesser, searchMethod.indexAccesser, searchMethod.indexFilter, searchMethod.dataFilter, selectColumns);
            break;
        case Consts.FULLINDEXSCAN:
            if (searchMethod.indId == 0) // scan key
                keys = searchDataFullScanKeyC(bp, searchMethod.expressionAccesser, searchMethod.indexFilter, searchMethod.dataFilter, selectColumns);
            else // get index
                keys = searchDataFullScanIndexC(bp, searchMethod.indId, searchMethod.expressionAccesser, searchMethod.indexFilter, searchMethod.dataFilter, selectColumns);
            break;
        case Consts.PARTINDEXSCAN:
            //if (searchMethod.indId == 0) // scan key
            //    result = searchDataPartScanKeyC(bp, searchMethod.expressionAccesser, searchMethod.indexFilter, searchMethod.dataFilter);
            //else // get index
                keys = searchDataPartScanIndexC(bp, searchMethod.indId, searchMethod.expressionAccesser, searchMethod.indexAccesser, searchMethod.indexFilter, searchMethod.dataFilter, selectColumns);
            break;
        default:
            keys = searchDataFullC(bp, searchMethod.expressionAccesser, searchMethod.dataFilter, selectColumns);
        }
        //debuger.printMsg("completed searching", true);
        removedNum = batchDeleteData(new HashSet(keys));

        locker--;
        activeBPs.remove(bp);
        return removedNum;
    }

    // modify data, filter by conditions
    // get the kyes with filter first, then bacth modify data
    public int modifyData(ArrayList colNames, ArrayList values, Prediction prediction){
        if (!verifyState())
            return -1;
        locker++;
        BP bp = beeper.getBP(); 
        activeBPs.add(bp);
        ArrayList keys = new ArrayList();

        int modifiedNum = 0;
        if (prediction == null) // null means get full dataset
            prediction = new Prediction();
        if (!prediction.columnsAnalyzed())
            prediction.analyzeColumns(metaData,null);

        HashSet selectColumns = new HashSet(metaData.getKeyColumns());

        //debuger.printMsg("start optmizing", true);
        Optimizer optimizer = new Optimizer(metaData.getKeyColumns(),memType,memData.size(),indexes,dtrace);
        SearchMethod searchMethod = optimizer.newSearchMethod();
        optimizer.optimizeSearch(prediction, searchMethod, selectColumns);
        //debuger.printMsg("end optmizing", true);

        //debuger.printMsg("start searching", true);
        switch (searchMethod.method){
        case Consts.INDEXGET:
            if (searchMethod.indId == 0) // get key
                keys = getDataByKey(bp, searchMethod.expressionAccesser, searchMethod.indexAccesser, searchMethod.indexFilter, searchMethod.dataFilter, selectColumns);
            else // get index
                keys = getDataByIndex(bp, searchMethod.indId, searchMethod.expressionAccesser, searchMethod.indexAccesser, searchMethod.indexFilter, searchMethod.dataFilter, selectColumns);
            break;
        case Consts.FULLINDEXSCAN:
            if (searchMethod.indId == 0) // scan key
                keys = searchDataFullScanKeyC(bp, searchMethod.expressionAccesser, searchMethod.indexFilter, searchMethod.dataFilter, selectColumns);
            else // get index
                keys = searchDataFullScanIndexC(bp, searchMethod.indId, searchMethod.expressionAccesser, searchMethod.indexFilter, searchMethod.dataFilter, selectColumns);
            break;
        case Consts.PARTINDEXSCAN:
            //if (searchMethod.indId == 0) // scan key
            //    result = searchDataPartScanKeyC(bp, searchMethod.expressionAccesser, searchMethod.indexFilter, searchMethod.dataFilter);
            //else // get index
                keys = searchDataPartScanIndexC(bp, searchMethod.indId, searchMethod.expressionAccesser, searchMethod.indexAccesser, searchMethod.indexFilter, searchMethod.dataFilter, selectColumns);
            break;
        default:
            keys = searchDataFullC(bp, searchMethod.expressionAccesser, searchMethod.dataFilter, selectColumns);
        }
        //debuger.printMsg("completed searching", true);
        modifiedNum = batchModifyMemDataByKeyS(new HashSet(keys),colNames,values);

        locker--;
        activeBPs.remove(bp);
        return modifiedNum;
    }

/*
    // internal process of modify data, with ASIGNED bp
    private boolean modifyMemDataByKeyInternal(BP bp, HashMap key, ArrayList<Integer> colIDs, ArrayList newValues){
        HashMap curValue = memData.get(key); // current value, to be modified
        HashMap crValue = getMemDataByKeyC(bp, key); // consistent read value, for log
        if (crValue == null)
        {
            //terminalBP(bp);
            return false;
        }

        // append log
        if (bufferLog || persistent)
        {
            HashMap newKey = generateNewKey(key, colIDs, newValues); //  build a new key, if columns not contain a key column with different data, it will return a key set equal to the old one.
            if (!newKey.equals(key)){ // if changes involved new key, it will generate a key not equal to the old one. modifying key will generate a delete and an insert log
                if ( memData.containsKey(newKey) && !crValue.equals(getMemDataByKeyC(bp, newKey))) { // detect duplicated key, require consistent get
                    //terminalBP(bp);
                    dtrace.trace(212);
                    return false;
                }
                FyLogEntry logD = new FyLogEntry(bp, 3, key, crValue); // generated a delete log
                for (int i=0; i<colIDs.size(); i++){
                    Integer colID = colIDs.get(i);
                    if (colID.intValue() < 0)
                        continue;
                    // update the values to generate a new entry
                    if (!metaData.isKeyColumn(colID)){ // record the non-key columns into value entry
                        curValue.remove(colID);
                        curValue.put(colID, newValues.get(i));
                    }
                }
                bp.increaseSeq();
                FyLogEntry logI = new FyLogEntry(bp, 1, newKey, curValue); // generated a insert log
                if (bufferLog) 
                {
                    logs.put(bp, logD);
                    logs.put(bp, logI);
                }
                if (persistent) // write to file for persistent
                {
                    try {
                        debuger.printMsg("begin write key changing log",true);
                        logWriter.write(logD.encodeString());
                        logWriter.write(logI.encodeString());
                        debuger.printMsg("begin flush key changing log",true);
                        logWriter.flush();
                        debuger.printMsg("end write key changing log",true);
                    } catch (IOException e) {
                        //terminalBP(bp);
                        dtrace.trace(211);
                        //dtrace.trace(curLogName);
                        return false;
                    }
                }
                memData.remove(key); //  delete old data entry
                memData.put(newKey, curValue);
            } else { // generate valueChangeEntry log for normal modify operation
                //Map newVal = new HashMap(); // (colid, {oldvalue, newvalue})
                FyLogEntry log = new FyLogEntry(bp, 2, key, null); // generated a modify log. to new the valueChangeEntry, we should new the logEntry first
                HashMap logVal = new HashMap();
                for (int i=0; i<colIDs.size(); i++){
                    FyLogEntry.valueChangeEntry valChange = log.generateNewChangeEntry();
                    Integer colID = colIDs.get(i);
                    if (colID.intValue() < 0)
                        continue;
                    // record valueChangeEntry
                    valChange.oldValue = (String)crValue.get(colID);
                    valChange.newValue = (String)newValues.get(i);
                    logVal.put(colID, valChange);
                    
                    // update memory data
                    if (!metaData.isKeyColumn(colID)){ // record the non-key columns into valueChanageEntry
                        curValue.remove(colID);
                        curValue.put(colID, newValues.get(i));
                    }
                    // update the values to generate a new entry
                }
                //newVal.put(new Integer(colID), valChange);
                log.setValue(logVal);
                if (bufferLog) 
                    logs.put(bp, log);
                if (persistent) // write to file for persistent
                {
                    try { 
                        debuger.printMsg("begin write modify log",true);
                        logWriter.write(log.encodeString());
                        debuger.printMsg("begin flush modify log",true);
                        logWriter.flush();
                        debuger.printMsg("end write modify log",true);
                    } catch (IOException e) {
                        //terminalBP(bp);
                        dtrace.trace(211);
                        //dtrace.trace(curLogName);
                        return false;
                    }
                }
                // update memory data
                memData.put(key, curValue); 
            }
        }

        return true;
    }
//*/
    // internal process of modify data, with ASIGNED bp, return modified value and logs (return in parameter)
    private HashMap modifyMemDataByKeyInternal(BP bp, HashMap key, ArrayList<Integer> colIDs, ArrayList newValues, FyDataLogEntry log, TreeMap curLogs){
        HashMap curValue = (HashMap)memData.get(key); // current value, to be modified
        HashMap crValue = getMemDataByKeyC(bp, key); // consistent read value, for log
        if (crValue == null){
            //terminalBP(bp);
            //debuger.printMsg(key.toString(),false);
            return null;
        }

        // generate valueChangeEntry log for normal modify operation
        //Map newVal = new HashMap(); // (colid, {oldvalue, newvalue})
        HashMap logVal = new HashMap();
        for (int i=0; i<colIDs.size(); i++){
            FyDataLogEntry.valueChangeEntry valChange = log.generateNewChangeEntry();
            Integer colID = colIDs.get(i);
            if (colID.intValue() < 0)
                continue;
            // record valueChangeEntry
            valChange.oldValue = (String)crValue.get(colID);
            valChange.newValue = (String)newValues.get(i);
            if (valChange.newValue==null && metaData.isNullable(colID) != 1){
                dtrace.trace(metaData.getColumnName(colID));
                dtrace.trace(248);
                return null;
            }
            logVal.put(colID, valChange);

            // update memory data
            if (!metaData.isKeyColumn(colID)){ // record the non-key columns into valueChanageEntry
                curValue.remove(colID);
                if (valChange.newValue != null)
                    curValue.put(colID, valChange.newValue);
            }
            // update the values to generate a new entry
        }
        //newVal.put(new Integer(colID), valChange);
        log.setValue(logVal);

        modifyRowInIndexes(bp,key,logVal,curLogs,false); // generate log only, not update indexes

        return curValue;
    }

    private boolean synchronizeLogsInNet(TreeMap curLogs){
        if (clients == null)
            return true;
        Iterator it = clients.keySet().iterator();
        boolean successed = true;
        while (it.hasNext()){
            String hostAddr = (String)it.next();
            CommunicationClient client = (CommunicationClient)clients.get(hostAddr);
            if (!client.sychronizeLogs(guid, curLogs))
                successed = false;
        }
        return successed;
    }

    // synchronize Logs in local, convert the logs as normal operations on datasets
    // if dataset is pending, store logs in pendingLogs
    public boolean synchronizeLogsLocal(TreeMap dataLogs){
        if (dataLogs == null || dataLogs.size() == 0)
            return true;
        if (quiesced){
            if (pendingLogs == null)
                pendingLogs = new TreeMap(Beeper.getComparator());
            pendingLogs.putAll(dataLogs);
        }else{
            if (pendingLogs != null){
                dataLogs.putAll(pendingLogs);
            }
            int lastOp = Consts.UNKNOWN;
            Iterator it = dataLogs.keySet().iterator();
            HashSet batchKeys = null;
            ArrayList batchColIDs = null;
            ArrayList batchValues = null;
            while (it.hasNext()){
                BP bp = (BP)it.next();
                FyDataLogEntry log = (FyDataLogEntry)dataLogs.get(bp);
                if (log.logData.op != lastOp){
                    // apply the logs in local, according to op type
                    if (batchKeys != null && batchKeys.size()>0){
                        switch(lastOp){
                            case Consts.INSERT:{
                                batchInsertDataI(batchColIDs, batchValues);
                            }
                            break;
                            case Consts.MODIFY:{
                                batchModifyMemDataByKeyI(batchKeys, batchColIDs, batchValues);
                            }
                            break;
                            case Consts.DELETE:{
                                batchDeleteData(batchKeys);
                            }
                            break;
                        }
                    }
                    batchKeys = new HashSet();
                    batchColIDs = new ArrayList();
                    batchValues = new ArrayList();
                    lastOp = log.logData.op;
                }
                switch(log.logData.op){
                    case Consts.INSERT:{
                        // build all columns ID to be inserted
                        if (batchColIDs == null || batchColIDs.size() == 0)
                            for (int i=0;i<metaData.getColumns().size();i++)
                                batchColIDs.add(new Integer(i));
                        ArrayList values = new ArrayList();
                        // build values
                        for (int i=0;i<batchColIDs.size();i++){
                            Integer colID = (Integer)batchColIDs.get(i);
                            values.add(metaData.isKeyColumn(colID)?(String)log.logData.key.get(colID):(String)log.logData.value.get(colID));
                        }
                        batchValues.add(values);
                    }
                    break;
                    case Consts.MODIFY:{
                        // build all non-key columns ID to be inserted
                        if (batchColIDs == null || batchColIDs.size() == 0)
                            for (int i=0;i<metaData.getColumns().size();i++){
                                Integer colID = (Integer)batchColIDs.get(i);
                                if (!metaData.isKeyColumn(colID))
                                    batchColIDs.add(new Integer(i));
                            }
                        batchKeys.add(new HashMap(log.logData.key));
                        ArrayList values = new ArrayList();
                        // build values
                        for (int i=0;i<batchColIDs.size();i++){
                            values.add((String)log.logData.value.get((Integer)batchColIDs.get(i)));
                        }
                        batchValues.add(values);
                    }
                    break;
                    case Consts.DELETE:{
                        batchKeys.add(new HashMap(log.logData.key));
                    }
                    break;
                }
            }
            // apply the last batch
            switch(lastOp){
                case Consts.INSERT:{
                    batchInsertDataI(batchColIDs, batchValues, false);
                }
                break;
                case Consts.MODIFY:{
                    batchModifyMemDataByKeyI(batchKeys, batchColIDs, batchValues, false);
                }
                break;
                case Consts.DELETE:{
                    batchDeleteData(batchKeys, false);
                }
                break;
            }
            pendingLogs=null;
        }
        return true;
    }

    public boolean applyChanges(TreeMap curLogs, boolean synchronizeInNet){
        if (curLogs == null || curLogs.size() == 0)
            return true;
        if (synchronizeInNet && !synchronizeLogsInNet(mergeDataLogs(null,null,curLogs))){
            //dtrace.trace(2012);
        }
        if (bufferLog) 
            logs.putAll(curLogs);
        // update memory dat
        updateLogsToMem(curLogs);
        if (persistent) // write to file for persistent
            if (!writeLogs(curLogs))
                return true;
            else
                return false;
        else
            return true;
    }

    // modify value of a set of column, identify columns by id
    public boolean modifyMemDataByKeyI(HashMap key, ArrayList<Integer> colIDs, ArrayList newValues){
        if (colIDs == null){
            dtrace.trace(214);
            return false;
        }
        if (newValues == null){
            dtrace.trace(215);
            return false;
        }
        if (colIDs.size() != newValues.size()){
            dtrace.trace(213);
            return false;
        }

        if (!verifyState())
            return false;
        locker++;
        BP bp  = beeper.getBP();
        activeBPs.add(bp);
        //boolean rest = false;
        try{
            TreeMap curLogs = new TreeMap(Beeper.getComparator());
            HashMap newKey = generateNewKey(key, colIDs, newValues); //  build a new key, if columns not contain a key column with different data, it will return a key set equal to the old one.
            if (!newKey.equals(key)){ // if changes involved new key, it will generate a key not equal to the old one. modifying key will generate a delete and an insert operations
                //if (getMemDataByKeyC(bp, newKey) != null){ // detect if duplicated, require consistent read;
                if (memData.containsKey(newKey)){ // detect if duplicated, require current mode read;
                    locker--;
                    activeBPs.remove(bp);
                    dtrace.trace(241);
                    return false;
                }

                HashMap value = getMemDataByKeyC(bp, key); // consistent read value of the key, to be kept in log. Return null means the data does not exist
                FyDataLogEntry logD = new FyDataLogEntry(Consts.DELETE, key, value); // generated a modify log. to new the valueChangeEntry, we should new the logEntry first
                if (!deleteDataByKeyInternal(bp, key, logD, curLogs)){
                    locker--;
                    bp.resetSeq();
                    activeBPs.remove(bp);
                    return false;
                }
                curLogs.put(new BP(bp), logD);
                bp.increaseSeq();

                FyDataEntry newData = mergeData(bp, newKey, colIDs, newValues);
                FyDataLogEntry logI = new FyDataLogEntry(Consts.INSERT, newKey, null); // generated a modify log. to new the valueChangeEntry, we should new the logEntry first
                if (!insertDataInternal(bp, newData, logI, curLogs)) {
                     locker--;
                     bp.resetSeq();
                     activeBPs.remove(bp);
                     return false;
                }
                curLogs.put(new BP(bp), logI);
                bp.increaseSeq();
            } else {
                FyDataLogEntry log = new FyDataLogEntry(Consts.MODIFY, key, null); // generated a modify log. to new the valueChangeEntry, we should new the logEntry first
                HashMap newValue = modifyMemDataByKeyInternal(bp, key, colIDs, newValues, log, curLogs);
                if (newValue == null){
                    locker--;
                    bp.resetSeq();
                    activeBPs.remove(bp);
                    return false;
                }
                curLogs.put(new BP(bp), log);
            }
            applyChanges(curLogs, true);
            //if (bufferLog) 
            //    logs.putAll(curLogs);
            // update memory dat
            //updateLogsToMem(curLogs);
            //if (persistent) // write to file for persistent
            //    if (!writeLogs(curLogs)){
            //        //terminalBP(bp);
            //        locker--;
            //        bp.resetSeq();
            //        activeBPs.remove(bp);
            //    }
            locker--;
            bp.resetSeq();
            activeBPs.remove(bp);
        }catch(Exception e){
            locker--;
            bp.resetSeq();
            activeBPs.remove(bp);
            dtrace.trace(10);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return false;
        }
        // modify memory data
        return true;
    }

    // call modifyMemDataByKeyI(ArrayList key, ArrayList colID, ArrayList newValues) 
    public boolean modifyMemDataByKeyS(HashMap key, ArrayList<String> colNames, ArrayList newValues){
        if (colNames == null){
            dtrace.trace(214);
            return false;
        }

        return modifyMemDataByKeyI(key, metaData.getColIDByName(colNames), newValues);
    }

    // batch modify value of a set of data, with assinged column&values, identify columns by id
    // return actually modified data number
    public int batchModifyMemDataByKeyI(HashSet keys, ArrayList<Integer> colIDs, ArrayList newValues, boolean synchronizeInNet){
        int modNum = 0;
        if (keys == null || keys.size() == 0){
            dtrace.trace(229);
            return modNum;
        }
        if (colIDs == null){
            dtrace.trace(214);
            return modNum;
        }
        if (newValues == null){
            dtrace.trace(215);
            return modNum;
        }
        if (colIDs.size() != newValues.size()){
            dtrace.trace(213);
            return modNum;
        }

        if (!verifyState())
            return modNum;
        locker++;
        BP bp  = beeper.getBP();
        activeBPs.add(bp);
        try{ ///??? key = key + 1 ???///
            HashSet newKeys = new HashSet(memData.keySet());
            newKeys.removeAll(keys);
            //boolean rest = false;
            TreeMap curLogs = new TreeMap(Beeper.getComparator()); // the logs genereated during current operation
            Iterator it = keys.iterator();
            while (it.hasNext()) {
                HashMap key = (HashMap)it.next();
                HashMap newKey = generateNewKey(key, colIDs, newValues); //  build a new key, if columns not contain a key column with different data, it will return a key set equal to the old one.
                if (!newKey.equals(key)){ // if changes involved new key, it will generate a key not equal to the old one. modifying key will generate a delete and an insert operations
                    // if (getMemDataByKeyC(bp, newKey) != null){ // detect if duplicated, require consistent read;
                    if (newKeys.contains(newKey)){ // detect if duplicated, require current mode read;
                        locker--;
                        bp.resetSeq();
                        activeBPs.remove(bp);
                        dtrace.trace(241);
                        return modNum;
                    }
                    newKeys.add(newKey);

                    HashMap value = getMemDataByKeyC(bp, key); // consistent read value of the key, to be kept in log. Return null means the data does not exist
                    FyDataLogEntry logD = new FyDataLogEntry(Consts.DELETE, key, value); // generated a modify log. to new the valueChangeEntry, we should new the logEntry first
                    if (!deleteDataByKeyInternal(bp, key, logD, curLogs)){
                         locker--;
                         bp.resetSeq();
                         activeBPs.remove(bp);
                         return modNum;
                    }
                    curLogs.put(new BP(bp), logD);
                    bp.increaseSeq();

                    FyDataEntry newData = mergeData(bp, key, colIDs, newValues);
                    FyDataLogEntry logI = new FyDataLogEntry(Consts.INSERT, newKey, null); // generated a modify log. to new the valueChangeEntry, we should new the logEntry first
                    if (!insertDataInternal(bp, newData, logI, curLogs)) {
                         locker--;
                         bp.resetSeq();
                         activeBPs.remove(bp);
                         return modNum;
                    }
                    curLogs.put(new BP(bp), logI);
                    bp.increaseSeq();
                } else {
                    FyDataLogEntry log = new FyDataLogEntry(Consts.MODIFY, key, null); // generated a modify log. to new the valueChangeEntry, we should new the logEntry first
                    HashMap newValue = modifyMemDataByKeyInternal(bp, key, colIDs, newValues, log, curLogs);
                    if (newValue == null){
                        continue;
                        //locker--;
                        //bp.resetSeq();
                        //activeBPs.remove(bp);
                        //return false;
                    }
                    curLogs.put(new BP(bp), log);
                    bp.increaseSeq();
                }
                modNum++;
            }
            applyChanges(curLogs,synchronizeInNet);
        }catch(Exception e){
            locker--;
            bp.resetSeq();
            activeBPs.remove(bp);
            dtrace.trace(10);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return modNum;
        }
        locker--;
        bp.resetSeq();
        activeBPs.remove(bp);
        // modify memory data
        return modNum;
    }

    public int batchModifyMemDataByKeyI(HashSet keys, ArrayList<Integer> colIDs, ArrayList newValues){
        return batchModifyMemDataByKeyI(keys,colIDs,newValues,true);
    }

    // batch modify value of a set of data, with assinged column&values, identify columns by id
    // call batchModifyMemDataByKeyI
    public int batchModifyMemDataByKeyS(HashSet keys, ArrayList<String> colNames, ArrayList newValues, boolean synchronizeInNet){
        if (colNames == null){
            dtrace.trace(214);
            return 0;
        }

        return batchModifyMemDataByKeyI(keys, metaData.getColIDByName(colNames), newValues, synchronizeInNet);
    }

    public int batchModifyMemDataByKeyS(HashSet keys, ArrayList<String> colNames, ArrayList newValues){
        return batchModifyMemDataByKeyS(keys,colNames,newValues,true);
    }

    // internal process of insert data, with ASIGNED bp, generate a insert log, which contains the new key and value
    private boolean insertDataInternal(BP bp, FyDataEntry data, FyDataLogEntry logI, TreeMap curLogs){
        if (data == null){
            dtrace.trace(238);
            return false;
        }
        if (data.key == null || data.key.size() == 0){
            dtrace.trace(237);
            return false;
        }
        // detect if duplicated, require current mode read;
        if (memData.containsKey(data.key)){
            dtrace.trace(241);
            return false;
        }

        logI.setKey(data.key);
        logI.setValue(data.value);
        insertRowToIndexes(bp, data, curLogs, false); // generate logs, not update indexes

        return true;
    }

    // internal process of insert data, with ASIGNED bp, generate a insert log, which contains the new key and value
    //private boolean insertDataInternal(BP bp, ArrayList<Integer> colIDs, ArrayList newValues, FyDataLogEntry logI, TreeMap curLogs){
    //    FyDataEntry data = generateDataEntry(bp, colIDs, newValues);

    //    return insertDataInternal(bp, data, logI, curLogs);
    //}

    // insert a data entry, input a set of column, identify columns by id
    public boolean insertDataI(ArrayList<Integer> colIDs, ArrayList newValues){
        if (colIDs == null){
            dtrace.trace(239);
            return false;
        }
        if (newValues == null){
            dtrace.trace(240);
            return false;
        }
        if (colIDs.size() != newValues.size()){
            dtrace.trace(213);
            return false;
        }

        if (!verifyState())
            return false;
        locker++;
        BP bp  = beeper.getBP();
        activeBPs.add(bp);
        try{
            TreeMap curLogs = new TreeMap(Beeper.getComparator());
            FyDataLogEntry logI = new FyDataLogEntry(Consts.INSERT, null, null); // generated a insert log
            FyDataEntry data = generateDataEntry(bp, colIDs, newValues);
            if (!insertDataInternal(bp, data, logI, curLogs)) {
                locker--;
                bp.resetSeq();
                activeBPs.remove(bp);
                return false;
            }
            curLogs.put(new BP(bp), logI);
            applyChanges(curLogs, true);
            // append log
            //if (bufferLog) 
            //    logs.putAll(curLogs);
            // update memory data, including index
            //updateLogsToMem(curLogs);
            //if (persistent) // write to file for persistent
            //    if (!writeLogs(curLogs)){
            //        //terminalBP(bp);
            //        locker--;
            //        bp.resetSeq();
            //        activeBPs.remove(bp);
            //    }

            locker--;
            bp.resetSeq();
            activeBPs.remove(bp);
        }catch(Exception e){
            locker--;
            bp.resetSeq();
            activeBPs.remove(bp);
            dtrace.trace(10);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return false;
        }
        return true;
    }

    // call insertDataI(ArrayList colID, ArrayList newValues) 
    public boolean insertDataS(ArrayList<String> colNames, ArrayList newValues){
        if (colNames == null){
            dtrace.trace(239);
            return false;
        }

        return insertDataI(metaData.getColIDByName(colNames), newValues);
    }

    // alias of insertDataS
     public boolean addData(ArrayList<String> colNames, ArrayList newValues){
        return insertDataS(colNames, newValues);
     }

    // batch insert a set of data entries, input a set of column, identify columns by id
    // input data item should be a content of specified columns, in sequence; 
    // return actually inserted number
    public int batchInsertDataI(ArrayList<Integer> colIDs, ArrayList<ArrayList> datas, boolean synchronizeInNet){
        int insNum = 0;
        if (colIDs == null){
            dtrace.trace(239);
            return insNum;
        }
        if (datas == null){
            dtrace.trace(245);
            return insNum;
        }

        if (!verifyState())
            return insNum;
        locker++;
        BP bp  = beeper.getBP();
        activeBPs.add(bp);
        HashSet newKeys = new HashSet(memData.keySet());
        TreeMap curLogs = new TreeMap(Beeper.getComparator()); // the logs genereated during current operation
        //ArrayList colIDs = new ArrayList((new java.util.TreeSet(metaData.getColumns().keySet()))); // convert hashkeys to sorted arraylist
        try{
            for (int i=0; i<datas.size(); i++){
                ArrayList newValues = datas.get(i);
                HashMap newKey = generateNewKey(new HashMap(), colIDs, newValues);
                if (newKeys.contains(newKey)){ // detect if duplicated, require current mode read;
                    locker--;
                    bp.resetSeq();
                    activeBPs.remove(bp);
                    dtrace.trace(241);
                    return insNum;
                }
                newKeys.add(newKey);
                FyDataLogEntry logI = new FyDataLogEntry(Consts.INSERT, null, null); // generated a insert log
                FyDataEntry data = generateDataEntry(bp, colIDs, newValues);
                if (!insertDataInternal(bp, data, logI, curLogs)){
                    locker--;
                    bp.resetSeq();
                    activeBPs.remove(bp);
                    return insNum;
                }
                insNum++;
                curLogs.put(new BP(bp), logI);
                bp.increaseSeq();
            }
        
            applyChanges(curLogs, synchronizeInNet);
            //if (bufferLog) 
            //    logs.putAll(curLogs);
            // update memory data, including index
            //updateLogsToMem(curLogs);
            //if (persistent && insNum>0)  // write to file for persistent
            //    if (!writeLogs(curLogs)){
            //        //terminalBP(bp);
            //        locker--;
            //        bp.resetSeq();
            //        activeBPs.remove(bp);
            //    }
        }catch(Exception e){
            locker--;
            bp.resetSeq();
            activeBPs.remove(bp);
            dtrace.trace(10);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return insNum;
        }

        locker--;
        bp.resetSeq();
        activeBPs.remove(bp);
        return insNum;
    }

    public int batchInsertDataI(ArrayList<Integer> colIDs, ArrayList<ArrayList> datas){
        return batchInsertDataI(colIDs,datas,true);
    }

    // batch insert a set of data entries, input a set of column, identify columns by id
    // input data item should be a content of specified columns, in sequence; 
    // call batchInsertDataI
    public int batchInsertDataS(ArrayList<String> colNames, ArrayList<ArrayList> datas, boolean synchronizeInNet){
        if (colNames == null){
            dtrace.trace(239);
            return 0;
        }
    
        return batchInsertDataI(metaData.getColIDByName(colNames), datas, synchronizeInNet);
    }

    public int batchInsertDataS(ArrayList<String> colNames, ArrayList<ArrayList> datas){
        return batchInsertDataS(colNames, datas, true);
    }

    // internal process of insert data, with ASIGNED bp. will generate the delete log
    private boolean deleteDataByKeyInternal(BP bp, HashMap key, FyDataLogEntry logD, TreeMap curLogs){
        if (key == null){
            dtrace.trace(244);
            return false;
        }
        //HashMap value = getMemDataByKeyC(bp, key); // consistent read value of the key, to be kept in log. Return null means the data does not exist
        //if (value == null){ // no data be deleted
        //    return false;
        //}
    
        //logD.setValue(value);
        deleteRowFromIndexes(bp, new FyDataEntry(key,logD.logData.value), curLogs, false); // just generate logs, not update indexes
        return true;
    }

    // delete a data entry, identify data by key, 
    public boolean deleteDataByKey(HashMap key){
        if (!verifyState())
            return false;
        locker++;
        BP bp  = beeper.getBP();
        activeBPs.add(bp);
        try{
            TreeMap curLogs = new TreeMap(Beeper.getComparator());
            HashMap value = getMemDataByKeyC(bp, key); // consistent read value of the key, to be kept in log. Return null means the data does not exist
            FyDataLogEntry logD = new FyDataLogEntry(Consts.DELETE, key, value); // generated a delete log
            if (!deleteDataByKeyInternal(bp, key, logD, curLogs)){
                locker--;
                activeBPs.remove(bp);
                return false;
            }
            curLogs.put(new BP(bp), logD);
            applyChanges(curLogs, true);
            // append log
            //if (bufferLog) 
            //    logs.putAll(curLogs);
            // update memory data, including index
            //updateLogsToMem(curLogs);
            //if (persistent)  // write to file for persistent
            //    if (!writeLogs(curLogs)){
            //        //terminalBP(bp);
            //        locker--;
            //        bp.resetSeq();
            //        activeBPs.remove(bp);
            //    }

            locker--;
            activeBPs.remove(bp);
        }catch(Exception e){
            locker--;
            activeBPs.remove(bp);
            dtrace.trace(10);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return false;
        }
        return true;
    }

    // batch delete a data entry, identify data by a set of keys, return actually deleted number
    public int batchDeleteData(HashSet<HashMap> keys, boolean synchronizeInNet){
        int delNum = 0;
        if (keys == null || keys.size() == 0)
            return delNum;
        if (keys == null){
            dtrace.trace(246);
            return delNum;
        }
    
        if (!verifyState())
            return delNum;
        locker++;
        BP bp  = beeper.getBP();
        activeBPs.add(bp);
        TreeMap curLogs = new TreeMap(Beeper.getComparator()); // the logs genereated during current operation
        try{
            Iterator it = keys.iterator();
            while (it.hasNext()){
                HashMap key = (HashMap)it.next();
                HashMap value = getMemDataByKeyC(bp, key); // consistent read value of the key, to be kept in log. Return null means the data does not exist
                FyDataLogEntry logD = new FyDataLogEntry(Consts.DELETE, key, value); // generated a delete log
                if (!deleteDataByKeyInternal(bp, key, logD, curLogs)){
                    continue;
                    //locker--;
                    //bp.resetSeq();
                    //activeBPs.remove(bp);
                    //return false;
                }
                delNum++;
                curLogs.put(new BP(bp),logD);
                bp.increaseSeq();
            }
            applyChanges(curLogs, synchronizeInNet);
            // append log
            //if (bufferLog) 
            //    logs.putAll(curLogs);
            // update memory data, including index
            //updateLogsToMem(curLogs);
            //if (persistent && delNum>0) // write to file for persistent
            //    if (!writeLogs(curLogs)){
            //        //terminalBP(bp);
            //        locker--;
            //        bp.resetSeq();
            //        activeBPs.remove(bp);
            //    }
        }catch(Exception e){
            locker--;
            bp.resetSeq();
            activeBPs.remove(bp);
            dtrace.trace(10);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return delNum;
        }

        locker--;
        bp.resetSeq();
        activeBPs.remove(bp);
        return delNum;
    }

    // batch delete a data entry, identify data by a set of keys, return actually deleted number
    public int batchDeleteData(HashSet<HashMap> keys){
        return batchDeleteData(keys, true);
    }

    // merge logs to compress size, reduce log applys. Retrieve all data logs and merge them
    // startBP is inclusive, endBP is exclusive
    // merged logs just used for redo on physical data.
    // 1. Just logs with same key could be merged
    // 2. Only BP+ can overwrite BP-
    // 3. Update overwrite values/BP of Insert, discard OP;
    // 4. Delete counteract on Insert, discard the log;
    // 5. Insert can not follow an Insert;
    // 6. Insert overwrite Delete, including BP and Values. And change OP to Update;
    // 7. Updeate&Delete can not follow a Delete;
    // 8. Delete will overwrite Update, including OP, BP and Values(null);
    // 9. Insert can not follow an Update;
    // 10. Update will overwrite Update values, including OP, BP and Values;
    private TreeMap mergeDataLogs(BP startBP, BP endBP, TreeMap rawLogs){
        //if (startBP == null)
        //    startBP = (BP)rawLogs.firstKey();
        //if (endBP == null)
        //    endBP = (BP)logs.lastKey();
        if (rawLogs == null || rawLogs.size() == 0)
            return new TreeMap();
        TreeMap mergedLogs = new TreeMap(Beeper.getComparator());
        TreeMap tempdLogs = new TreeMap(Beeper.getComparator());
        HashMap keyList = new HashMap(); // merged keys
        if (startBP==null && endBP==null)
            tempdLogs.putAll(rawLogs);
        else if (startBP==null && endBP!=null)
            tempdLogs.putAll(rawLogs.tailMap(endBP));
        else if (startBP!=null && endBP==null)
            tempdLogs.putAll(rawLogs.headMap(startBP));
        else
            tempdLogs.putAll(rawLogs.subMap(endBP,startBP));
        Iterator it = tempdLogs.keySet().iterator();
        while (it.hasNext()){
            BP newBp = (BP)it.next();
            FyBaseLogEntry rawLog = (FyBaseLogEntry)tempdLogs.get(newBp);
            if (rawLog == null)
                continue;
            if (rawLog.logType != Consts.DATA){
                // mergedLogs.put(newBp, rawLog);
                continue;
            }
            FyDataLogEntry newLog = (FyDataLogEntry)tempdLogs.get(newBp);
            BP oldBp = (BP)keyList.get(newLog.logData.key);
            if (oldBp != null){
                FyDataLogEntry oldLog = (FyDataLogEntry)mergedLogs.get(oldBp);
                switch (oldLog.logData.op){
                case Consts.INSERT:
                    switch (newLog.logData.op){
                    case Consts.INSERT:
                        dtrace.trace(1002);
                        continue;
                    case Consts.MODIFY: // Update new values will overwrite values/BP of Insert, discard OP;
                        if (newLog.logData.value == null)
                            continue;
                        Iterator colIt = newLog.logData.value.keySet().iterator();
                        while (colIt.hasNext()){
                            Integer colID = (Integer)colIt.next();
                            FyDataLogEntry.valueChangeEntry chgEntry =(FyDataLogEntry.valueChangeEntry)newLog.logData.value.get(colID);
                            oldLog.logData.value.put(colID, chgEntry.newValue);
                        }
                        mergedLogs.remove(oldBp);
                        mergedLogs.put(newBp, oldLog);
                        keyList.put(newLog.logData.key, newBp);
                        break;
                    case Consts.DELETE: // Delete counteract on Insert, discard the log;
                        mergedLogs.remove(oldBp);
                        keyList.remove(newLog.logData.key);
                        break;
                    default:
                        dtrace.trace(1001);
                        continue;
                    }
                    break;
                case Consts.MODIFY:
                    switch (newLog.logData.op){
                        case Consts.INSERT:
                            dtrace.trace(1002);
                            continue;
                        case Consts.MODIFY: // Update will overwrite Update values, including OP, BP and Values;
                            if (newLog.logData.value == null)
                                continue;
                            Iterator colIt = newLog.logData.value.keySet().iterator();
                            while (colIt.hasNext()){
                                Integer colID = (Integer)colIt.next();
                                FyDataLogEntry.valueChangeEntry chgEntry =(FyDataLogEntry.valueChangeEntry)newLog.logData.value.get(colID);
                                oldLog.logData.value.put(colID, chgEntry);
                            }
                            mergedLogs.remove(oldBp);
                            mergedLogs.put(newBp, oldLog);
                            keyList.put(newLog.logData.key, newBp);
                            break;
                        case Consts.DELETE: // Delete will overwrite Update, including OP, BP and Values(null);
                            mergedLogs.remove(oldBp);
                            mergedLogs.put(newBp, newLog);
                            keyList.put(newLog.logData.key, newBp);
                            break;
                        default:
                            dtrace.trace(1001);
                            continue;
                    }
                    break;
                case Consts.DELETE: // Insert overwrite Delete, including BP and Values. And change OP to Update;
                    switch (newLog.logData.op){
                            case Consts.INSERT:
                                if (newLog.logData.value == null)
                                    continue;
                                Iterator colIt = newLog.logData.value.keySet().iterator();
                                while (colIt.hasNext()){
                                    Integer colID = (Integer)colIt.next();
                                    FyDataLogEntry.valueChangeEntry chgEntry = oldLog.generateNewChangeEntry();
                                    chgEntry.newValue = (String)newLog.logData.value.get(colID);
                                    chgEntry.oldValue = (String)oldLog.logData.value.get(colID);
                                    oldLog.logData.value.put(colID, chgEntry);
                                }
                                oldLog.logData.op = Consts.MODIFY;
                                mergedLogs.remove(oldBp);
                                mergedLogs.put(newBp, oldLog);
                                keyList.put(newLog.logData.key, newBp);
                            break;
                        case Consts.MODIFY:
                            dtrace.trace(1002);
                            continue;
                        case Consts.DELETE:
                            dtrace.trace(1002);
                            continue;
                        default:
                            dtrace.trace(1001);
                            continue;
                    }
                    break;
                default:
                    dtrace.trace(1001);
                    continue;
                }
            }else{
                keyList.put(newLog.logData.key, newBp);
                mergedLogs.put(newBp, newLog);
            }
        }
        return mergedLogs;
    }

    // merge logs to compress size, reduce log applys. Retrieve all index logs and merge them
    // merging base on indexed data of an index
    private TreeMap mergeIndexLogs(BP startBP, BP endBP, int indId, FyLogData rawLogs){
        TreeMap mergedLogs = new TreeMap(Beeper.getComparator());
        FyIndex index = (FyIndex)indexes.get(Integer.valueOf(indId));
        if (index == null || index.getState() != Consts.VALID || index.getIndData() == null)
            return mergedLogs;

        if (startBP == null)
            startBP = (BP)rawLogs.firstKey();
        //if (endBP == null)
        //    endBP = (BP)logs.lastKey();
        TreeMap tempdLogs = new TreeMap(Beeper.getComparator());
        HashMap indexedDataList = new HashMap(); // indexed data list (indexedData:ArrayList of related logs' logBp)
        tempdLogs.putAll(rawLogs.subMap(endBP,startBP));
        Iterator it = tempdLogs.keySet().iterator();
        while (it.hasNext()){
            BP newBp = (BP)it.next();
            FyBaseLogEntry rawLog = (FyBaseLogEntry)tempdLogs.get(newBp);
            if (rawLog == null)
                continue;
            if (rawLog.logType != Consts.INDEX){
                // mergedLogs.put(newBp, rawLog);
                continue;
            }
            FyIndexLogEntry newLog = (FyIndexLogEntry)tempdLogs.get(newBp);
            ArrayList existingLogs = (ArrayList)indexedDataList.get(newLog.datas);
            if (existingLogs != null){
                //merge new log with existing logs
                boolean matched = false;
                //check if can match an existing log
                for (int i=0;i<existingLogs.size();i++){
                    BP oldBp = (BP)existingLogs.get(i);
                    FyIndexLogEntry oldLog = (FyIndexLogEntry)rawLogs.get(oldBp);
                    if (oldLog == null)
                        continue;
                    // datas & key are matched, then merge.
                    // since index logs just contains INSERT/DELETE, if new log matched a old log, it means they will counteract.
                    if (oldLog.datas.equals(newLog.datas) && oldLog.key.equals(newLog.key)){
                        if (oldLog.op == newLog.op){
                            dtrace.trace(1002);
                        }else{
                            mergedLogs.remove(oldBp);
                            existingLogs.remove(oldBp);
                            indexedDataList.put(newLog.datas, existingLogs);
                        }
                        matched = true;
                        break;
                    }
                }
                if (!matched){
                    indexedDataList.put(newLog.datas, existingLogs);
                    mergedLogs.put(newBp, newLog);
                }
            }else{
                existingLogs = new ArrayList();
                indexedDataList.put(newLog.datas, existingLogs);
                mergedLogs.put(newBp, newLog);
            }
        }
        return mergedLogs;
    }
    
    // save log control data to file
    public boolean saveLogControl(BP endBp){
        String logControlFile = baseLogName+".sys";
        try{ // initialize reader and writer of metadata file.
            int bufLen = 16;
            File dummmyfile = new File(logControlFile);
            if (!dummmyfile.exists()){
                dtrace.trace(logControlFile);
                dtrace.trace(21);
                dummmyfile.getParentFile().mkdirs();
                dummmyfile.createNewFile();
            }
            BufferedWriter dataWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dummmyfile.getCanonicalPath(), false)));
            char dataEntry[] = new char[bufLen*4];
            String tmpStr = String.valueOf(curLogSeq);
            tmpStr.getChars(0,tmpStr.length(),dataEntry,0);
            tmpStr = String.valueOf(endBp.level0);
            tmpStr.getChars(0,tmpStr.length(),dataEntry,bufLen*1);
            tmpStr = String.valueOf(endBp.level1);
            tmpStr.getChars(0,tmpStr.length(),dataEntry,bufLen*2);
            tmpStr = String.valueOf(endBp.level2);
            tmpStr.getChars(0,tmpStr.length(),dataEntry,bufLen*3);
            dataWriter.write(dataEntry);
            dataWriter.flush();
            dataWriter.close();
            return true;
        }
        catch (Exception e){
            dtrace.trace(logControlFile);
            dtrace.trace(23);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return false;
        }
    }
    
    // switch to another log file
    private void switchLogFile(){
        curLogSeq++;
        if (curLogSeq >= (Integer)paras.getParameter("logNumber"))
            curLogSeq = 1;
        logWriter.switchWriter(baseLogName+"_"+String.valueOf(curLogSeq)+".log");
    }
    
    // release logs from memory. endBP is exclusive
    private void releaseLogs(BP endBp){
        Iterator it = logs.keySet().iterator();
        while (it.hasNext()){
            BP curBp = (BP)it.next();
            if (beeper.compareBP(curBp, endBp, true)<0)
                //logs.remove(curBp);
                it.remove();
            else
                break;
        }
    }
    
    // implement logs to physical data. endBP is exclusive
    public int implementLogs(BP endBp){
        if (logs == null || logs.size() == 0)
            return 0;
        try{
            TreeMap tempLogs = mergeDataLogs(beeper.compare(lastAppliedBp, endBp)<=0?lastAppliedBp:endBp,endBp,logs);
            int failedLogs = 0;
            // for db data source, only the master can implement logs to data source;
            // for file data source, if data is stored in local, it should implement logs
            if ((phyType >= 1 && phyType <= 10 && isMaster(false)) || 
                (phyType >= 11 && phyType <= 20 && storeLocal))
                failedLogs = phyData.implementLog(tempLogs, true);
            saveLogControl(endBp);
            lastAppliedBp = endBp;

            // release logs from memory. 
            // It seems this flag is unnecessary, because we've kept active BPs, which will not be implemented & released
            //if (!keepLogInMemory) 
                releaseLogs(endBp);
            if (logWriter.getFileSize() >= (Integer)paras.getParameter("logFileSize")) // switch log file
                switchLogFile();
            return failedLogs;
        }catch (Exception e){
            dtrace.trace(10);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return 0;
        }
    }
    
    // get the last appliable bp, which mean get the smallest bp - 1 from activeBps
    public BP getLastAppliableBp(){
       BP lastAppliableBp = beeper.getCurBP();
       for (int i=0;i<activeBPs.size();i++){
           BP activeBp = (BP)activeBPs.get(i);
           lastAppliableBp = (beeper.compare(activeBp,lastAppliableBp)<0?(new BP(activeBp)):lastAppliableBp);
       }
       return lastAppliableBp;
    }
    
    // get the size of the datasets
    public long sizeOf(){
        long totalSize = 0;
        // calculate metadata size
        totalSize += metaData.sizeOf();

        // calculate indexes size
        Iterator it = indexes.values().iterator();
        while (it.hasNext()){
            totalSize += 4; // 4 bytes for index id
            FyIndex index = (FyIndex)it.next();
            totalSize += index==null?0:index.sizeOf();
        }

        // calculate memory data size
        it = memData.keySet().iterator();
        while (it.hasNext()){
            HashMap key = (HashMap)it.next();
            totalSize += CommUtility.sizeOf(key);
            HashMap value = (HashMap)memData.get(key);
            totalSize += CommUtility.sizeOf(value);
        }

        return totalSize;
    }

    // release resource
    public boolean release(){
        if (!awake()) // wait quice awake by other process
            return false;
        quiesced = true;
        try{
            // wait all sessions release dataset
            while (locker > 0){
                try{
                    Thread.sleep((Integer)paras.getParameter("_spinTime"));
                }
                catch (InterruptedException e) {
                    dtrace.trace(e.getMessage());
                    dtrace.trace(101);
                }
            }
            if (logWriter != null){
                logWriter.flush();
                logWriter.close();
            }
            implementLogs(beeper.getCurBP());
            //saveLogControl(beeper.getCurBP()); // have saved in implementLogs()
            if (dbconn != null && !dbconn.isClosed())
                dbconn.close();
        }catch (Exception e){
            dtrace.trace(105);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return false;
        }finally{
            loaded = false;
            initialized = false;
            Iterator it = indexes.values().iterator();
            while (it.hasNext()){
                FyIndex index = (FyIndex)it.next();
                index.release();
            }
            if (activeBPs!= null)
                activeBPs.clear();
            if (metaData!= null)
                metaData.releaseData();
            if (logs!= null)
                logs.clear();
            if (phyData!= null)
                phyData.release();
            if (memData!= null)
                memData.releaseAll();
            if (keyColumns!= null)
                keyColumns.clear();
            quiesced = false;
            return true;
        }
    }
    
    public String getGuid(){
        return guid;
    }
    
    // for test
    public MemBaseData getMemData(){
        return memData;
    }
}
