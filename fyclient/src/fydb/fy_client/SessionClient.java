/**
 * @(#)SessionClient.java	0.01 11/05/18
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_client;

import fydb.fy_comm.CommUtility;
import fydb.fy_comm.Debuger;
import fydb.fy_comm.FyMetaData;
import fydb.fy_comm.InitParas;
import fydb.fy_comm.NetThread;
import fydb.fy_comm.Prediction;
import fydb.fy_comm.SessionBase;
import fydb.fy_comm.Tracer;
import fydb.fy_comm.Consts;

//import fydb.fy_data.FyDataSet;

import java.net.Socket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
//import java.util.Map;

public class SessionClient extends NetThread implements SessionBase{
    protected InitParas paras;
    protected Tracer dtrace;
    protected Debuger debuger;

    private int asynLastCallCmd;   // the last asynchronized call command
    private int asynStatus = Consts.CLEAR; // status of asynchronized calling 

    private ArrayList asynCallParameters;  // parameters of synchronized call
    private Object asynResult;   // store the result of asynchronized calling
    private int sessionID;
    private boolean isClosed = false;
    private boolean isSuperMan = false;

    // local session server
    public SessionClient(int sessionID, InitParas paras, Tracer dtrace, Debuger debuger, Socket clientSocket) {
        super(paras, dtrace, debuger);
        super.setClientSocket(clientSocket);

        this.paras = paras;
        this.dtrace = dtrace;
        this.debuger = debuger;
        this.sessionID = sessionID;

        asynCallParameters = new ArrayList();

        setName("SessionClient");
    }

    /**
     * release session resource
     * */
     public void release() {
         if (!requireChannel())
             return;
         writeBytes(CommUtility.intToByteArray(Consts.BYEBYE)); // send cmd type
         releaseChannel();
         terminate();
         asynCallParameters.clear();
         asynStatus = Consts.CLEAR;
         super.closeSocket();
         isClosed = true;
     }

    /**
     * get session ID
     * */
    public int getSessionID() {
        return sessionID;
    }

    /**
     * Detect if session closed.
     * */
    public boolean isClosed(){
        return super.isClosed();
        //return isClosed;
    }

    /**
     * build a key with specified column names and values
     * @param dataSetName dataset name
     * @param colNames column names
     * @param values column values. position in the array should be kept aligned with column names
     * */
    public HashMap buildKeyWithName(String dataSetName, ArrayList<String> colNames, ArrayList<String> values) {
        return (getMetaData(dataSetName)).buildKeyWithColName(colNames, values);
    }

    /**
     * get meta rightExpression of dataset
     * 
     * @return FyMetaData
     */
    public FyMetaData getMetaData(String dataSetName){
        if (!requireChannel())
            return null;
        writeBytes(CommUtility.intToByteArray(Consts.GETMETADATA)); // send cmd type
        writeObject(dataSetName);   // send parameters
        FyMetaData metaData = (FyMetaData)readObject();
        releaseChannel();
        if (metaData != null)
            metaData.setTracer(dtrace);
        return metaData;
    }

    /**
     * read single value from specified dataset
     * @param dataSetName dataset name
     * @param key key of rightExpression to be read
     */
    public HashMap singleRead(String dataSetName, HashMap key) {
        if (!requireChannel())
            return null;
        writeBytes(CommUtility.intToByteArray(Consts.SINGLEREAD)); // send cmd type
        writeObject(dataSetName);   // send parameters
        writeObject(key);           // send parameters
        HashMap data = (HashMap)readObject();
        releaseChannel();
        return data;
    }

    public boolean modifyDataByKeyI(String dataSetName, HashMap key,
                                    ArrayList<Integer> colIDs,
                                    ArrayList newValues) {
        if (!requireChannel())
            return false;
        writeBytes(CommUtility.intToByteArray(Consts.MODIFYDATABYKEI)); // send cmd type
        writeObject(dataSetName);   // send parameters
        writeObject(key);           // send parameters
        writeObject(colIDs);        // send parameters
        writeObject(newValues);     // send parameters
        byte[] rest = readBytes();
        releaseChannel();
        return rest!=null&&rest[0]==0x01;
    }

    public boolean modifyDataByKeyS(String dataSetName, HashMap key,
                                    ArrayList<String> colNames,
                                    ArrayList newValues) {
        if (!requireChannel())
            return false;
        writeBytes(CommUtility.intToByteArray(Consts.MODIFYDATABYKES)); // send cmd type
        writeObject(dataSetName);   // send parameters
        writeObject(key);           // send parameters
        writeObject(colNames);      // send parameters
        writeObject(newValues);     // send parameters
        byte[] rest = readBytes();
        releaseChannel();
        return rest!=null&&rest[0]==0x01;
    }

    public boolean modifyDataByKeyM(String dataSetName, HashMap key,
                                    HashMap mappedDatas) {
        writeBytes(CommUtility.intToByteArray(Consts.MODIFYDATABYKEM)); // send cmd type
        writeObject(dataSetName);   // send parameters
        writeObject(key);           // send parameters
        writeObject(mappedDatas);   // send parameters
        byte[] rest = readBytes();
        releaseChannel();
        return rest!=null&&rest[0]==0x01;
    }

    /**batch modify value of a set of rightExpression, with assinged column&values, identify columns by id. return modified number
     * @param dataSetName dataset name
     * @param colNames column names being modified
     * @param newValues new rightExpression
     * @param asynchronized asynchronized mode. if called in asynchronized mode, it will return immediately.
     * @return actually modified rightExpression number. 
     * if called asynchronized mode, and if asynchronization status is clear, return 0, otherwise, return -1
     */
    public int batchModifyMemDataByKey(String dataSetName, HashSet keys,
                                       ArrayList<String> colNames,
                                       ArrayList newValues, boolean asynchronized) {
        if (asynStatus != Consts.CLEAR && asynStatus != Consts.RUNNING) { 
            dtrace.trace(5006);
            return -1;
        }
        if (asynchronized){
            asynStatus = Consts.PREINITIALIZING; // prepare initialize asyn call
            asynCallParameters.clear();
            asynCallParameters.add(dataSetName);
            asynCallParameters.add(keys);
            asynStatus = Consts.INITIALIZING;   // prepared initialize asyn call
            asynLastCallCmd = Consts.BATCHMODIFYMEMDATABYKEY;
            this.start();
            return 0;
        }else{
            if (!requireChannel())
                return -1;
            writeBytes(CommUtility.intToByteArray(Consts.BATCHMODIFYMEMDATABYKEY)); // send cmd type
            writeObject(dataSetName);   // send parameters
            writeObject(keys);          // send parameters
            writeObject(colNames);      // send parameters
            writeObject(newValues);     // send parameters
            int modNum = ((Integer)readObject()).intValue();
            releaseChannel();
            return modNum;
        }
    }

    public boolean insertDataI(String dataSetName, ArrayList<Integer> colIDs,
                               ArrayList newValues) {
        if (!requireChannel())
            return false;
        writeBytes(CommUtility.intToByteArray(Consts.INSERTDATAI)); // send cmd type
        writeObject(dataSetName);   // send parameters
        writeObject(colIDs);        // send parameters
        writeObject(newValues);     // send parameters
        byte[] rest = readBytes();
        releaseChannel();
        return rest!=null&&rest[0]==0x01;
    }

    public boolean insertDataS(String dataSetName, ArrayList<String> colNames,
                               ArrayList newValues) {
        if (!requireChannel())
            return false;
        writeBytes(CommUtility.intToByteArray(Consts.INSERTDATAS)); // send cmd type
        writeObject(dataSetName);   // send parameters
        writeObject(colNames);      // send parameters
        writeObject(newValues);     // send parameters
        byte[] rest = readBytes();
        releaseChannel();
        return rest!=null&&rest[0]==0x01;
    }

    public boolean insertDataM(String dataSetName, HashMap mappedDatas) {
        if (!requireChannel())
            return false;
        writeBytes(CommUtility.intToByteArray(Consts.INSERTDATAM)); // send cmd type
        writeObject(dataSetName);   // send parameters
        writeObject(mappedDatas);   // send parameters
        byte[] rest = readBytes();
        releaseChannel();
        return rest!=null&&rest[0]==0x01;
    }

    /**batch insert a set of rightExpression, with assinged columns, and a set of values. return inserted number
     * @param dataSetName dataset name
     * @param colNames column names inserting rightExpression
     * @param datas inserting rightExpression
     * @param asynchronized asynchronized mode. if called in asynchronized mode, it will return immediately.
     * @return actually inserted rightExpression number. 
     * if called asynchronized mode, and if asynchronization status is clear, return 0, otherwise, return -1
     */
    public int batchInsertData(String dataSetName, ArrayList<String> colNames,
                               ArrayList<ArrayList> datas, boolean asynchronized) {
        if (asynStatus != Consts.CLEAR && asynStatus != Consts.RUNNING) { 
            dtrace.trace(5006);
            return -1;
        }
        if (asynchronized){
            asynStatus = Consts.PREINITIALIZING; // prepare initialize asyn call
            asynCallParameters.clear();
            asynCallParameters.add(dataSetName);
            asynCallParameters.add(colNames);
            asynCallParameters.add(datas);
            asynStatus = Consts.INITIALIZING;   // prepared initialize asyn call
            asynLastCallCmd = Consts.BATCHINSERTDATA;
            this.start();
            return 0;
        }else{
            if (!requireChannel())
                return -1;
            writeBytes(CommUtility.intToByteArray(Consts.BATCHINSERTDATA)); // send cmd type
            writeObject(dataSetName);   // send parameters
            writeObject(colNames);      // send parameters
            writeObject(datas);         // send parameters
            int insNum = ((Integer)readObject()).intValue();
            releaseChannel();
            return insNum;
        }
    }

    // delete a data entry, by key
    public boolean deleteDataByKey(String dataSetName, HashMap key) {
        if (!requireChannel())
            return false;
        writeBytes(CommUtility.intToByteArray(Consts.DELETEDATABYKEY)); // send cmd type
        writeObject(dataSetName);   // send parameters
        writeObject(key);           // send parameters
        byte[] rest = readBytes();
        releaseChannel();
        return rest!=null&&rest[0]==0x01;
    }

    /**
     * batch delete a set of rightExpression, with assinged keys. return actually deleted number
     * @param dataSetName dataset name
     * @param keys keys to be deleted
     * @param asynchronized asynchronized mode. if called in asynchronized mode, it will return immediately.
     * @return actually deleted rightExpression number. 
     * if called asynchronized mode, and if asynchronization status is clear, return 0, otherwise, return -1
     */
    public int batchDeleteData(String dataSetName, HashSet<HashMap> keys, boolean asynchronized) {
        if (asynStatus != Consts.CLEAR && asynStatus != Consts.RUNNING) { 
            dtrace.trace(5006);
            return -1;
        }
        if (asynchronized){
            asynStatus = Consts.PREINITIALIZING; // prepare initialize asyn call
            asynCallParameters.clear();
            asynCallParameters.add(dataSetName);
            asynCallParameters.add(keys);
            asynStatus = Consts.INITIALIZING;   // prepared initialize asyn call
            asynLastCallCmd = Consts.BATCHDELETEDATA;
            this.start();
            return 0;
        }else{
            if (!requireChannel())
                return -1;
            writeBytes(CommUtility.intToByteArray(Consts.BATCHDELETEDATA)); // send cmd type
            writeObject(dataSetName);   // send parameters
            writeObject(keys);          // send parameters
            int delNum = ((Integer)readObject()).intValue();
            releaseChannel();
            return delNum;
        }
    }

    /**
     * get all datasets list
     * 
     * @return ArrayList
     * */
     public ArrayList getDataSetsList(){
         if (!requireChannel())
             return null;
         writeBytes(CommUtility.intToByteArray(Consts.GETDATASETLIST)); // send cmd type
         ArrayList dsList = (ArrayList)readObject();
         releaseChannel();
         return dsList;
     }

    /**
     * search data from dataset, filtered by conditions
     * @param dataSetName dataset name
     * @param prediction criterias of searching
     * @param asynchronized running mode
     * @return ArrayList list of values (FyDataEntry)
     */
    public ArrayList searchData(String dataSetName, Prediction prediction, boolean asynchronized) {
        if (asynStatus != Consts.CLEAR && asynStatus != Consts.RUNNING) { 
            dtrace.trace(5006);
            return null;
        }
        if (asynchronized){
            asynStatus = Consts.PREINITIALIZING; // prepare initialize asyn call
            asynCallParameters.clear();
            asynCallParameters.add(dataSetName);
            asynCallParameters.add(prediction);
            asynStatus = Consts.INITIALIZING;   // prepared initialize asyn call
            asynLastCallCmd = Consts.SEARCHDATA;
            this.start();
            return null;
        }else{
            if (!requireChannel())
                return null;
            writeBytes(CommUtility.intToByteArray(Consts.SEARCHDATA)); // send cmd type
            writeObject(dataSetName);                    // send parameters
            writeObject(prediction);                     // send parameters
            ArrayList dsList = (ArrayList)readObject();
            releaseChannel();
            return dsList;
        }
    }
    
    /**
     * remove data from dataset, filtered by conditions
     * @param dataSetName dataset name
     * @param prediction criterias of searching
     * @param asynchronized running mode
     * @return int number of data removed, -1 means failed
     */
    public int removeData(String dataSetName, Prediction prediction, boolean asynchronized) {
        if (asynStatus != Consts.CLEAR && asynStatus != Consts.RUNNING) { 
            dtrace.trace(5006);
            return -1;
        }
        if (asynchronized){
            asynStatus = Consts.PREINITIALIZING; // prepare initialize asyn call
            asynCallParameters.clear();
            asynCallParameters.add(dataSetName);
            asynCallParameters.add(prediction);
            asynStatus = Consts.INITIALIZING;   // prepared initialize asyn call
            asynLastCallCmd = Consts.REMOVEDATA;
            this.start();
            return 0;
        }else{
            if (!requireChannel())
                return -1;
            writeBytes(CommUtility.intToByteArray(Consts.REMOVEDATA)); // send cmd type
            writeObject(dataSetName);                    // send parameters
            writeObject(prediction);                     // send parameters
            int removedNum = CommUtility.byteArrayToInt(readBytes());
            releaseChannel();
            return removedNum;
        }
    }

    /**
     * add data in dataset
     * @param dataSetName dataset name
     * @param colNames columns to be inserted with data
     * @param values datas to be inserted
     * @return int number of data removed, -1 means failed
     */
    public boolean addData(String dataSetName, ArrayList colNames, ArrayList values) {
        if (!requireChannel())
            return false;
        writeBytes(CommUtility.intToByteArray(Consts.ADDDATA)); // send cmd type
        writeObject(dataSetName);   // send parameters
        writeObject(colNames);      // send parameters
        writeObject(values);        // send parameters
        byte[] rest = readBytes();
        releaseChannel();
        return rest!=null&&rest[0]==0x01;
    }
    
    /**
     * modify data in dataset, filtered by conditions
     * @param dataSetName dataset name
     * @param colNames columns to be moidfied
     * @param values new datas
     * @param prediction criterias of searching
     * @param asynchronized running mode
     * @return int number of data removed, -1 means failed
     */
    public int modifyData(String dataSetName, ArrayList colNames, ArrayList values, Prediction prediction, boolean asynchronized) {
        if (asynStatus != Consts.CLEAR && asynStatus != Consts.RUNNING) { 
            dtrace.trace(5006);
            return -1;
        }
        if (asynchronized){
            asynStatus = Consts.PREINITIALIZING; // prepare initialize asyn call
            asynCallParameters.clear();
            asynCallParameters.add(dataSetName);
            asynCallParameters.add(colNames);
            asynCallParameters.add(values);
            asynCallParameters.add(prediction);
            asynStatus = Consts.INITIALIZING;   // prepared initialize asyn call
            asynLastCallCmd = Consts.MODIFYDATA;
            this.start();
            return 0;
        }else{
            if (!requireChannel())
                return -1;
            writeBytes(CommUtility.intToByteArray(Consts.MODIFYDATA)); // send cmd type
            writeObject(dataSetName);                    // send parameters
            writeObject(colNames);                       // send parameters
            writeObject(values);                         // send parameters
            writeObject(prediction);                     // send parameters
            int removedNum = CommUtility.byteArrayToInt(readBytes());
            releaseChannel();
            return removedNum;
        }
    }

    /**
     * join data from 2 filtered dataSet
     * @param dataSetName1 dataset1 name
     * @param dataSetName2 dataset2 name
     * @param selectColumns1 columns to be selected from dataset1
     * @param selectColumns2 columns to be selected from dataset2
     * @param prediction1 criteria of searching dataset1
     * @param prediction2 criteria of searching dataset2
     * @param joinPred join criteria
     * @param joinMode join mode
     * @param asynchronized running mode
     * @return ArrayList list of values (FyDataEntry)
     */
    public ArrayList searchJoinData(String dataSetName1, 
                                    String dataSetName2, 
                                    HashSet selectColumns1, 
                                    HashSet selectColumns2, 
                                    Prediction prediction1, 
                                    Prediction prediction2, 
                                    Prediction joinPred,
                                    int joinMode,
                                    boolean asynchronized){
        if (asynStatus != Consts.CLEAR && asynStatus != Consts.RUNNING) { 
            dtrace.trace(5006);
            return null;
        }
        if (asynchronized){
            asynStatus = Consts.PREINITIALIZING; // prepare initialize asyn call
            asynCallParameters.clear();
            asynCallParameters.add(dataSetName1);
            asynCallParameters.add(dataSetName2);
            asynCallParameters.add(selectColumns1);
            asynCallParameters.add(selectColumns2);
            asynCallParameters.add(prediction1);
            asynCallParameters.add(prediction2);
            asynCallParameters.add(joinPred);
            asynCallParameters.add(Integer.valueOf(joinMode));
            asynStatus = Consts.INITIALIZING;   // prepared initialize asyn call
            asynLastCallCmd = Consts.SEARCHJOINDATA;
            this.start();
            return null;
        }else{
            if (!requireChannel())
                return null;
            writeBytes(CommUtility.intToByteArray(Consts.SEARCHJOINDATA)); // send cmd type
            writeObject(dataSetName1);                    // send parameters
            writeObject(dataSetName2);                    // send parameters
            writeObject(selectColumns1);                  // send parameters
            writeObject(selectColumns2);                  // send parameters
            writeObject(prediction1);                     // send parameters
            writeObject(prediction2);                     // send parameters
            writeObject(joinPred);                        // send parameters
            writeBytes(CommUtility.intToByteArray(joinMode));
            ArrayList dsList = (ArrayList)readObject();
            releaseChannel();
            return dsList;
        }
    }

    /**
     * get last error message
     * @return the last error message
     */
    public String getLastError() {
        if (!requireChannel())
            return null;
        writeBytes(CommUtility.intToByteArray(Consts.GETLASTERROR)); // send cmd type
        String msg = (String)readObject();
        releaseChannel();
        return msg;
    }
    
    /**
     * get version
     * @return version
     */
    public String getVersion() {
        if (!requireChannel())
            return null;
        writeBytes(CommUtility.intToByteArray(Consts.GETVERSION)); // send cmd type
        String version = (String)readObject();
        releaseChannel();
        return version;
    }
    
    /**
     * promote to be a super man
     * @return version
     */
    public boolean promote(String password) {
        if (!requireChannel())
            return false;
        writeBytes(CommUtility.intToByteArray(Consts.PROMOTE)); // send cmd type
        writeObject(password);
        byte[] rest = readBytes();
        releaseChannel();
        return rest!=null&&rest[0]==0x01;
    }

    /**
     * startup db server
     * @return success or failed
     */
    public boolean startup() {
        if (!requireChannel())
            return false;
        writeBytes(CommUtility.intToByteArray(Consts.STARTUP)); // send cmd type
        byte[] rest = readBytes();
        releaseChannel();
        return rest!=null&&rest[0]==0x01;
    }
    
    /**
     * shutdown db server
     * @return success or failed
     */
    public boolean shutdown() {
        if (!requireChannel())
            return false;
        writeBytes(CommUtility.intToByteArray(Consts.SHUTDOWN)); // send cmd type
        byte[] rest = readBytes();
        releaseChannel();
        return rest!=null&&rest[0]==0x01;
    }

    /**
     * get dataset size
     * @return size. -1 means error
     */
    public long getDatasetSize(String dataSetName) {
        if (!requireChannel())
            return -1;
        writeBytes(CommUtility.intToByteArray(Consts.GETDATASETSIZE)); // send cmd type
        writeObject(dataSetName);
        byte[] rest = readBytes();
        releaseChannel();
        return CommUtility.byteArrayToLong(rest);
    }
    
    /**
     * get db size
     * @return size. -1 means error
     */
    public long getDBSize() {
        if (!requireChannel())
            return -1;
        writeBytes(CommUtility.intToByteArray(Consts.GETDBSIZE)); // send cmd type
        byte[] rest = readBytes();
        releaseChannel();
        return CommUtility.byteArrayToLong(rest);
    }
    
    /**
     * release Dataset
     * @return success or failed
     */
    public boolean releaseDataset(String dataSetName) {
        if (!requireChannel())
            return false;
        writeBytes(CommUtility.intToByteArray(Consts.RELEASEDATASET)); // send cmd type
        writeObject(dataSetName);
        byte[] rest = readBytes();
        releaseChannel();
        return rest!=null&&rest[0]==0x01;
    }

    /**
     * reload Dataset
     * @return success or failed
     */
    public boolean reloadDataset(String dataSetName) {
        if (!requireChannel())
            return false;
        writeBytes(CommUtility.intToByteArray(Consts.RELOADDATASET)); // send cmd type
        writeObject(dataSetName);
        byte[] rest = readBytes();
        releaseChannel();
        return rest!=null&&rest[0]==0x01;
    }

    /**
     * copy dataset to local file
     * @return success or failed
     */
    public boolean copyDatasetToFile(String dataSetName, String fileName) {
        if (!requireChannel())
            return false;
        writeBytes(CommUtility.intToByteArray(Consts.COPYDATASETTOFILE)); // send cmd type
        writeObject(dataSetName);
        writeObject(fileName);
        byte[] rest = readBytes();
        releaseChannel();
        return rest!=null&&rest[0]==0x01;
    }

    /**
     * Get Last asynchronized call state
     * @return int
     * */
    public int getLastAsynState(){
        return asynStatus;
    }

    /**
     * Get asynchronized call result. 
     * 
     * @return Object. if calling is not completed, it will return null
     * */
    public Object getAsynchronizedResult(){
        switch (asynStatus){
        case Consts.CLEAR:
            dtrace.trace(5005);
            return null;
        case Consts.RUNNING:
            dtrace.trace(5006);
            return null;
        case Consts.PREINITIALIZING:
        case Consts.INITIALIZING:
        case Consts.INITIALIZED:
            dtrace.trace(5007);
            return null;
        case Consts.FAILED:
            dtrace.trace(5008);
            asynCallParameters.clear(); // clear asynchronized call parameters
            asynStatus = Consts.CLEAR; // reset status
            return null;
        case Consts.COMPLETED:
            asynCallParameters.clear(); // clear asynchronized call parameters
            asynStatus = Consts.CLEAR; // reset status
            return asynResult;
        }
        return null;
    }

    /**
     * start asynchronized thread
     * 
     * */
    public void start() {
        //if (running && super.isAlive()){
        //    dtrace.trace(5004);
        //    return;
        //}
        if (asynStatus != Consts.INITIALIZING){
            dtrace.trace(5002);
            return;
        }
        running = true;
        if (!super.isAlive())
            super.start();
    }

    public void run() {
        //if (running && super.isAlive()){
        //    dtrace.trace(5004);
        //    return;
        //}
        //if (asynStatus != Consts.INITIALIZING){
        //    dtrace.trace(5002);
        //    return;
        //}
        do {
            if (isClosed()){
                isClosed = true;
                terminate();
            }
            try {
                // asynchronized call
                switch (asynStatus){
                case Consts.INITIALIZING:
                    asynStatus = Consts.INITIALIZED; // change status to initialized, run the asyn call in next round
                    break;
                case Consts.INITIALIZED:
                    asynStatus = Consts.RUNNING; // change status to running, run the asyn call
                    switch (asynLastCallCmd){
                        case Consts.BATCHDELETEDATA:{
                            if (asynCallParameters == null || asynCallParameters.size() != 2){
                                dtrace.trace(5009);
                                asynStatus = Consts.FAILED;
                                break;
                            }
                            String dataSetName = (String)asynCallParameters.get(0);
                            HashSet keys = (HashSet)asynCallParameters.get(1);
                            int callResult = batchDeleteData(dataSetName, keys, false);
                            asynCallParameters.clear();
                            asynResult = new Integer(callResult);
                            asynStatus = Consts.COMPLETED;
                        }
                        break;
                        case Consts.BATCHINSERTDATA:{
                            if (asynCallParameters == null || asynCallParameters.size() != 3){
                                dtrace.trace(5009);
                                asynStatus = Consts.FAILED;
                                break;
                            }
                            String dataSetName = (String)asynCallParameters.get(0);
                            ArrayList colNames = (ArrayList)asynCallParameters.get(1);
                            ArrayList datas = (ArrayList)asynCallParameters.get(2);
                            int callResult = batchInsertData(dataSetName, colNames, datas, false);
                            asynCallParameters.clear();
                            asynResult = new Integer(callResult);
                            asynStatus = Consts.COMPLETED;
                        }
                        break;
                        case Consts.BATCHMODIFYMEMDATABYKEY:{
                            if (asynCallParameters == null || asynCallParameters.size() != 3){
                                dtrace.trace(5009);
                                asynStatus = Consts.FAILED;
                                break;
                            }
                            String dataSetName = (String)asynCallParameters.get(0);
                            HashSet keys = (HashSet)asynCallParameters.get(1);
                            ArrayList colNames = (ArrayList)asynCallParameters.get(2);
                            ArrayList newValues = (ArrayList)asynCallParameters.get(3);
                            int callResult = batchModifyMemDataByKey(dataSetName, keys, colNames, newValues, false);
                            asynCallParameters.clear();
                            asynResult = new Integer(callResult);
                            asynStatus = Consts.COMPLETED;
                        }
                        break;
                        case Consts.SEARCHDATA:{
                            if (asynCallParameters == null || asynCallParameters.size() != 2){
                                dtrace.trace(5009);
                                asynStatus = Consts.FAILED;
                                break;
                            }
                            String dataSetName = (String)asynCallParameters.get(0);
                            Prediction filter = (Prediction)asynCallParameters.get(1);
                            asynResult = searchData(dataSetName, filter, false);
                            asynCallParameters.clear();
                            asynStatus = Consts.COMPLETED;
                        }
                        break;
                        case Consts.REMOVEDATA:{
                            if (asynCallParameters == null || asynCallParameters.size() != 2){
                                dtrace.trace(5009);
                                asynStatus = Consts.FAILED;
                                break;
                            }
                            String dataSetName = (String)asynCallParameters.get(0);
                            Prediction filter = (Prediction)asynCallParameters.get(1);
                            asynResult = removeData(dataSetName, filter, false);
                            asynCallParameters.clear();
                            asynStatus = Consts.COMPLETED;
                        }
                        break;
                        case Consts.MODIFYDATA:{
                            if (asynCallParameters == null || asynCallParameters.size() != 4){
                                dtrace.trace(5009);
                                asynStatus = Consts.FAILED;
                                break;
                            }
                            String dataSetName = (String)asynCallParameters.get(0);
                            ArrayList colNames = (ArrayList)asynCallParameters.get(1);
                            ArrayList values = (ArrayList)asynCallParameters.get(2);
                            Prediction filter = (Prediction)asynCallParameters.get(3);
                            asynResult = modifyData(dataSetName, colNames, values, filter, false);
                            asynCallParameters.clear();
                            asynStatus = Consts.COMPLETED;
                        }
                        break;
                        case Consts.SEARCHJOINDATA:{
                            if (asynCallParameters == null || asynCallParameters.size() != 2){
                                dtrace.trace(5009);
                                asynStatus = Consts.FAILED;
                                break;
                            }
                            String dataSetName1 = (String)asynCallParameters.get(0);
                            String dataSetName2 = (String)asynCallParameters.get(1);
                            HashSet selectColumns1 = (HashSet)asynCallParameters.get(2);
                            HashSet selectColumns2 = (HashSet)asynCallParameters.get(3);
                            Prediction prediction1 = (Prediction)asynCallParameters.get(6);
                            Prediction prediction2 = (Prediction)asynCallParameters.get(7);
                            Prediction joinPred = (Prediction)asynCallParameters.get(8);
                            int joinMode = (Integer)asynCallParameters.get(9);
                            asynResult = searchJoinData(dataSetName1,dataSetName2,selectColumns1,selectColumns2,prediction1,prediction2,joinPred,joinMode,false);
                            asynCallParameters.clear();
                            asynStatus = Consts.COMPLETED;
                        }
                        break;
                        default:
                            dtrace.trace(5011);
                            asynStatus = Consts.FAILED;
                            asynCallParameters.clear();
                            break;
                    }
                    break;
                }
                try {
                    sleeping = true;
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    dtrace.trace(36);
                } finally {
                    sleeping = false;
                }
            } catch (Exception e) {
                dtrace.trace(10);
                if (debuger.isDebugMode())
                    e.printStackTrace();
            } finally {
                sleeping = false;
            }
        } while (running && asynStatus == Consts.INITIALIZED);
        sleeping = false;
    }
}
