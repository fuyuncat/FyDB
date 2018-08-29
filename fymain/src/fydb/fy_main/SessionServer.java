/**
 * @(#)SessionServer.java	0.01 11/05/17
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_main;

import fydb.fy_comm.CommUtility;
import fydb.fy_comm.Debuger;
import fydb.fy_comm.FyMetaData;
import fydb.fy_comm.InitParas;
import fydb.fy_comm.NetThread;
import fydb.fy_comm.Prediction;
import fydb.fy_comm.SessionBase;
import fydb.fy_comm.Tracer;
import fydb.fy_comm.Consts;

import fydb.fy_data.FyDataSet;
import fydb.fy_data.Optimizer;
import fydb.fy_data.Optimizer.JoinMethod;
import fydb.fy_data.BP;
import fydb.fy_data.Beeper;

import java.net.Socket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

public class SessionServer extends NetThread  implements SessionBase{
    protected InitParas paras;
    protected Tracer dtrace;
    protected Debuger debuger;
    protected Beeper beeper;
    private Map dataSets;
    private Manager db;
    private boolean isSuperMan = false;

    private int asynStatus = Consts.CLEAR; // status of asynchronized calling 
    private int asynLastCallCmd;  // the last asynchronized call command

    private boolean isLocal = true;   // default is local
    private ArrayList asynCallParameters;  // parameters of synchronized call
    private Object asynResult;   // store the result of asynchronized calling
    private int sessionID;
    private boolean isClosed = false;

    // local session server
    public SessionServer(int sessionID, Map dataSets, InitParas paras,
                         Tracer dtrace, Debuger debuger, Beeper beeper, Manager db) {
        super(paras, dtrace, debuger);

        this.dataSets = dataSets;
        this.paras = paras;
        this.dtrace = dtrace;
        this.debuger = debuger;
        this.beeper = beeper;
        this.sessionID = sessionID;
        this.db = db;

        asynCallParameters = new ArrayList();

        setName("SessionServer");
    }

    // remote session server
    public SessionServer(int sessionID, Map dataSets, InitParas paras,
                         Tracer dtrace, Debuger debuger, Beeper beeper,
                         Socket serverSocket, Manager db) {
        this(sessionID, dataSets, paras, dtrace, debuger, beeper, db);
        this.isLocal = false;
        super.setServerSocket(serverSocket);
    }

    /**
     * release session resource
     * */
     public void release() {
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
     * get meta rightExpression of dataset
     * 
     * @return FyMetaData
     */
    public FyMetaData getMetaData(String dataSetName){
        FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
        if (dataSet == null) {
            dtrace.trace(206);
            return null;
        }
        return dataSet.getMetaData();
    }

    /**
     * build a key with specified column names and values
     * @param dataSetName dataset name
     * @param colNames column names
     * @param values column values. position in the array should be kept aligned with column names
     * */
    public HashMap buildKeyWithName(String dataSetName,
                                    ArrayList<String> colNames,
                                    ArrayList<String> values) {
        return (getMetaData(dataSetName)).buildKeyWithColName(colNames, values);
    }

    /**
     * read single value from specified dataset
     * @param dataSetName dataset name
     * @param key key of rightExpression to be read
     * @return HashMap values (colId1:value1;colId2:value2 ...)
     */
    public HashMap singleRead(String dataSetName, HashMap key) {
        if (key == null) {
            dtrace.trace(207);
            return null;
        }

        FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
        if (dataSet == null) {
            dtrace.trace(206);
            return null;
        }
        while (dataSet.requireAccess() ==
               false)// get access, can not excceed max accesser limitation
        {
            try {
                Thread.sleep((Integer)paras.getParameter("_spinTime"));
            } catch (InterruptedException e) {
                dtrace.trace(e.getMessage());
                dtrace.trace(101);
                return null;
            }
        }
        HashMap value = dataSet.getMemDataByKey(key);
        dataSet.releaseAccess();
        return value;
    }
    
    public boolean modifyDataByKeyI(String dataSetName, HashMap key,
                                    ArrayList<Integer> colIDs,
                                    ArrayList newValues) {
        if (key == null) {
            dtrace.trace(207);
            return false;
        }

        if (colIDs == null) {
            dtrace.trace(214);
            return false;
        }

        if (newValues == null) {
            dtrace.trace(215);
            return false;
        }

        if (colIDs.size() != newValues.size()) {
            dtrace.trace(213);
            return false;
        }

        FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
        if (dataSet == null) {
            dtrace.trace(206);
            return false;
        }
        while (dataSet.requireAccess() == false) {
            // get access, can not excceed max accesser limitation
            try {
                Thread.sleep((Integer)paras.getParameter("_spinTime"));
            } catch (InterruptedException e) {
                dtrace.trace(e.getMessage());
                dtrace.trace(101);
                return false;
            }
        }
        boolean modified = dataSet.modifyMemDataByKeyI(key, colIDs, newValues);
        dataSet.releaseAccess();
        return modified;
    }

    public boolean modifyDataByKeyS(String dataSetName, HashMap key,
                                    ArrayList<String> colNames,
                                    ArrayList newValues) {
        if (key == null) {
            dtrace.trace(207);
            return false;
        }

        if (colNames == null) {
            dtrace.trace(214);
            return false;
        }

        if (newValues == null) {
            dtrace.trace(215);
            return false;
        }

        if (colNames.size() != newValues.size()) {
            dtrace.trace(213);
            return false;
        }

        FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
        if (dataSet == null) {
            dtrace.trace(206);
            return false;
        }
        while (dataSet.requireAccess() == false) {
            // get access, can not excceed max accesser limitation
            try {
                Thread.sleep((Integer)paras.getParameter("_spinTime"));
            } catch (InterruptedException e) {
                dtrace.trace(e.getMessage());
                dtrace.trace(101);
                return false;
            }
        }
        boolean modified =
            dataSet.modifyMemDataByKeyS(key, colNames, newValues);
        dataSet.releaseAccess();
        return modified;
    }

    public boolean modifyDataByKeyM(String dataSetName, HashMap key,
                                    HashMap mappedDatas) {
        if (mappedDatas == null) {
            dtrace.trace(207);
            return false;
        }

        ArrayList colIDs = new ArrayList();
        ArrayList newValues = new ArrayList();
        Iterator it = mappedDatas.keySet().iterator();
        while (it.hasNext()) {
            Integer colID = (Integer)it.next();
            String newValue = (String)mappedDatas.get(colID);
            colIDs.add(colID);
            newValues.add(newValue);
        }

        return modifyDataByKeyI(dataSetName, key, colIDs, newValues);
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
            if (isLocal) // in local mode, we should awake the thread
                this.start();
            return 0;
        }else{
            int modNum = 0;
            if (keys == null || keys.size() == 0) {
                dtrace.trace(229);
                return modNum;
            }
    
            if (colNames == null) {
                dtrace.trace(214);
                return modNum;
            }
    
            if (newValues == null) {
                dtrace.trace(215);
                return modNum;
            }
    
            if (colNames.size() != newValues.size()) {
                dtrace.trace(213);
                return modNum;
            }
            FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
            if (dataSet == null) {
                dtrace.trace(206);
                return modNum;
            }
            while (dataSet.requireAccess() == false) {
                // get access, can not excceed max accesser limitation
                try {
                    Thread.sleep((Integer)paras.getParameter("_spinTime"));
                } catch (InterruptedException e) {
                    dtrace.trace(e.getMessage());
                    dtrace.trace(101);
                    return modNum;
                }
            }
            modNum = dataSet.batchModifyMemDataByKeyS(keys, colNames, newValues, true);
            dataSet.releaseAccess();
            return modNum;
        }
    }

    public boolean insertDataI(String dataSetName, ArrayList<Integer> colIDs,
                               ArrayList newValues) {
        if (colIDs == null) {
            dtrace.trace(239);
            return false;
        }

        if (newValues == null) {
            dtrace.trace(240);
            return false;
        }

        if (colIDs.size() != newValues.size()) {
            dtrace.trace(247);
            return false;
        }

        FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
        if (dataSet == null) {
            dtrace.trace(206);
            return false;
        }
        while (dataSet.requireAccess() == false) {
            // get access, can not excceed max accesser limitation
            try {
                Thread.sleep((Integer)paras.getParameter("_spinTime"));
            } catch (InterruptedException e) {
                dtrace.trace(e.getMessage());
                dtrace.trace(101);
                return false;
            }
        }
        boolean inserted = dataSet.insertDataI(colIDs, newValues);
        dataSet.releaseAccess();
        return inserted;
    }

    public boolean insertDataS(String dataSetName, ArrayList<String> colNames,
                               ArrayList newValues) {
        if (colNames == null) {
            dtrace.trace(239);
            return false;
        }

        if (newValues == null) {
            dtrace.trace(240);
            return false;
        }

        if (colNames.size() != newValues.size()) {
            dtrace.trace(247);
            return false;
        }

        FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
        if (dataSet == null) {
            dtrace.trace(206);
            return false;
        }
        while (dataSet.requireAccess() == false) {
            // get access, can not excceed max accesser limitation
            try {
                Thread.sleep((Integer)paras.getParameter("_spinTime"));
            } catch (InterruptedException e) {
                dtrace.trace(e.getMessage());
                dtrace.trace(101);
                return false;
            }
        }
        boolean inserted = dataSet.insertDataS(colNames, newValues);
        dataSet.releaseAccess();
        return inserted;
    }

    public boolean insertDataM(String dataSetName, HashMap mappedDatas) {
        if (mappedDatas == null) {
            dtrace.trace(239);
            return false;
        }

        ArrayList colIDs = new ArrayList();
        ArrayList newValues = new ArrayList();
        Iterator it = mappedDatas.keySet().iterator();
        while (it.hasNext()) {
            Integer colID = (Integer)it.next();
            String newValue = (String)mappedDatas.get(colID);
            colIDs.add(colID);
            newValues.add(newValue);
        }

        return insertDataI(dataSetName, colIDs, newValues);
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
            if (isLocal) // in local mode, we should awake the thread
                this.start();
            return 0;
        }else{
            int insNum = 0;
            if (colNames == null) {
                dtrace.trace(239);
                return insNum;
            }
    
            if (datas == null) {
                dtrace.trace(245);
                return insNum;
            }
    
            FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
            if (dataSet == null) {
                dtrace.trace(206);
                return insNum;
            }
            while (dataSet.requireAccess() == false) {
                // get access, can not excceed max accesser limitation
                try {
                    Thread.sleep((Integer)paras.getParameter("_spinTime"));
                } catch (InterruptedException e) {
                    dtrace.trace(e.getMessage());
                    dtrace.trace(101);
                    return insNum;
                }
            }
            insNum = dataSet.batchInsertDataS(colNames, datas, true);
            dataSet.releaseAccess();
            return insNum;
        }
    }

    // delete a data entry, by key
    public boolean deleteDataByKey(String dataSetName, HashMap key) {
        if (key == null) {
            dtrace.trace(233);
            return false;
        }

        FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
        if (dataSet == null) {
            dtrace.trace(206);
            return false;
        }
        while (dataSet.requireAccess() == false) {
            // get access, can not excceed max accesser limitation
            try {
                Thread.sleep((Integer)paras.getParameter("_spinTime"));
            } catch (InterruptedException e) {
                dtrace.trace(e.getMessage());
                dtrace.trace(101);
                return false;
            }
        }
        boolean deleted = dataSet.deleteDataByKey(key);
        dataSet.releaseAccess();
        return deleted;
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
            if (isLocal) // in local mode, we should awake the thread
                this.start();
            return 0;
        }else{
            int delNum = 0;
            if (keys == null) {
                dtrace.trace(233);
                return delNum;
            }
    
            FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
            if (dataSet == null) {
                dtrace.trace(206);
                return delNum;
            }
            while (dataSet.requireAccess() == false) {
                // get access, can not excceed max accesser limitation
                try {
                    Thread.sleep((Integer)paras.getParameter("_spinTime"));
                } catch (InterruptedException e) {
                    dtrace.trace(e.getMessage());
                    dtrace.trace(101);
                    return delNum;
                }
            }
            delNum = dataSet.batchDeleteData(keys);
            dataSet.releaseAccess();
            return delNum;
        }
    }

    /**
     * Detect if session closed.
     * */
    public boolean isClosed(){
        return super.isClosed();
        //return isClosed;
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
            if (isLocal) // in local mode, we should awake the thread
                this.start();
            return null;
        }else{
            ArrayList datas = null;
            FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
            if (dataSet == null) {
                debuger.printMsg(dataSetName,false);
                debuger.printMsg(dataSets.toString(),false);
                dtrace.trace(206);
                return null;
            }
            while (dataSet.requireAccess() == false) {
                // get access, can not excceed max accesser limitation
                try {
                    Thread.sleep((Integer)paras.getParameter("_spinTime"));
                } catch (InterruptedException e) {
                    dtrace.trace(e.getMessage());
                    dtrace.trace(101);
                    return null;
                }
            }
            datas = dataSet.searchData(prediction);
            dataSet.releaseAccess();
            return datas;
        }
    }
    
    /**
     * remove data from dataset, filtered by conditions
     * @param dataSetName dataset name
     * @param prediction criterias of searching
     * @param asynchronized running mode
     * @return int number of data removed
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
            if (isLocal) // in local mode, we should awake the thread
                this.start();
            return 0;
        }else{
            int removedNum = 0;
            FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
            if (dataSet == null) {
                dtrace.trace(206);
                return -1;
            }
            while (dataSet.requireAccess() == false) {
                // get access, can not excceed max accesser limitation
                try {
                    Thread.sleep((Integer)paras.getParameter("_spinTime"));
                } catch (InterruptedException e) {
                    dtrace.trace(e.getMessage());
                    dtrace.trace(101);
                    return -1;
                }
            }
            removedNum = dataSet.removeData(prediction);
            dataSet.releaseAccess();
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
        if (colNames == null || values == null) {
            dtrace.trace(233);
            return false;
        }

        FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
        if (dataSet == null) {
            dtrace.trace(206);
            return false;
        }
        while (dataSet.requireAccess() == false) {
            // get access, can not excceed max accesser limitation
            try {
                Thread.sleep((Integer)paras.getParameter("_spinTime"));
            } catch (InterruptedException e) {
                dtrace.trace(e.getMessage());
                dtrace.trace(101);
                return false;
            }
        }
        boolean inserted = dataSet.addData(colNames, values);
        dataSet.releaseAccess();
        return inserted;
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
            asynCallParameters.add(prediction);
            asynStatus = Consts.INITIALIZING;   // prepared initialize asyn call
            asynLastCallCmd = Consts.REMOVEDATA;
            if (isLocal) // in local mode, we should awake the thread
                this.start();
            return 0;
        }else{
            int modifiedNum = 0;
            FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
            if (dataSet == null) {
                dtrace.trace(206);
                return -1;
            }
            while (dataSet.requireAccess() == false) {
                // get access, can not excceed max accesser limitation
                try {
                    Thread.sleep((Integer)paras.getParameter("_spinTime"));
                } catch (InterruptedException e) {
                    dtrace.trace(e.getMessage());
                    dtrace.trace(101);
                    return -1;
                }
            }
            modifiedNum = dataSet.modifyData(colNames, values, prediction);
            dataSet.releaseAccess();
            return modifiedNum;
        }
    }

    // detect if data of key&vale match all filters
    private boolean matchJoiner(HashMap leftValues, HashMap rightValues, Prediction joinPred, FyMetaData metaData){
        boolean matched = false; 
        if (joinPred.type == Consts.BRANCH){
            if (joinPred.leftNode == null || joinPred.rightNode == null){
               dtrace.trace(119);
               return false;
            }
            if (joinPred.junction == Consts.AND)  // and
               matched = matchJoiner(leftValues, rightValues, joinPred.leftNode, metaData) && matchJoiner(leftValues, rightValues, joinPred.rightNode, metaData);
            else // or
               matched = matchJoiner(leftValues, rightValues, joinPred.leftNode, metaData) || matchJoiner(leftValues, rightValues, joinPred.rightNode, metaData);
        }else if (joinPred.type == Consts.LEAF){
            if (joinPred.leftColId < 0) // filter is expression
                return joinPred.leftExpression==null?joinPred.rightExpression==null:joinPred.leftExpression.equals(joinPred.rightExpression);
            if (leftValues == null || rightValues == null)
                return false;
            String leftData = (String)leftValues.get(Integer.valueOf(joinPred.leftColId));
            String rightData = (String)rightValues.get(Integer.valueOf(joinPred.rightColId));
            if (leftData == null && rightData == null)
                return true;
            else if (leftData == null || rightData == null)
                return false;
            return CommUtility.anyDataCompare(leftData, joinPred.comparator, rightData, metaData.getColumnType(Integer.valueOf(joinPred.leftColId))) == 1;
        }else{ // no predication means alway true
            return true;
        }
        return matched;
    }

    // loop join data results
    private ArrayList joinDataLoop(ArrayList outter, ArrayList inner, Prediction joinPred, FyMetaData metaData, int joinMode, boolean rotate){
        ArrayList joinResult = new ArrayList();
        boolean[] innerMatched = joinMode==Consts.OUTERJOIN?new boolean[inner.size()]:null;
        // to decide if the inner datas should be returned for outer join
        if (joinMode==Consts.OUTERJOIN)
            for (int j=0;j<innerMatched.length;j++)
                innerMatched[j] = false;
        for (int i=0;i<outter.size();i++){
            HashMap outDatas = (HashMap)outter.get(i);
            boolean outerMatched = false;
            for (int j=0;j<inner.size();j++){
                HashMap inDatas = (HashMap)inner.get(j);
                // for outer joint, not predicate the join conditions
                if (matchJoiner(outDatas,inDatas,joinPred,metaData)){
                    HashMap[] joinEntries = rotate?new HashMap[]{inDatas,outDatas}:new HashMap[]{outDatas,inDatas};
                    joinResult.add(joinEntries);
                    outerMatched = true;
                    if (joinMode==Consts.OUTERJOIN)
                        innerMatched[j] = true;
                }
            }
            if (!outerMatched && joinMode!=Consts.INNERJOIN){
                HashMap[] joinEntries = rotate?new HashMap[]{null,outDatas}:new HashMap[]{outDatas,null};
                joinResult.add(joinEntries);
            }
        }
        if (joinMode==Consts.OUTERJOIN)
            for (int j=0;j<inner.size();j++)
                if (!innerMatched[j]){
                    HashMap inDatas = (HashMap)inner.get(j);
                    HashMap[] joinEntries = rotate?new HashMap[]{inDatas,null}:new HashMap[]{null,inDatas};
                    joinResult.add(joinEntries);
                }
        return joinResult;
    }

    // join 2 data result on join predication
    private ArrayList joinData(ArrayList d1, ArrayList d2, Prediction joinPred, FyMetaData metaData, int joinMode){
        Optimizer optimizer = new Optimizer(dtrace);
        JoinMethod joinMethod = optimizer.newJoinMethod();
        optimizer.optimizeJoin(joinMethod,d1,d2,joinMode);
        switch (joinMethod.method){
            case Consts.LOOPJOIN: {
                if (joinMethod.out == 1)
                    return joinDataLoop(d1,d2,joinPred,metaData,joinMode,false);
                else
                    return joinDataLoop(d2,d1,joinPred,metaData,joinMode,true);
            }
            break;
            case Consts.HASHJOIN:
            case Consts.SORTJOIN:
            break;
        }
        return null;
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
            asynLastCallCmd = Consts.SEARCHDATA;
            if (isLocal) // in local mode, we should awake the thread
                this.start();
            return null;
        }else{
            FyDataSet dataSet1 = (FyDataSet)dataSets.get(dataSetName1.toUpperCase());
            FyDataSet dataSet2 = (FyDataSet)dataSets.get(dataSetName2.toUpperCase());
            if (dataSet1 == null || dataSet2 == null)
                return null;
            else {
                if (!joinPred.columnsAnalyzed() && !joinPred.analyzeColumns(dataSet1.getMetaData(),dataSet2.getMetaData()))
                    return null;
                BP bp=beeper.getBP();
                selectColumns1 = selectColumns1==null?new HashSet(dataSet1.getMetaData().getColumns().keySet()):selectColumns1;
                selectColumns2 = selectColumns2==null?new HashSet(dataSet2.getMetaData().getColumns().keySet()):selectColumns2;
                HashSet requriedColumns1 = new HashSet(selectColumns1);
                HashSet requriedColumns2 = new HashSet(selectColumns2);
                requriedColumns1.addAll(joinPred.getAllColIDs(Consts.LEAF));
                requriedColumns2.addAll(joinPred.getAllColIDs(Consts.RIGHT));
                ArrayList d1 = dataSet1.searchData(bp,prediction1,requriedColumns1);
                ArrayList d2 = dataSet2.searchData(bp,prediction2,requriedColumns2);
                return joinData(d1,d2,joinPred,dataSet2.getMetaData(),joinMode);
            }
        }
    }

    /**
     * get last error message
     * @return the last error message
     */
    public String getLastError() {
        String msg = dtrace.getLastErrorMsg();
        return msg;
    }
    
    /**
     * get version
     * @return version
     */
    public String getVersion() {
        String msg = db.getVersion();
        return msg;
    }
    
    /**
     * promote to be a super man
     * @return success or failed
     */
    public boolean promote(String password) {
        return db.promote(password);
    }
    
    /**
     * startup db server
     * @return success or failed
     */
    public boolean startup() {
        if (!isSuperMan){
            dtrace.trace(130);
            return false;
        }else
            return db.launch();
    }
    
    /**
     * shutdown db server
     * @return success or failed
     */
    public boolean shutdown() {
        if (!isSuperMan){
            dtrace.trace(130);
            return false;
        }else
            return db.shutdown();
    }

    /**
     * get dataset/db size
     * @return size. -1 means error
     */
    public long getSize(String dataSetName) {
        if (!isSuperMan){
            dtrace.trace(130);
            return -1;
        }else
            return db.calculateSize(dataSetName);
    }
    
    /**
     * release Dataset
     * @return success or failed
     */
    public boolean releaseDataset(String dataSetName) {
        if (!isSuperMan){
            dtrace.trace(130);
            return false;
        }else
            return db.releaseDataset(dataSetName);
    }

    /**
     * reload Dataset
     * @return success or failed
     */
    public boolean reloadDataset(String dataSetName) {
        if (!isSuperMan){
            dtrace.trace(130);
            return false;
        }else
            return db.reloadDataset(dataSetName);
    }

    /**
     * copy dataset to local file
     * @return success or failed
     */
    public boolean copyDatasetToFile(String dataSetName, String fileName) {
        //if (!isSuperMan){
        //    dtrace.trace(130);
        //    return false;
        //}else
        return db.copyDatasetToFile(dataSetName, fileName);
    }

    private void responseClient(){
        if (isClosed()){
            isClosed = true;
            terminate();
            return;
        }
        int msgType = CommUtility.byteArrayToInt(readBytes());
        if (msgType == 0)
            return;
        switch(msgType){
            case Consts.GETMETADATA: {// buildKeyWithName
                String dataSetName = (String)readObject();
                writeObject(getMetaData(dataSetName));
            }
            break;
            case Consts.SINGLEREAD: {// singleRead
                String dataSetName = (String)readObject();
                HashMap key = (HashMap)readObject();
                writeObject(singleRead(dataSetName,key));
            }
            break;
            case Consts.MODIFYDATABYKEI: {// modifyDataByKeyI
                String dataSetName = (String)readObject();
                HashMap key = (HashMap)readObject();
                ArrayList colIDs = (ArrayList)readObject();
                ArrayList newValues = (ArrayList)readObject();
                writeBytes(modifyDataByKeyI(dataSetName,key,colIDs,newValues)?new byte[]{(byte)0x01}:new byte[]{(byte)0xFF});
            }
            break;
            case Consts.MODIFYDATABYKES: {// modifyDataByKeyS
                String dataSetName = (String)readObject();
                HashMap key = (HashMap)readObject();
                ArrayList colNames = (ArrayList)readObject();
                ArrayList newValues = (ArrayList)readObject();
                writeBytes(modifyDataByKeyS(dataSetName,key,colNames,newValues)?new byte[]{(byte)0x01}:new byte[]{(byte)0xFF});
            }
            break;
            case Consts.MODIFYDATABYKEM: {// modifyDataByKeyM
                String dataSetName = (String)readObject();
                HashMap key = (HashMap)readObject();
                HashMap mappedDatas = (HashMap)readObject();
                writeBytes(modifyDataByKeyM(dataSetName,key,mappedDatas)?new byte[]{(byte)0x01}:new byte[]{(byte)0xFF});
            }
            break;
            case Consts.BATCHMODIFYMEMDATABYKEY: {// batchModifyMemDataByKey
                String dataSetName = (String)readObject();
                HashSet keys = (HashSet)readObject();
                ArrayList colNames = (ArrayList)readObject();
                ArrayList newValues = (ArrayList)readObject();
                writeObject(new Integer(batchModifyMemDataByKey(dataSetName,keys,colNames,newValues,false)));
            }
            break;
            case Consts.INSERTDATAI: {// insertDataI
                String dataSetName = (String)readObject();
                ArrayList colIDs = (ArrayList)readObject();
                ArrayList newValues = (ArrayList)readObject();
                writeBytes(insertDataI(dataSetName,colIDs,newValues)?new byte[]{(byte)0x01}:new byte[]{(byte)0xFF});
            }
            break;
            case Consts.INSERTDATAS: {// insertDataS
                String dataSetName = (String)readObject();
                ArrayList colNames = (ArrayList)readObject();
                ArrayList newValues = (ArrayList)readObject();
                writeBytes(insertDataS(dataSetName,colNames,newValues)?new byte[]{(byte)0x01}:new byte[]{(byte)0xFF});
            }
            break;
            case Consts.INSERTDATAM: {// insertDataM
                String dataSetName = (String)readObject();
                HashMap mappedDatas = (HashMap)readObject();
                writeBytes(insertDataM(dataSetName,mappedDatas)?new byte[]{(byte)0x01}:new byte[]{(byte)0xFF});
            }
            break;
            case Consts.BATCHINSERTDATA: {// batchInsertData
                String dataSetName = (String)readObject();
                ArrayList colNames = (ArrayList)readObject();
                ArrayList datas = (ArrayList)readObject();
                writeObject(new Integer(batchInsertData(dataSetName,colNames,datas,false)));
            }
            break;
            case Consts.DELETEDATABYKEY: {// deleteDataByKey
                String dataSetName = (String)readObject();
                HashMap key = (HashMap)readObject();
                writeBytes(deleteDataByKey(dataSetName,key)?new byte[]{(byte)0x01}:new byte[]{(byte)0xFF});
            }
            break;
            case Consts.BATCHDELETEDATA: {// batchDeleteData
                String dataSetName = (String)readObject();
                HashSet keys = (HashSet)readObject();
                writeObject(new Integer(batchDeleteData(dataSetName,keys,false)));
            }
            break;
            case Consts.GETDATASETLIST: {// getDataSetsList
                writeObject(getDataSetsList());
            }
            break;
            case Consts.SEARCHDATA: {// searchData
                String dataSetName = (String)readObject();
                Prediction prediction = (Prediction)readObject();
                writeObject(searchData(dataSetName,prediction,false));
            }
            break;
            case Consts.REMOVEDATA: {// removeData
                String dataSetName = (String)readObject();
                Prediction prediction = (Prediction)readObject();
                writeBytes(CommUtility.intToByteArray(removeData(dataSetName,prediction,false)));
            }
            break;
            case Consts.ADDDATA: {// addData
                String dataSetName = (String)readObject();
                ArrayList colNames = (ArrayList)readObject();
                ArrayList values = (ArrayList)readObject();
                writeBytes(addData(dataSetName,colNames,values)?new byte[]{(byte)0x01}:new byte[]{(byte)0xFF});
            }
            case Consts.MODIFYDATA: {// removeData
                String dataSetName = (String)readObject();
                ArrayList colNames = (ArrayList)readObject();
                ArrayList values = (ArrayList)readObject();
                Prediction prediction = (Prediction)readObject();
                writeBytes(CommUtility.intToByteArray(modifyData(dataSetName,colNames,values,prediction,false)));
            }
            break;
            case Consts.SEARCHJOINDATA:{
                String dataSetName1 = (String)readObject();
                String dataSetName2 = (String)readObject();
                HashSet selectColumns1 = (HashSet)readObject();
                HashSet selectColumns2 = (HashSet)readObject();
                Prediction prediction1 = (Prediction)readObject();
                Prediction prediction2 = (Prediction)readObject();
                Prediction joinPred = (Prediction)readObject();
                int joinMode = CommUtility.byteArrayToInt(readBytes());
                writeObject(searchJoinData(dataSetName1,dataSetName2,selectColumns1,selectColumns2,prediction1,prediction2,joinPred,joinMode,false));
            }
            break;
            case Consts.GETLASTERROR: {// get last error message
                writeObject(getLastError());
            }
            break;
            case Consts.GETVERSION: {// get version info
                writeObject(getVersion());
            }
            break;
            case Consts.PROMOTE: {// promote to be administrator
                String password = (String)readObject();
                isSuperMan = promote(password);
                writeBytes(isSuperMan?new byte[]{(byte)0x01}:new byte[]{(byte)0xFF});
            }
            break;
            case Consts.STARTUP: {// start up db server
                writeBytes(startup()?new byte[]{(byte)0x01}:new byte[]{(byte)0xFF});
            }
            break;
            case Consts.SHUTDOWN: {// shutdown db server
                writeBytes(shutdown()?new byte[]{(byte)0x01}:new byte[]{(byte)0xFF});
            }
            break;
            case Consts.GETDATASETSIZE: {// get dataset size
                String dataSetName = (String)readObject();
                writeBytes(CommUtility.longToByteArray(getSize(dataSetName)));
            }
            break;
            case Consts.GETDBSIZE: {// get db size
                writeBytes(CommUtility.longToByteArray(getSize(null)));
            }
            break;
            case Consts.RELEASEDATASET: {// release dataset
                String dataSetName = (String)readObject();
                writeBytes(releaseDataset(dataSetName)?new byte[]{(byte)0x01}:new byte[]{(byte)0xFF});
            }
            break;
            case Consts.RELOADDATASET: {// reload dataset
                String dataSetName = (String)readObject();
                writeBytes(reloadDataset(dataSetName)?new byte[]{(byte)0x01}:new byte[]{(byte)0xFF});
            }
            break;
            case Consts.COPYDATASETTOFILE: {// copy dataset to file
                String dataSetName = (String)readObject();
                String fileName = (String)readObject();
                writeBytes(copyDatasetToFile(dataSetName, fileName)?new byte[]{(byte)0x01}:new byte[]{(byte)0xFF});
            }
            break;
            case Consts.BYEBYE:{ // byebye
                super.closeSocket();
                terminate();
            }
            break;
            default:
                dtrace.trace(37);
                return;
        }
    }

    /**
     * get all datasets list
     * 
     * @return ArrayList
     * */
     public ArrayList getDataSetsList(){
        return new ArrayList(dataSets.keySet());
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
            //dtrace.trace(5006);
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
     * start thread
     * 
     * */
    public void start() {
        if (running && super.isAlive()){
            dtrace.trace(5004);
            return;
        }
        if (asynStatus != Consts.INITIALIZING && isLocal){
            dtrace.trace(5003);
            return;
        }
        running = true;
        super.start();
    }

    public void run() {
        //if (running && super.isAlive()){
        //    dtrace.trace(5004);
        //    return;
        //}
        //if (asynStatus != Consts.INITIALIZING && isLocal){ // only if in asynchronized mode calling or remote server session mode, it will raise the thread running
        //    dtrace.trace(5003);
        //    return;
        //}
        do {
            try {
                if (!isLocal) { // running as server, should always response client
                    //if (!requireChannel())
                    //    continue;
                    responseClient();
                    //releaseChannel();
                }
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
                            int callResult = removeData(dataSetName, filter, false);
                            asynCallParameters.clear();
                            asynResult = new Integer(callResult);
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
                            Prediction filter = (Prediction)asynCallParameters.get(1);
                            ArrayList colNames = (ArrayList)asynCallParameters.get(2);
                            ArrayList newValues = (ArrayList)asynCallParameters.get(3);
                            int callResult = modifyData(dataSetName, colNames, newValues, filter, false);
                            asynCallParameters.clear();
                            asynResult = new Integer(callResult);
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
                    sleep(1);
                } catch (InterruptedException e) {
                    dtrace.trace(36);
                    if (debuger.isDebugMode())
                        e.printStackTrace();
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
        } while (running && (!isLocal || asynStatus == Consts.INITIALIZED));
        sleeping = false;
    }
}
