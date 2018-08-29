/**
 * @(#)Optimizer.java	0.01 11/06/01
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_data;

import fydb.fy_comm.Consts;
import fydb.fy_comm.Prediction;

import fydb.fy_comm.Tracer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class Optimizer {
    private Tracer dtrace;
    private ArrayList keyColumns;
    private int dataSize;
    private HashMap indexes;
    private int memType;

    // searching method
    public class SearchMethod {
        public int method = Consts.FULLDATASCAN;
        public int indId = -1; // -1 means scan table, 0 means key, others are index id
        public Prediction indexAccesser = new Prediction();
        public Prediction expressionAccesser = new Prediction();
        public Prediction indexFilter = new Prediction();
        public Prediction dataFilter = new Prediction();

        public void copyTo(SearchMethod otherMethod){
            otherMethod.method = this.method;
            otherMethod.indId = this.indId;
            indexAccesser.copyTo(otherMethod.indexAccesser);
            expressionAccesser.copyTo(otherMethod.expressionAccesser);
            indexFilter.copyTo(otherMethod.indexFilter);
            dataFilter.copyTo(otherMethod.dataFilter);
        }
    }
    
    public class JoinMethod {
        public int method = Consts.LOOPJOIN;
        public int out = 0;
        //public int in = 0;

        public void copyTo(JoinMethod otherMethod){
            otherMethod.method = this.method;
            otherMethod.out = this.out;
            //otherMethod.in = this.in;
        }
    }
    
    public SearchMethod newSearchMethod(){
        return new SearchMethod();
    }

    public JoinMethod newJoinMethod(){
        return new JoinMethod();
    }

    public Optimizer(Tracer dtrace) {
        this.dtrace = dtrace;
    }

    public Optimizer(ArrayList keyColumns, int memType, int dataSize, HashMap indexes, Tracer dtrace) {
        this.keyColumns = keyColumns;
        this.memType = memType;
        this.dataSize = dataSize;
        this.indexes = indexes;
        this.dtrace = dtrace;
    }

    // further split accesser to columns accesser and expression accesser
    private void splitAccesser(){
    }

    // considering special index, detect Prediction, assign accesser and filter
    // return the RAW(potential) accesser, indexFilter, dataFilter. Reqiure detectIndex procedure further detect
    private void detectPrediction(Prediction prediction, SearchMethod searchMethod, ArrayList indexColumns){
        if (prediction.type == Consts.BRANCH){
            if (prediction.leftNode == null || prediction.rightNode == null){
                dtrace.trace(119);
                //return;
            }else{
                if (prediction.junction == Consts.AND) { // still in top level, detect children
                    detectPrediction(prediction.leftNode, searchMethod, indexColumns);
                    detectPrediction(prediction.rightNode, searchMethod, indexColumns);
                }else{ // OR will change the level, and the children will not be detected, it should be added to filter directly
                    Prediction newNode = new Prediction();
                    prediction.copyTo(newNode);
                    searchMethod.dataFilter.add(newNode, prediction.parentNode==null?Consts.UNKNOWN:prediction.parentNode.junction, false, true);
                    //return;
                }
            }
        }else if (prediction.type == Consts.LEAF){
            Integer colID = new Integer(prediction.leftColId);
            // only the top level indexed prediction columns with EQUAL comparators will be "access"
            // if a column has duplicated EQUAL comparator, the data will be exchange as ness access;
            // example. col1=A AND col1=B  =>  col1=A and A=B
            // expression without column comparation would also be access
            if (prediction.leftColId == -1) { // it's expression without column comparation
                 Prediction newNode = new Prediction();
                 prediction.copyTo(newNode);
                 searchMethod.expressionAccesser.add(newNode, Consts.AND, true, true); // expression add to left
            //}else if (prediction.comparator == Consts.EQ && indexColumns.contains(colID)){
            // all predication with non-NEQ could be potential accesser
            }else if (prediction.comparator != Consts.NEQ && indexColumns.contains(colID)){
                // delete if is an inaccesable column already
                // if not, then detect if duplicated.
                Prediction existingNode = searchMethod.indexAccesser.getFirstPredByColId(prediction.leftColId, true);
                // if duplicated, detect if equal to same value, NULL value should be considered
                if (existingNode != null){  
                    // if equal to same value, it could be ignored, otherwise, convert to expression
                    if ((existingNode.rightExpression == null && existingNode.rightExpression != prediction.rightExpression ) || 
                        (existingNode.rightExpression != null && !existingNode.rightExpression.equals(prediction.rightExpression))){
                        Prediction newNode = new Prediction(prediction);
                        newNode.leftColId = -1;
                        newNode.leftExpression = existingNode.rightExpression;
                        searchMethod.expressionAccesser.add(newNode, Consts.AND, true, true); // expression add to left
                    }
                }else{
                    Prediction newNode = new Prediction();
                    prediction.copyTo(newNode);
                    searchMethod.indexAccesser.add(newNode, Consts.AND, false, true); // column comparation add to right
                }
            // top level indexed prediction column with non EQ comparator, 
            // ---incorrect(we store an arraylist of keys, instead of a single key): or key columns would be potential indexFilter---
            //}else if((prediction.comparator != Consts.EQ && indexColumns.contains(colID)) || keyColumns.contains(colID)){
            }else if((prediction.comparator == Consts.NEQ && indexColumns.contains(colID))){
                Prediction newNode = new Prediction();
                prediction.copyTo(newNode);
                searchMethod.indexFilter.add(newNode, prediction.parentNode==null?Consts.UNKNOWN:prediction.parentNode.junction, false, true); // add to filter
            }else{ // other prediction column would be dataFilter
                Prediction newNode = new Prediction();
                prediction.copyTo(newNode);
                searchMethod.dataFilter.add(newNode, prediction.parentNode==null?Consts.UNKNOWN:prediction.parentNode.junction, false, true); // add to filter
            }
        }else{
            //dtrace.trace(121);
            return;
        }
    }

    // match searchMethod with an index, identify real accesser and filter
    private void detectIndex(SearchMethod searchMethod, ArrayList indexColumns, int indexType){
        //HashSet accessrColIDs = searchMethod.indexAccesser.getAllColIDs(Consts.LEFT);
        // if all columns matched as access, it could call get directly
        //if (accessrColIDs.containsAll(indexColumns) && indexColumns.containsAll(accessrColIDs)){
        //    searchMethod.method = Consts.INDEXGET;
        //}else{ 
            searchMethod.method = Consts.FULLDATASCAN;
            Prediction revisedAccesser = new Prediction();
            // for TREEMAP&SORTEDMAP, detect if can call partindexscan or fullindexscan
            if (indexType == Consts.TREEMAP || indexType == Consts.SORTEDMAP){
                boolean stopAccess = false;
                for (int i=0;i<indexColumns.size();i++){
                    Integer colID = (Integer)indexColumns.get(i);
                    Prediction node = searchMethod.indexAccesser.getFirstPredByColId(colID.intValue(),true);
                    if (node != null && !stopAccess) {// detect if accesser contains leading columns of index
                        // leading column in accesser is real accesser
                        searchMethod.method = Consts.PARTINDEXSCAN;
                        revisedAccesser.add(new Prediction(node),Consts.AND,false,true);
                        // real index accesser is kept in revisedAccesser, the left ones are actually expresion accessor or indexfilter
                        searchMethod.indexAccesser.remove(node); 
                        if (node.comparator != Consts.EQ)
                            stopAccess = true;
                    }else
                        break;
                }
                if (revisedAccesser.type == Consts.UNKNOWN) // no accessable column, call fullindexscan
                    searchMethod.method = Consts.FULLINDEXSCAN;
                else{
                    HashSet accessrColIDs = revisedAccesser.getAllColIDs(Consts.LEFT);
                    // all columns are EQ compare, choose INDEXGET
                    if (accessrColIDs.containsAll(indexColumns) && indexColumns.containsAll(accessrColIDs) && !stopAccess)
                        searchMethod.method = Consts.INDEXGET;
                }
            }else if (indexType == Consts.HASHMAP){
                boolean accessable = true;
                for (int i=0;i<indexColumns.size();i++){
                    Integer colID = (Integer)indexColumns.get(i);
                    Prediction node = searchMethod.indexAccesser.getFirstPredByColId(colID.intValue(),true);
                    // detect if accesser contains all columns of index
                    if (node == null || node.comparator != Consts.EQ){
                        accessable = false;
                        break;
                    }
                }
                if (accessable){
                    searchMethod.method = Consts.INDEXGET;
                    searchMethod.indexAccesser.copyTo(revisedAccesser);
                }else{
                    searchMethod.method = Consts.FULLINDEXSCAN;
                    //searchMethod.indexAccesser.copyTo(searchMethod.indexFilter);
                    //searchMethod.indexAccesser.clear();
                }
            }
            // express accessers are expressionAccesser
            Prediction node = searchMethod.indexAccesser.getFirstPredByColId(-1,true);
            while (node != null){
                searchMethod.expressionAccesser.add(new Prediction(node),Consts.AND,false,true);
                searchMethod.indexAccesser.remove(node);
                node = searchMethod.indexAccesser.getFirstPredByColId(-1,true);
            }
            // other accessers are actually index filters
            Prediction suppIndexFilter = new Prediction();
            searchMethod.indexAccesser.copyTo(suppIndexFilter);
            searchMethod.indexFilter.add(suppIndexFilter, Consts.AND,false,true);
            // the revised accesser is the final index accesser
            revisedAccesser.copyTo(searchMethod.indexAccesser);
            revisedAccesser = null;

            // check if indexFilter contains any indexed column (beacause it may also contains key columns)
            // obsoleted: we do not put the key prediction column into indexfilter anymore...
            /*boolean isRealIndexFilter = false;
            for (int i=0;i<indexColumns.size();i++){
                Integer colID = (Integer)indexColumns.get(i);
                node = searchMethod.indexFilter.getFirstPredByColId(colID.intValue(),true);
                if (node != null) {// detect if accesser contains leading columns of index
                    isRealIndexFilter = true;
                    break;
                }
            }
            // if it does not contain any indexed column, move it to data filter
            if (!isRealIndexFilter){
                Prediction tempIndexFilter = searchMethod.indexFilter.cloneMe();
                searchMethod.dataFilter.add(tempIndexFilter, Consts.AND,false,true);
                searchMethod.indexFilter.clear();
            }//*/

            // no indexAccesser but has indexFilter would be FULLINDEXSCAN
            // no indexAccesser and no indexFilter would be FULLDATASCAN
            if (searchMethod.indexAccesser.size() == 0){
                if (searchMethod.indexFilter.size() > 0)
                    searchMethod.method = Consts.FULLINDEXSCAN;
                else
                    searchMethod.method = Consts.FULLDATASCAN;
            }

            if (searchMethod.method == Consts.FULLDATASCAN){
                searchMethod.dataFilter.add(searchMethod.indexAccesser,Consts.AND,true,true);
                searchMethod.dataFilter.add(searchMethod.indexFilter,Consts.AND,true,true);
                searchMethod.indId = -1;
            }

            // for fulldatascan, should move indexAccesser&indexFilter into dataFilter
            //searchMethod.dataFilter.add(searchMethod.indexAccesser,Consts.AND,true,true);
            //searchMethod.dataFilter.add(searchMethod.indexFilter,Consts.AND,true,true);
            //searchMethod.method = Consts.FULLDATASCAN;
            //searchMethod.indId = -1;
        //}
    }
    
    // optimize the searching path
    // Only if the index contains all predicting columns, the index could be hitted;
    // Onlu if all predicting columns are key columns, vice versa, and all operators are = and all junctions are "and", the key has the highest priority
    // Only if all predicting columns are hash index columns, vice versa, and all operators are = and all junctions are "and", the hash index has the highest priority following key
    // Index with few columns number has higher priority then the one with more columns, key has higher priorit than other indexes
    // if no index could be hitted, access memData directly
    // 1: key(all columns matched with ==&"and")
    // 2: hash index(all columns matched with ==&"and")
    // 3: index(part columns matched, selected columns are indexed columns or key columns)
    // 4: key(part columns matched)
    // 5: index(part columns matched, fewer columns in index, require to access datasets to get selected columns)
    // 6: index(part columns matched, more columns in index, require to access datasets to get selected columns)
    //
    // keyGet > indexGet > keyScan(sorted or tree) > indexScan (sorted or tree) > keyFullScan > indexFullScan > memdatFullScan
    // keyGet: key = accessColData;
    // indexGet: index = accessColData;
    // keyScan: key is sorted or tree && part of all accessColData columns is(are) leading columns of key;
    // indexScan: index is sorted or tree && part of all accessColData columns is(are) leading columns of index;
    // indexFullScan: index is hash && part of allColumns is(are) columns of index;
    //                index is sorted or tree && none accessColData columns is leading column of key;
    // keyFullScan: 
    // memdatFullScan:
    //
    // @return int index ID, 0 is key, -1 means not matched, will involve full scan
    public void optimizeSearch(Prediction prediction, SearchMethod searchMethod, HashSet selectColumns){
        // detect key first
        searchMethod.indId = 0;
        detectPrediction(prediction, searchMethod, keyColumns);
        detectIndex(searchMethod,keyColumns,memType);
        if (searchMethod.method == Consts.INDEXGET)
            return;
        HashSet keyCols = new HashSet(keyColumns);
        HashSet availableColumns1 = new HashSet(keyCols);
        Iterator it = indexes.keySet().iterator();
        while (it.hasNext()){
            SearchMethod searchMethod2 = new SearchMethod();
            Integer indID = (Integer)it.next();
            FyIndex index = (FyIndex)indexes.get(indID);
            searchMethod2.indId = indID.intValue();
            detectPrediction(prediction, searchMethod2, index.getIndColumns());
            detectIndex(searchMethod2,index.getIndColumns(),index.getIndType());
            HashSet availableColumns2 = new HashSet(index.getIndColumns());
            availableColumns2.addAll(keyCols);
            if (searchMethod2.method == Consts.INDEXGET){
                searchMethod2.copyTo(searchMethod);
                return;
            }else { // compare 2 methodes, choose the best one
                // we simply choose the one with more accesser
                if ((searchMethod2.method < searchMethod.method))
                    searchMethod2.copyTo(searchMethod);
                else if (searchMethod2.method == searchMethod.method){
                    if (availableColumns1.containsAll(selectColumns) && !availableColumns2.containsAll(selectColumns)){
                        continue;
                    }else if(!availableColumns1.containsAll(selectColumns) && availableColumns2.containsAll(selectColumns)){
                        searchMethod2.copyTo(searchMethod);
                    }else if ((searchMethod2.method == Consts.PARTINDEXSCAN || searchMethod2.method == Consts.FULLINDEXSCAN)){
                        if (searchMethod.indId == 0) // previous index is the key
                            if (index.getIndData().size() < dataSize)
                                searchMethod2.copyTo(searchMethod);
                        else{
                            if (searchMethod.indId < 0) // full scan, should not reach here!
                                searchMethod2.copyTo(searchMethod);
                            else if (searchMethod.indId == 0){ // key. always choose key
                            }else{
                                FyIndex preIndex = (FyIndex)indexes.get(Integer.valueOf(searchMethod.indId));
                                if (index.getIndData().size() < preIndex.getIndData().size()) 
                                    searchMethod2.copyTo(searchMethod);
                            }
                        }
                    }else if (searchMethod2.indexAccesser.size() > searchMethod.indexAccesser.size()){
                            searchMethod2.copyTo(searchMethod);
                    }
                }
            }
            availableColumns1 = availableColumns2;
        }
    }
    
    public void optimizeJoin(JoinMethod joinMethod, ArrayList d1, ArrayList d2, int joinMode){
        joinMethod.method = Consts.LOOPJOIN;
        if (joinMode == Consts.LEFTJOIN)
            joinMethod.out = 1;
        else if (joinMode == Consts.RIGHTJOIN)
            joinMethod.out = 2;
        else if (joinMode == Consts.OUTERJOIN){ // for outer join, choose the larger one as out loop
            if (d1.size() < d2.size())
                joinMethod.out = 2;
            else
                joinMethod.out = 1;
        } else {
            if (d1.size() < d2.size())
                joinMethod.out = 1;
            else
                joinMethod.out = 2;
        }
    }
}
