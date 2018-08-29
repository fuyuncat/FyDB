/**
 * @(#)SessionBase.java	0.01 11/05/18
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_comm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public interface SessionBase{
    public int getSessionID();

    public HashMap buildKeyWithName(String dataSetName,
                                    ArrayList<String> colNames,
                                    ArrayList<String> values);

    // read single value from specified dataset
    public HashMap singleRead(String dataSetName, HashMap key);

    public boolean modifyDataByKeyI(String dataSetName, HashMap key,
                                    ArrayList<Integer> colIDs,
                                    ArrayList newValues);

    public boolean modifyDataByKeyS(String dataSetName, HashMap key,
                                    ArrayList<String> colNames,
                                    ArrayList newValues) ;

    public boolean modifyDataByKeyM(String dataSetName, HashMap key,
                                    HashMap mappedDatas) ;

    // batch modify value of a set of data, with assinged column&values, identify columns by id. return modified number
    public int batchModifyMemDataByKey(String dataSetName, HashSet keys,
                                       ArrayList<String> colNames,
                                       ArrayList newValues, boolean asynchronized) ;

    public boolean insertDataI(String dataSetName, ArrayList<Integer> colIDs,
                               ArrayList newValues) ;

    public boolean insertDataS(String dataSetName, ArrayList<String> colNames,
                               ArrayList newValues) ;

    public boolean insertDataM(String dataSetName, HashMap mappedDatas);

    // batch insert a set of data, with assinged columns, and a set of values. return inserted number
    public int batchInsertData(String dataSetName, ArrayList<String> colNames,
                               ArrayList<ArrayList> datas, boolean asynchronized);

    // delete a data entry, by key
    public boolean deleteDataByKey(String dataSetName, HashMap key) ;

    // batch delete a set of data, with assinged keys. return actually deleted number
    public int batchDeleteData(String dataSetName, HashSet<HashMap> keys, boolean asynchronized) ;
    
    //get all datasets list
    public ArrayList getDataSetsList();
    
    //search data from dataset, filtered by conditions
    public ArrayList searchData(String dataSetName, Prediction filter, boolean asynchronized);
    
    // get last asynchronized call state
    public int getLastAsynState();

    // get asynchronized call result
    public Object getAsynchronizedResult();
}
