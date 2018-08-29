 /*
  * @(#)FyIndex.java   0.01 11/05/24
  *
  * Copyright 2011 Fuyuncat. All rights reserved.
  * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
  * Email: fuyucat@gmail.com
  * WebSite: www.HelloDBA.com
  */
package fydb.fy_data;

import fydb.fy_comm.CommUtility;
import fydb.fy_comm.Consts;
import fydb.fy_comm.FyMetaData;
import fydb.fy_comm.Tracer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class FyIndex {
    private Tracer  dtrace;
    private ArrayList indColumns;
    private boolean initialized = false; // we may load index defination from local xml file. In such senarios, 
                                         // indColumns will be asssinged as columns Name, 
                                         // and it requires re-assingn column when loading data from physical source
    private int state = Consts.VALID;     // index state. VALID; INVALID
    private int indexType;
    // indxed data is ArrayList. map to arraylist of Keys
    private MemBaseData indData;        // indexedColData(Arraylist):keys(Arraylist)

    public FyIndex(Tracer dtrace, ArrayList indColumns, int indexType) {
        this.indColumns = indColumns;
        this.indexType = indexType;
        this.dtrace = dtrace;
    }
    
    public ArrayList getIndColumns(){
        return this.indColumns;
    }
    
    public void setIndData(MemBaseData indData){
        this.indData = indData;
    }
    
    public MemBaseData getIndData(){
        return this.indData;
    }

    public int getIndType(){
        return this.indexType;
    }
    
    public void setState(int state){
        this.state = state;
    }

    public int getState(){
        return state;
    }
    
    public void initialize(FyMetaData metaData){
        if (initialized)
            return;
        ArrayList newIndColumns = new ArrayList();
        for (int i=0;i<indColumns.size();i++){
            String indColumn = null;
            try{
                indColumn = (String)indColumns.get(i);
            }catch(ClassCastException e){
                continue;
            }
            // detect if it has been set as colID already
            Integer colID = null;
            try {
                colID = Integer.valueOf(indColumn);
            }catch(NumberFormatException e){ // otherwise, convert name to ID
                colID = Integer.valueOf(metaData.getColumnID(indColumn));
            }
            if (colID != null && colID.intValue() >= 0)
               newIndColumns.add(colID);
        }
        this.indColumns =newIndColumns;
        
        switch (indexType){
        case Consts.HASHMAP: //HashMap
            indData = new MemHashKVData(dtrace);
            break;
        case Consts.TREEMAP: //TreeMap:
        case Consts.SORTEDMAP: //SortedMap:
            indData = new MemTreeKVData(new FyDataSet.ColumnsComparator(indColumns, metaData), dtrace);
            break;
        }

        initialized = true;
    }

    public boolean hasInitialized(){
        return initialized;
    }

    // get the size of index
    public long sizeOf(){
        long totalSize = 0;
        totalSize += indColumns.size()*4;
        Iterator it = indData.keySet().iterator();
        while (it.hasNext()){
            ArrayList indColData = (ArrayList)it.next();
            totalSize += CommUtility.sizeOf(indColData);
            ArrayList keys = (ArrayList)indData.get(indColData);
            for (int i=0;i<keys.size();i++){
                totalSize += CommUtility.sizeOf((HashMap)keys.get(i));
            }
        }
        return totalSize;
    }
    
    public void release(){
        indData = null;
        initialized = false;
    }
}
