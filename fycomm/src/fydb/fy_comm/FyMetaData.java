/*
 * @(#)FyMetaData.java	0.01 11/04/20
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_comm;

import fydb.fy_comm.Tracer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class FyMetaData implements java.io.Serializable {
    private static Tracer dtrace; // = new Tracer(); the static member will NOT be serialized

    private String name;
    private long entryCount = 0;
    private HashMap columns; // column_id:column_properties<name,type,iskey>
    private ArrayList keyColumns; // column_id; If is HashMap, position is meaningless, if is TreeMap/SortedMap, position is physical position

    public FyMetaData(FyMetaData otherMeta) {
        this.dtrace = otherMeta.dtrace;
        this.name = new String(otherMeta.name);
        this.entryCount = otherMeta.entryCount;
        this.columns = new HashMap(otherMeta.columns);
        this.keyColumns = new ArrayList(otherMeta.keyColumns);
    }
      
    public FyMetaData(Tracer dtrace, String name) {
        this.dtrace = dtrace;
        this.name = name;
    }
     
    public FyMetaData(Tracer dtrace, String name, long entryCount, HashMap columns, ArrayList keyColNames) {
        this(dtrace, name);
        this.entryCount = entryCount;
        this.columns = columns;
        this.keyColumns = getColIDByName(keyColNames);
    }
    
    //public void setName(String name){
    //    this.name = name;
    //}
    
    public String getName(){
        return new String(name);
    }
    
    public void setEntryCount(long entryCount){
        this.entryCount = entryCount;
    }

    public void setTracer(Tracer dtrace){
        this.dtrace = dtrace;
    }

    public long getEntryCount(){
        return entryCount;
    }

    //public void setColumns(HashMap columns){
    //    this.columns = columns;
    //    Iterator it = columns.keySet().iterator(); 
    //    while(it.hasNext()) {
    //        Integer colID = (Integer)it.next();
    //        Map colProperties = (HashMap)columns.get(colID);
    //        if ( ((Integer)colProperties.get("K")).intValue() >= 0 )
    //            keyColumns.put(colID, new String((String)colProperties.get("N")));
    //    }
    //}

    public HashMap getColumns(){
        return new HashMap(columns);
    }
    
    public ArrayList getKeyColumns(){
        return new ArrayList(keyColumns);
    }

    public void releaseData(){
        name = "";
        entryCount = 0;
        columns.clear();
    }

    // return column name
    public String getColumnName(Integer colID){
        if (columns == null){
            dtrace.trace(222);
            return null;
        }
        HashMap colProperties = (HashMap)columns.get(colID);
        if (colProperties == null){
            dtrace.trace(222);
            return null;
        }
        return (String)colProperties.get("N");
    }

    // return column name
    public int getColumnID(String colName){
        if (colName == null)
            return -1;
        Iterator it = columns.keySet().iterator();
        while(it.hasNext()){
            Integer colID = (Integer)it.next();
            HashMap colProperties = (HashMap)columns.get(colID);
            if (colName.equalsIgnoreCase((String)colProperties.get("N")))
                return colID.intValue();
        }
        return -1;
    }

    // return column data type
    public int getColumnType(Integer colID){
        if (columns == null){
            dtrace.trace(222);
            return Consts.UNKNOWN;
        }
        HashMap colProperties = (HashMap)columns.get(colID);
        if (colProperties == null){
            dtrace.trace(222);
            return Consts.UNKNOWN;
        }
        return ((Integer)colProperties.get("T")).intValue();
    }

    // test a column is a key column or not
    public boolean isKeyColumn(Integer colID){
        if (keyColumns == null){
            dtrace.trace(222);
            return false;
        }
        return keyColumns.contains(colID);
    }
    
    //  a column if allow null values; 0 means not allow, 1 allow; -1 error
    public int isNullable(Integer colID){
        if (columns == null){
            dtrace.trace(222);
            return -1;
        }else{
            HashMap colProperties = (HashMap)columns.get(colID);
            if (colProperties == null){
                dtrace.trace(222);
                return -1;
            }else{
                return ((Integer)colProperties.get("NL")).intValue();
            }
        }
    }

    // get column IDs identifiey by names.
    public ArrayList<Integer> getColIDByName(ArrayList<String> colNames){
        ArrayList<Integer> colIDs = new ArrayList<Integer>();
        for (int i=0; i<colNames.size(); i++){
            String colName = colNames.get(i);
            if (colName == null)
            {
                dtrace.trace(505);
                colIDs.add(new Integer(-1));
                continue;
            }
            boolean foundColumn = false;
            Iterator it = columns.keySet().iterator(); 
            while(it.hasNext()) {
                Integer colID = (Integer)it.next();
                if (colName.equals((String)((HashMap)columns.get(colID)).get("N")))
                {
                    colIDs.add(colID);
                    foundColumn = true;
                    break;
                }
            }
            if (!foundColumn)
            {
                dtrace.trace(505);
                colIDs.add(-1);
            }
        }
        return colIDs;
    }

    // generate a key with the input key column name and value; can just accept key columns
    public HashMap buildKeyWithColName(ArrayList<String> colNames, ArrayList<String> values){
        if (colNames == null){
            dtrace.trace(216);
            return null;
        }
        if (values == null){
            dtrace.trace(217);
            return null;
        }
        if (values.size() != keyColumns.size()){
            dtrace.trace(218);
            return null;
        }
        if (colNames.size() != keyColumns.size()){
            dtrace.trace(219);
            return null;
        }
        HashMap key = new HashMap();
        ArrayList<Integer> colIDs = getColIDByName(colNames);
        for (int i=0; i<colIDs.size(); i++){
            Integer colID = colIDs.get(i);
            if (isKeyColumn(colID)) { // the column set contains a key column
                key.put(colID, (String)values.get(i));
            }else{
                dtrace.trace(220);
                return null;
            }
        }
        return key;
    }

    // generate a key with the input key column name and value
    public HashMap identifyKey(HashMap data){
        if (data == null)
            return null;
        HashMap key = new HashMap();
        for (int i=0;i<keyColumns.size();i++){
            Integer colID = (Integer)keyColumns.get(i);
            String colVal =  (String)data.get(colID);
            key.put(new Integer(colID), new String(colVal));
        }
        return key;
    }

    // generate a key with the input key column name and value
    public HashMap buildKeyWithColID(ArrayList<Integer> colIDs, ArrayList<String> values){
        if (colIDs == null){
            dtrace.trace(242);
            return null;
        }
        if (values == null){
            dtrace.trace(217);
            return null;
        }
        if (values.size() != colIDs.size()){
            dtrace.trace(243);
            return null;
        }
        HashMap key = new HashMap();
        for (int i=0; i<colIDs.size(); i++){
            Integer colID = colIDs.get(i);
            if (isKeyColumn(colID)) { // the column set contains a key column
                key.put(colID, (String)values.get(i));
            }else{
                dtrace.trace(220);
                return null;
            }
        }
        return key;
    }

    // convert metat info to string
    // ID adopt sequence number starting from 0, key columns inclusive
    // totalColNum(4b)keyNum(4b)keylen1(4b)keyname1 keynullable11(1b)keytype(4b)..collen1(4b)colname1colnullable11(1b)coltype1(4b)
    public byte[] encodeMeta(){
        byte[] stream = new byte[0];
        stream = CommUtility.appendToArrayB(stream,CommUtility.intToByteArray(columns.size()));
        stream = CommUtility.appendToArrayB(stream,CommUtility.intToByteArray(keyColumns.size()));
        byte[] keyCols = new byte[0];
        byte[] otherCols = new byte[0];
        for (int i=0;i<columns.size();i++){
            HashMap colProperties = (HashMap)columns.get(Integer.valueOf(i));
            byte[] colPropBytes = new byte[0];
            colPropBytes = CommUtility.appendToArrayB(colPropBytes,CommUtility.intToByteArray(((String)colProperties.get("N")).length()));
            colPropBytes = CommUtility.appendToArrayB(colPropBytes,((String)colProperties.get("N")).getBytes());
            colPropBytes = CommUtility.appendToArrayB(colPropBytes,(Integer)colProperties.get("NL")==1?(byte)0x1:(byte)0x0);
            colPropBytes = CommUtility.appendToArrayB(colPropBytes,CommUtility.intToByteArray(((Integer)colProperties.get("T")).intValue()));

            if (isKeyColumn(Integer.valueOf(i)))
                keyCols = CommUtility.appendToArrayB(keyCols,colPropBytes);
            else
                otherCols = CommUtility.appendToArrayB(otherCols,colPropBytes);
        }
        stream = CommUtility.appendToArrayB(stream,keyCols);
        stream = CommUtility.appendToArrayB(stream,otherCols);
        return stream;
    }

    // convert string to metat info
    // ID adopt sequence number starting from 0, key columns inclusive
    // totalColNum(4b)keyNum(4b)keylen1(4b)keyname1 keynullable11(1b)keytype(4b)..collen1(4b)colname1colnullable11(1b)coltype1(4b)
    public int decodeMeta(byte[] stream, int off, String encoding){
        keyColumns = new ArrayList();
        columns = new HashMap();
    
        int initOff = off;

        int colNum = 0;
        colNum = CommUtility.readIntFromBlock(stream,off);
        off+=4; // skip colNum

        int keyNum = 0;
        keyNum = CommUtility.readIntFromBlock(stream,off);
        off+=4; // skip keyNum

        // parsing columns
        for (int i=0;i<colNum;i++){
            HashMap colProperties= new HashMap();
            int len = CommUtility.readIntFromBlock(stream,off);
            off+=4; // skip keyNum
            String colName = CommUtility.readStringFromBlock(stream,off,len,encoding);
            colProperties.put("N", colName);
            off+=len; // skip colName
            int nullable = stream[off]==(byte)0x1?1:0;
            colProperties.put("NL", Integer.valueOf(nullable));
            off+=1; // skip nullable
            int dataType = CommUtility.readIntFromBlock(stream,off);
            colProperties.put("T", Integer.valueOf(dataType));
            off+=4; // skip datatype
            columns.put(Integer.valueOf(i),colProperties);
            if (i<keyNum){
                keyColumns.add(Integer.valueOf(i));
                colProperties.put("K", Integer.valueOf(i));
            }else
                colProperties.put("K", -1);
        }
   
        return off-initOff;
    }
    
    // get the size of metadata
    public long sizeOf(){
        long totalSize = name.getBytes().length;
        totalSize += keyColumns.size()*4;
        Iterator it = columns.values().iterator();
        while (it.hasNext()){
            totalSize += 16; // 4 bytes for colId; 4 bytes for NL property; 4 bytes for T property; 4 bytes for K property
            HashMap colProps = (HashMap)it.next();
            String str = (String)colProps.get("N"); // column name
            totalSize += str==null?0:str.getBytes().length;
        }
        return totalSize;
    }
}
