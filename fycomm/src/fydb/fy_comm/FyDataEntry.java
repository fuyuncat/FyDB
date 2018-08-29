/**
 * @(#)FyDataEntry.java	0.01 11/04/19
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_comm;

import java.sql.Timestamp;

import java.util.HashMap;

public class FyDataEntry  implements java.io.Serializable{
    //public BP bp;
    public HashMap key;  // column_id:column_value (notice: a key may not contain all key-columns)
    public HashMap value;  // column_id:column_value (notice: a value may not contain all columns)

    public FyDataEntry() {
    }

    public FyDataEntry(HashMap key, HashMap value) {
        //this.bp = bp;
        this.key = key;
        this.value = value;
    }
    
    // endcode data entry to byte array, considering data type
    // colId1(4b)colLen1(4b)col1...
    // id in sequence
    public byte[] encodeData(int colNum, FyMetaData metaData){
        byte[] dataBytes = new byte[0];
        try{
            for (int i=0;i<colNum;i++){
                String colData = (String)key.get(Integer.valueOf(i));
                colData = colData==null?(String)value.get(Integer.valueOf(i)):colData;
                if (colData != null){
                    int colType = (Integer)((HashMap)metaData.getColumns().get(Integer.valueOf(i))).get("T");
                    byte[] colBytes = new byte[0];
                    switch (colType){
                        case Consts.INTEGER:{
                            int rData = Integer.parseInt(colData);
                            colBytes = CommUtility.intToByteArray(rData);
                            break;
                        }
                        case Consts.LONG:{
                            long rData = Long.parseLong(colData);
                            colBytes = CommUtility.longToByteArray(rData);
                            break;
                        }
                        case Consts.DOUBLE:{
                            double rData = Double.parseDouble(colData);
                            colBytes = CommUtility.doubleToByteArray(rData);;
                            break;
                        }
                        case Consts.STRING:{
                            colBytes = colData.getBytes();
                            break;
                        }
                        case Consts.DATE:
                        case Consts.TIMESTAMP:{
                            Timestamp rData = Timestamp.valueOf(colData);
                            colBytes = CommUtility.longToByteArray(rData.getTime());
                            break;
                        }
                        case Consts.BOOLEAN:{
                            colBytes = new byte[1];
                            colBytes[0] = Boolean.parseBoolean(colData)?(byte)1:(byte)0;
                            break;
                        }
                        default:{
                            break;
                        }
                    }
                    dataBytes = CommUtility.appendToArrayB(dataBytes,CommUtility.intToByteArray(i));
                    dataBytes = CommUtility.appendToArrayB(dataBytes,CommUtility.intToByteArray(colBytes.length));
                    dataBytes = CommUtility.appendToArrayB(dataBytes,colBytes);
                }
            }
        }catch(Exception e){
            e.printStackTrace();
            dataBytes = new byte[0];
        }
        return dataBytes;
    }

    // dedcode byte array to data entry, considering data type
    // colId1(4b)colLen1(4b)col1...
    // id in sequence
    public int decodeData(byte[] stream, int off, int entryLen, int keyNum, String encoding, FyMetaData metaData){
        key = new HashMap();
        value = new HashMap();

        int initOff = off;

        // parsing columns
        while (off-initOff<entryLen){
            int colId = 0;
            colId = CommUtility.readIntFromBlock(stream,off);
            off+=4; // skip colId
            int len = CommUtility.readIntFromBlock(stream,off);
            off+=4; // skip keyNum
            byte[] colBytes = new byte[len];
            CommUtility.arrayCopy(stream,off,colBytes,0,colBytes.length);
            String colData = "";
            int colType = (Integer)((HashMap)metaData.getColumns().get(Integer.valueOf(colId))).get("T");
            switch (colType){
            case Consts.INTEGER:{
                    int rData = CommUtility.byteArrayToInt(colBytes);
                    colData = String.valueOf(rData);
                    break;
                }
                case Consts.LONG:{
                    long rData = CommUtility.byteArrayToLong(colBytes);
                    colData = String.valueOf(rData);
                    break;
                }
                case Consts.DOUBLE:{
                    double rData = CommUtility.byteArrayToDouble(colBytes);
                    colData = String.valueOf(rData);
                    break;
                }
                case Consts.STRING:{
                    colData = new String(colBytes);
                    break;
                }
                case Consts.DATE:
                case Consts.TIMESTAMP:{
                    Timestamp rData = new Timestamp(CommUtility.byteArrayToLong(colBytes));
                    colData = rData.toString();
                    break;
                }
                case Consts.BOOLEAN:{
                    boolean rData = colBytes[1]==(byte)1?true:false;
                    colData = String.valueOf(rData);
                    break;
                }
                default:{
                    break;
                }
            }
            off+=len; // skip colName
            if (colId<keyNum)
                key.put(Integer.valueOf(colId), colData);
            else
                value.put(Integer.valueOf(colId), colData);
        }
        
        return off-initOff;
    }
    // endcode data entry to byte array
    // colId1(4b)colLen1(4b)col1...
    // id in sequence
    public byte[] encodeData(int colNum){
        byte[] dataBytes = new byte[0];
        for (int i=0;i<colNum;i++){
            String colData = (String)key.get(Integer.valueOf(i));
            colData = colData==null?(String)value.get(Integer.valueOf(i)):colData;
            if (colData != null){
                byte[] colBytes = colData.getBytes();
                dataBytes = CommUtility.appendToArrayB(dataBytes,CommUtility.intToByteArray(i));
                dataBytes = CommUtility.appendToArrayB(dataBytes,CommUtility.intToByteArray(colBytes.length));
                dataBytes = CommUtility.appendToArrayB(dataBytes,colBytes);
            }
        }
        return dataBytes;
    }

    // dedcode byte array to data entry
    // colId1(4b)colLen1(4b)col1...
    // id in sequence
    public int decodeData(byte[] stream, int off, int entryLen, int keyNum, String encoding){
        key = new HashMap();
        value = new HashMap();

        int initOff = off;

        // parsing columns
        while (off-initOff<entryLen){
            int colId = 0;
            colId = CommUtility.readIntFromBlock(stream,off);
            off+=4; // skip colId
            int len = CommUtility.readIntFromBlock(stream,off);
            off+=4; // skip keyNum
            String colData = CommUtility.readStringFromBlock(stream,off,len,encoding);
            off+=len; // skip colName
            if (colId<keyNum)
                key.put(Integer.valueOf(colId), colData);
            else
                value.put(Integer.valueOf(colId), colData);
        }
        
        return off-initOff;
    }
}
