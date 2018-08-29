/**
 * @(#)FyDataLogEntry.java	0.01 11/04/19
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_data;

//import fydb.fy_comm.BP;

//import java.util.ArrayList;
import fydb.fy_comm.Consts;
import fydb.fy_comm.Tracer;

import java.io.Serializable;

import java.util.HashMap;
import java.util.Iterator;

public class FyDataLogEntry extends FyBaseLogEntry implements Serializable{
    public class valueChangeEntry implements Serializable{
        String oldValue;
        String newValue;
    }

    //public BP bp;
    public class LogContent implements Serializable{
        public HashMap key;
        public int op;        // operation. 0: ignore; 1: insert; 2: modify; 3: delete;
        public HashMap value; // insert, the value equal with a data entry, which is the new value inserted
                              // modify, the value contains  colid:valueChangeEntry<oldValue, newValue>
                              // delete, the value equal with a data entry, which is the old value deleted
        public LogContent(int op, HashMap key, HashMap value){
            this.op = op;
            this.key = key;
            this.value = value;
        }
    }
  
    public LogContent  logData;    // content of the log.

    public FyDataLogEntry() {
        super(Consts.DATA);
        this.logData = new LogContent(-1,null,null);
    }

    //public FyLogEntry(BP bp, int op, HashMap key, HashMap value) {
    public FyDataLogEntry(int op, HashMap key, HashMap value) {
        this();
        //this.bp = bp;
        //this.bp.setSeq(seq);
        this.logData = new LogContent(op, key, value);
    }
    
    public void setKey(HashMap key){
        this.logData.key = key;
    }

    public void setValue(HashMap value){
        this.logData.value = value;
    }

    public valueChangeEntry generateNewChangeEntry(){
        return new valueChangeEntry();
    }

    /*
     * Contents of a log entry:
     * BP bp; (level0,level1,level2,seq)
     * int op; // operation. 0: ignore; 1: insert; 2: modify; 3: delete;
     * HashMap key;  // col_id1:keyValue1,col_id2:keyValue2..
     * HashMap value; // insert, the value equal with a data entry, which is the new value inserted
     *                // modify, the value contains  colid:valueChangeEntrys<oldValue, newValue>
     *                // delete, the value equal with a data entry, which is the old value deleted
     * 1|keynum;id1,len1,value;id2,len2,value2;..|colnum;id1,len1,value;id2,len2,value2;..$
     * 3|keynum;id1,len1,value;id2,len2,value2;..|colnum;id1,len1,value;id2,len2,value2;..$
     * 2|keynum;id1,len1,value;id2,len2,value2;..|colnum;id1,oldlen1,oldvalue1,newlen1,newdvalue1;id2,oldlen2,oldvalue2,newlen2,newdvalue2;..$
     * 
     * the log content following a log head as below
     * loglen|level0,level1,level2,seq|
     */
    public String encodeString(){
        String stream = new String();
        if (logType != Consts.DATA) // just encode DATA log
            return stream;
        stream = String.valueOf(logData.op)+"|";
        //stream = bp.encodeString()+"|"+String.valueOf(op)+"|"+String.valueOf(key.size())+";"; // bp and key info
        if (logData.key != null) {
            stream += String.valueOf(logData.key.size())+";"; // bp and key info
            Iterator it = logData.key.keySet().iterator();
            while (it.hasNext()){
                Integer colID = (Integer)it.next();
                String keyVal = (String)logData.key.get(colID);
                stream += String.valueOf(colID.intValue())+","+String.valueOf(keyVal.length())+","+keyVal+";";
            }
        } else {
            stream += "0|";
        }
        stream += "|";
        if (logData.value != null) {
            stream += String.valueOf(logData.value.size())+";";
            switch (logData.op) {
            case Consts.INSERT:
            case Consts.DELETE: // value format with insert/delete are same
                Iterator it = logData.value.keySet().iterator();
                while (it.hasNext()){
                    Integer colID = (Integer)it.next();
                    String val = (String)logData.value.get(colID);
                    if (val == null)
                        continue;
                    stream += String.valueOf(colID.intValue())+","+String.valueOf(val.length())+","+val+";";
                }
                break;
            case Consts.MODIFY: // valueChangeEntries
                it = logData.value.keySet().iterator();
                while (it.hasNext()){
                    Integer colID = (Integer)it.next();
                    valueChangeEntry val = (valueChangeEntry)logData.value.get(colID);
                    if (val == null)
                        continue;
                    stream += String.valueOf(colID.intValue())+","+String.valueOf(val.oldValue==null?0:val.oldValue.length())+","+(val.oldValue==null?"":val.oldValue)
                                                              +","+String.valueOf(val.newValue==null?0:val.newValue.length())+","+(val.newValue==null?"":val.newValue)+";";
                }
                break;
            }
        } else {
            stream += "0|";
        }
        stream += "$";
        //stream = String.valueOf(stream.length())+"|"+stream;
        return stream;
    }

    // decode string to contents of logentry
    public int decodeString(char[] stream, int off){
        if (logType != Consts.DATA) // just decode DATA log
            return 0;
        logData.key = new HashMap();
        logData.value = new HashMap();
        logData.op = -1;
        String tmpStr = new String();
        int initOff = off;
        try{
            // read op
            while (stream[off]!='|'){
                tmpStr += String.valueOf(stream[off]);
                off++;
            }
            logData.op = Integer.parseInt(tmpStr);
            off++; // skip "|"

            // read key
            tmpStr = "";
            // read key size
            while (stream[off]!=';'){
                tmpStr += String.valueOf(stream[off]);
                off++;
            }
            int keySize = Integer.parseInt(tmpStr.trim());
            off++; // skip ";"
            // read key entries
            for (int i=0; i<keySize; i++){
                // read colID
                tmpStr = "";
                while (stream[off]!=','){
                    tmpStr += String.valueOf(stream[off]);
                    off++;
                }
                Integer colID = new Integer(Integer.parseInt(tmpStr.trim()));
                off++; // skip ","

                 // read length of keyval
                 tmpStr = "";
                 while (stream[off]!=','){
                     tmpStr += String.valueOf(stream[off]);
                     off++;
                 }
                 int valLen = Integer.parseInt(tmpStr.trim());
                 off++; // skip ","
                 
                 String keyVal = valLen==0?null:String.valueOf(stream,off,valLen);
                 off+=(valLen+1); // skip keyVal and ";"
                 logData.key.put(colID, keyVal);
            }
            off++; // skip "|"

            // read values
            tmpStr = "";
            // read value size
            while (stream[off]!=';'){
                tmpStr += String.valueOf(stream[off]);
               off++;
            }
            int valSize = Integer.parseInt(tmpStr.trim());
            off++; // skip ";"
            // read key entries
            for (int i=0; i<valSize; i++){
                // read colID
                tmpStr = "";
                while (stream[off]!=','){
                    tmpStr += String.valueOf(stream[off]);
                    off++;
                }
                Integer colID = new Integer(Integer.parseInt(tmpStr.trim()));
                off++; // skip ","

                switch(logData.op){
                case Consts.INSERT:
                case Consts.DELETE:{
                        // read length of value
                        String strVale = new String();
                        tmpStr = "";
                        while (stream[off]!=','){
                            tmpStr += String.valueOf(stream[off]);
                            off++;
                        }
                        int valLen = Integer.parseInt(tmpStr.trim());
                        off++; // skip ","

                        strVale = valLen==0?null:String.valueOf(stream,off,valLen);
                        off+=(valLen+1); // skip keyVal and ";"
                        logData.value.put(colID, strVale);
                    }
                    break;
                case Consts.MODIFY:{
                        valueChangeEntry chgVal = new valueChangeEntry();
                        //read oldvalue
                        tmpStr = "";
                        while (stream[off]!=','){
                            tmpStr += String.valueOf(stream[off]);
                            off++;
                        }
                        int valLen = Integer.parseInt(tmpStr.trim());
                        off++; // skip ","
                        chgVal.oldValue = valLen==0?null:String.valueOf(stream,off,valLen);
                        off+=(valLen+1); // skip oldvalue and ","

                         //read newvalue
                        tmpStr = "";
                        while (stream[off]!=','){
                            tmpStr += String.valueOf(stream[off]);
                            off++;
                        }
                        valLen = Integer.parseInt(tmpStr.trim());
                        off++; // skip ","
                        chgVal.newValue = valLen==0?null:String.valueOf(stream,off,valLen);
                        off+=(valLen+1); // skip newvalue and ";"

                        logData.value.put(colID, chgVal);
                    }
                    break;
                }
            }
            off++; // skip "|"
            off++; // skip "$"
        }catch(Exception e){
            //dtrace.trace(26);
            e.printStackTrace();
            return -1;
        }
        return off-initOff;
    }
}
