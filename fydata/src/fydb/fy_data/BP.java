/**
 * @(#)BP.java	0.01 11/04/26
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_data;

import java.io.Serializable;

public final class BP implements Serializable{
    public long level0 = 0;
    public long level1 = 0;
    public long level2 = 0;
    public int seq = 0;

    public BP() {
    }
    
    public BP(BP bp) {
        this.level0 = bp.level0;
        this.level1 = bp.level1;
        this.level2 = bp.level2;
        this.seq = bp.seq;
    }
    
    public BP(long level0, long level1, long level2) {
        this.level0 = level0;
        this.level1 = level1;
        this.level2 = level2;
    }
    
    public void setSeq(int seq){
        this.seq = seq;
    }

    public void resetSeq(){
        this.seq = 0;
    }

    public void increaseSeq(){
        this.seq++;
    }

    public void decreaseSeq(){
        this.seq--;
    }

    // encode a BP to string
    // level0,level1,level2,seq|
    public String encodeString(){
        return String.valueOf(level0)+","+String.valueOf(level1)+","+String.valueOf(level2)+","+String.valueOf(seq);
    }
    
    public int decodeString(char[] stream, int off){
        String tmpStr = new String();
        int initOff = off;
        try{
            // read level0
            while (stream[off]!=','){
                tmpStr += String.valueOf(stream[off]);
                off++;
            }
            level0 = Long.parseLong(tmpStr.trim());
            off++; // skip ","
            // read level1
            tmpStr = "";
            while (stream[off]!=','){
                tmpStr += String.valueOf(stream[off]);
                off++;
            }
            level1 = Long.parseLong(tmpStr.trim());
            off++; // skip ","
            // read level2
            tmpStr = "";
            while (stream[off]!=','){
                tmpStr += String.valueOf(stream[off]);
                off++;
            }
            level2 = Long.parseLong(tmpStr.trim());
            off++; // skip ","
            // read seq
            tmpStr = "";
            while (stream[off]!='|'){
                tmpStr += String.valueOf(stream[off]);
                off++;
            }
            seq = Integer.parseInt(tmpStr.trim());
            off++; // skip "|,"
         }catch(Exception e){
             e.printStackTrace();
             return -1;
         }
         return off-initOff;
   }
}

    
