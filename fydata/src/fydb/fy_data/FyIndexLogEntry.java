/**
 * @(#)FyIndexLogEntry.java	0.01 11/05/25
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_data;

import fydb.fy_comm.Consts;
import fydb.fy_comm.Tracer;

import java.io.Serializable;

import java.util.ArrayList;
import java.util.HashMap;

public class FyIndexLogEntry extends FyBaseLogEntry implements Serializable{
    //an indexlogentry base on a single data row.

    public int op;  
    //public int indexType;      // index type
    public int indId;          // unique index id
    public ArrayList datas;    // indexed datasof affected row. it's ArrayList.
    //public ArrayList keyList;// key list
    public HashMap key;        // key of affected row

    public FyIndexLogEntry() {
        super(Consts.INDEX);
    }

    public FyIndexLogEntry(int op, ArrayList datas, HashMap key) {
        this();
        this.op = op;
        //this.indexType = indexType;
        this.datas = datas;
        this.key = key;
    }
}
