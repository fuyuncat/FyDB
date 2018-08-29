/**
 * @(#)FyBaseLogEntry.java	0.01 11/05/25
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_data;

import fydb.fy_comm.Tracer;

import java.io.Serializable;

public class FyBaseLogEntry implements Serializable {
    //protected Tracer dtrace; // = new Tracer();

    public int         logType;    // log type -- 1:data; 2:index
    //public MemBaseData logObject;  // pointer of the object

    public FyBaseLogEntry(int logType) {
        this.logType = logType;
        //this.logObject = logObject;
    }
}
