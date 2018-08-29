/**
 * @(#)LogLeader.java	0.01 11/05/16
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_data;

import fydb.fy_comm.Debuger;
import fydb.fy_comm.InitParas;
import fydb.fy_comm.Tracer;

import fydb.fy_data.FyDataSet;

import java.util.Iterator;
import java.util.Map;

public class LogLeader extends Thread {
    protected InitParas paras;
    protected Tracer dtrace;
    protected Debuger debuger;
    protected Beeper beeper;
    private Map dataSets;
    private SysMetaData sysMetaData;

    private boolean logging = true;
    private boolean paused = false;
    private boolean sleeping = false;

    public LogLeader(Map dataSets, InitParas paras, Tracer dtrace,
                     Debuger debuger, Beeper beeper, SysMetaData sysMetaData) {
        this.dataSets = dataSets;
        this.paras = paras;
        this.dtrace = dtrace;
        this.debuger = debuger;
        this.beeper = beeper;
        this.sysMetaData = sysMetaData;
        setName("LogLeader");
    }
    
    public void interrupt(){
        logging = false;
        super.interrupt();
    }

    public void terminate() {
        logging = false;
        while (sleeping){
            try{
                sleep(1);
            }catch(InterruptedException e){
                interrupt();
            }
        }
        interrupt();
    }

    public boolean setPause() {
        paused = !paused;
        return paused;
    }

    public void run() {
        do {
            try {
                sysMetaData.saveMetaData();
                // implentment logs into physical data source 
                if (!paused && dataSets != null) {
                    Iterator it = dataSets.values().iterator();
                    while (it.hasNext()) {
                        FyDataSet dataSet = (FyDataSet)it.next();
                        dataSet.implementLogs(dataSet.getLastAppliableBp());
                    }
                }
                //debuger.printMsg("log writer heart beat!", true);

                try {
                    sleeping = true;
                    sleep((Integer)paras.getParameter("_logApplyInteval"));
                } catch (InterruptedException e) {
                    dtrace.trace(28);
                }finally{
                    sleeping = false;
                }
            } catch (Exception e) {
                dtrace.trace(29);
                if (debuger.isDebugMode())
                    e.printStackTrace();
            }finally{
                sleeping = false;
            }
        } while (logging);
        sleeping = false;
    }
}
