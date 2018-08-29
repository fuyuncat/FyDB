/**
 * @(#)Beeper.java	0.01 11/04/19
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_data;

import fydb.fy_comm.InitParas;

import fydb.fy_comm.Tracer;

//import fydb.fy_data.CommunicationClient;
//import fydb.fy_data.ExCommunicator;
//import fydb.fy_data.SysMetaData;

import java.io.Serializable;

import java.util.ArrayList;
import java.util.Comparator;
//import java.util.Date;
//import java.util.UUID;

public final class Beeper{
    protected InitParas paras; // = new InitParas();
    protected Tracer dtrace; // = new Tracer();
    private ExCommunicator communicator;

    protected static long level0;
    protected static long level1;
    protected static long level2;
    
    //private static boolean initialized = false;
    private static ArrayList queue;
    private byte[] lock = new byte[0]; // used for synchronize

    public static class BPComparator implements Comparator <BP>, Serializable {
        public int compare(BP b1, BP b2) {
            if (b1 == null && b2 == null)
                return 0;
            else if (b1 == null)
                return -1;
            else if (b2 == null)
                return 1;
            if (b1.level0 < b2.level0)
                return -1;
            else if ((b1.level0 > b2.level0))
                return 1;
            else {
                if (b1.level1 < b2.level1)
                    return -1;
                else if ((b1.level1 > b2.level1))
                    return 1;
                else {
                    if (b1.level2 < b2.level2)
                        return -1;
                    else if ((b1.level2 > b2.level2))
                        return 1;
                    else {
                        if (b1.seq < b2.seq)
                            return -1;
                        else if ((b1.seq > b2.seq))
                            return 1;
                        else {
                            return 0;
                        }
                    }
                }
            }
        }
    } 

    // a revsersed comparator
    public static class BPReverseComparator implements Comparator <BP>, Serializable {
        public int compare(BP b1, BP b2) {
            if (b1 == null && b2 == null)
                return 0;
            else if (b1 == null)
                return 1;
            else if (b2 == null)
                return -1;
            if (b1.level0 < b2.level0)
                return 1;
            else if ((b1.level0 > b2.level0))
                return -1;
            else {
                if (b1.level1 < b2.level1)
                    return 1;
                else if ((b1.level1 > b2.level1))
                    return -1;
                else {
                    if (b1.level2 < b2.level2)
                        return 1;
                    else if ((b1.level2 > b2.level2))
                        return -1;
                    else {
                        if (b1.seq < b2.seq)
                            return 1;
                        else if ((b1.seq > b2.seq))
                            return -1;
                        else {
                            return 0;
                        }
                    }
                }
            }
        }
    } 

    public Beeper(InitParas paras, Tracer dtrace, ExCommunicator communicator) {
        this.paras = paras;
        this.dtrace = dtrace;
        this.communicator = communicator;
        /*if (initialized)
            return;
        if (level2 == 0)
        {
            Date date = new Date();
            level2 = date.getTime();
            level1 = 0;
            level0 = 0;
        }
        if (queue == null)
            queue = new ArrayList();

        initialized = true;
         */
    }
    
    public Beeper(BP lastBp, InitParas paras, Tracer dtrace, ExCommunicator communicator) {
        this(paras, dtrace, communicator);
        if (lastBp != null){
            level2 = lastBp.level2;
            level1 = lastBp.level1;
            level0 = lastBp.level0;
        }else{
            level0 = 0;
            level1 = 0;
            level2 = 0;
        }
    }
    
    public static BPComparator getComparator(){
        return new BPComparator();
    }
    
    public static BPReverseComparator getReverseComparator(){
        return new BPReverseComparator();
    }
    
    // b1 < b2 return -1, b1==b2 return 0, b1 > b2 return 1
    public int compareBP(BP b1, BP b2, boolean compareSeq) {
        if (b1.level0 < b2.level0)
            return -1;
        else if ((b1.level0 > b2.level0))
            return 1;
        else {
            if (b1.level1 < b2.level1)
                return -1;
            else if ((b1.level1 > b2.level1))
                return 1;
            else {
                if (b1.level2 < b2.level2)
                    return -1;
                else if ((b1.level2 > b2.level2))
                    return 1;
                else if (compareSeq){
                    if (b1.seq < b2.seq)
                        return -1;
                    else if ((b1.seq > b2.seq))
                        return 1;
                    else {
                        return 0;
                    }
                }
                else
                    return 0;
            }
        }
    }

    //generate a new bp
    public BP getBP() {
        /*// enqueue method to void concurrent call        
        UUID uuid = UUID.randomUUID();
        queue.add(uuid);
        while (!uuid.equals(queue.get(0))) // enqueue
        {
            try{
                Thread.sleep(paras._bpSpinTime);
            }
            catch (InterruptedException e) {
                dtrace.trace(e.getMessage());
                dtrace.trace(13);
                queue.remove(uuid);
                return null;
            }
        }
        //*/
        BP bp = null;
        synchronized(lock){
            if (communicator.hasRemoteBeeper()){
                CommunicationClient client = communicator.getBeeperHost();
                if (client!=null && !client.isClosed())
                    bp = client.getBP();
            }
            if (bp == null){
                level2++;
                if (level2 >= 281474976710656L) // 2^48
                {
                    level1++;
                    if (level1 >= 281474976710656L) // 2^48
                    {
                        level0++;
                        level1 = 1;
                    }
                    level2 = 1;
                }
                bp = new BP(level0, level1, level2);
            }
        }
        SysMetaData.assignLastBp(bp);
        // queue.remove(uuid); // dequeue
        return bp;
    }

    // get current generated BP
    public BP getCurBP() {
        BP bp = null;
        if (communicator.hasRemoteBeeper()){
            CommunicationClient client = communicator.getBeeperHost();
            if (client!=null && !client.isClosed())
                bp = client.getCurBP();
        }
        if (bp == null)
            bp = new BP(level0, level1, level2);
        return bp;
    }
    
    // detect the next value of a bp
    public BP detectNextBP(BP baseBP){
        baseBP.level2++;
        if (baseBP.level2 >= 281474976710656L) // 2^48
        {
            baseBP.level1++;
            if (baseBP.level1 >= 281474976710656L) // 2^48
            {
                baseBP.level0++;
                baseBP.level1 = 1;
            }
            baseBP.level2 = 1;
        }
        BP bp = new BP(baseBP.level0, baseBP.level1, baseBP.level2);
        return bp;
    }

    // b1 < b2 return -1, b1==b2 return 0, b1 > b2 return 1
    public int compare(BP b1, BP b2) {
        if (b1.level0 < b2.level0)
            return -1;
        else if ((b1.level0 > b2.level0))
            return 1;
        else {
            if (b1.level1 < b2.level1)
                return -1;
            else if ((b1.level1 > b2.level1))
                return 1;
            else {
                if (b1.level2 < b2.level2)
                    return -1;
                else if ((b1.level2 > b2.level2))
                    return 1;
                else {
                    return 0;
                }
            }
        }
    }
}
