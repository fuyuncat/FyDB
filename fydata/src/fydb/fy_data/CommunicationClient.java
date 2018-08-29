/*
 * @(#)CommunicationServer.java	0.01 11/06/23
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_data;

import fydb.fy_comm.CommUtility;
import fydb.fy_comm.Consts;
import fydb.fy_comm.Debuger;
import fydb.fy_comm.FyMetaData;
import fydb.fy_comm.InitParas;
import fydb.fy_comm.NetThread;
import fydb.fy_comm.Tracer;

import java.net.Socket;

import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;

public class CommunicationClient extends NetThread {
    private boolean isClosed = false;

    public CommunicationClient(InitParas paras, Tracer dtrace, Debuger debuger, Socket clientSocket) {
        super(paras, dtrace, debuger);
        this.isClosed = !super.setClientSocket(clientSocket);

        this.paras = paras;
        this.dtrace = dtrace;
        this.debuger = debuger;

        setName("CommunicationClient");
    }

    public boolean isClosed(){
        return super.isClosed();
        //return isClosed;
    }

    public void release() {
        int msgType = Consts.BYEBYE;
        if (!requireChannel())
            return;
        writeBytes(CommUtility.intToByteArray(msgType));
        releaseChannel();
        terminate();
        super.closeSocket();
        isClosed = true;
    }

    public boolean isBeeperHost(){
        if (isClosed())
            return false;
        int msgType = Consts.ISBEEPERHOST;
        if (!requireChannel())
            return false;
        writeBytes(CommUtility.intToByteArray(msgType));
        byte[] rest = readBytes();
        releaseChannel();
        return rest!=null&&rest[0]==0x01;
    }

    public boolean handoverBeeper(){
        if (isClosed())
            return false;
        int msgType = Consts.HANDOVERBEEPER;
        if (!requireChannel())
            return false;
        writeBytes(CommUtility.intToByteArray(msgType));
        byte[] rest = readBytes();
        releaseChannel();
        return rest!=null&&rest[0]==0x01;
    }

    public HashSet synchronizeServers(HashSet serverNames){
        if (isClosed())
            return new HashSet();
        int msgType = Consts.SYNCHRONIZESERVERS;
        if (!requireChannel())
            return new HashSet();;
        writeBytes(CommUtility.intToByteArray(msgType));
        writeObject(serverNames);
        HashSet result = (HashSet)readObject();
        releaseChannel();
        return result;
    }

    public BP getBP(){
        if (isClosed())
            return null;
        int msgType = Consts.GETBP;
        if (!requireChannel())
            return null;
        writeBytes(CommUtility.intToByteArray(msgType));
        BP bp = (BP)readObject();
        releaseChannel();
        return bp;
    }

    public BP getCurBP(){
        if (isClosed())
            return null;
        int msgType = Consts.GETCURBP;
        if (!requireChannel())
            return null;
        writeBytes(CommUtility.intToByteArray(msgType));
        BP bp = (BP)readObject();
        releaseChannel();
        return bp;
    }

    public boolean isMaster(String guid){
        if (isClosed())
            return false;
        int msgType = Consts.ISMASTER;
        if (!requireChannel())
            return false;
        writeBytes(CommUtility.intToByteArray(msgType));
        writeObject(guid);
        byte[] rest = readBytes();
        releaseChannel();
        return rest!=null&&rest[0]==0x01;
    }

    public boolean handoverMaster(String guid){
        if (isClosed())
            return false;
        int msgType = Consts.HANDOVERMASTER;
        if (!requireChannel())
            return false;
        writeBytes(CommUtility.intToByteArray(msgType));
        writeObject(guid);
        byte[] rest = readBytes();
        releaseChannel();
        return rest!=null&&rest[0]==0x01;
    }

    public boolean sychronizeLogs(String guid, TreeMap curLogs){
        if (isClosed())
            return false;
        int msgType = Consts.SYNCHRONIZELOGS;
        if (!requireChannel())
            return false;
        writeBytes(CommUtility.intToByteArray(msgType));
        writeObject(guid);
        writeObject(curLogs);
        byte[] rest = readBytes();
        releaseChannel();
        return rest!=null&&rest[0]==0x01;
    }

    public FyMetaData getMetaData(String guid){
        if (isClosed())
            return null;
        int msgType = Consts.GETMETADATA;
        if (!requireChannel())
            return null;
        writeBytes(CommUtility.intToByteArray(msgType));
        writeObject(guid);
        FyMetaData metaData = (FyMetaData)readObject();
        releaseChannel();
        return metaData;
    }
    
    public HashMap getDataProps(String guid){
        if (isClosed())
            return null;
        int msgType = Consts.GETDATAPROPS;
        if (!requireChannel())
            return null;
        writeBytes(CommUtility.intToByteArray(msgType));
        writeObject(guid);
        HashMap dataProps = (HashMap)readObject();
        releaseChannel();
        return dataProps;
    }
    
    public boolean beginFetch(String guid){
        if (isClosed())
            return false;
        int msgType = Consts.BEGINFETCH;
        if (!requireChannel())
            return false;
        writeBytes(CommUtility.intToByteArray(msgType));
        writeObject(guid);
        byte[] rest = readBytes();
        releaseChannel();
        return rest!=null&&rest[0]==0x01;
    }

    public HashMap batchFetch(String guid, int batchSize){
        if (isClosed())
            return new HashMap();
        int msgType = Consts.BATCHFETCH;
        if (!requireChannel())
            return new HashMap();
        writeBytes(CommUtility.intToByteArray(msgType));
        writeObject(guid);
        writeBytes(CommUtility.intToByteArray(batchSize));
        HashMap fetchDatas = (HashMap)readObject();
        releaseChannel();
        return fetchDatas;
    }

    public void start() {
        if (running && super.isAlive()){
            dtrace.trace(5004);
            return;
        }
        running = true;
        if (!super.isAlive())
            super.start();
    }

    public void run() {
        do {
            try {
                try {
                    sleeping = true;
                    sleep(1);
                } catch (InterruptedException e) {
                    dtrace.trace(36);
                } finally {
                    sleeping = false;
                }
            } catch (Exception e) {
                dtrace.trace(10);
                if (debuger.isDebugMode())
                    e.printStackTrace();
            } finally {
                sleeping = false;
            }
        } while (running);
        sleeping = false;
    }
}
