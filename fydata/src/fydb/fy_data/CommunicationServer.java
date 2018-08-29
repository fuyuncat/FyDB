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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class CommunicationServer extends NetThread{
    private boolean isClosed = false;
    private Map dataSets;
    private ExCommunicator communicator;

    private String clientAddr; // address of client connecting to this server

    public CommunicationServer(InitParas paras, Tracer dtrace, Debuger debuger, Socket socket, Map dataSets, String clientAddr, ExCommunicator communicator) {
        super(paras, dtrace, debuger);
        this.isClosed = !super.setServerSocket(socket);

        this.paras = paras;
        //this.dtrace = dtrace;
        dtrace = new Tracer(false);
        this.debuger = debuger;
        this.dataSets = dataSets;
        this.communicator = communicator;
        this.clientAddr = clientAddr;

        setName("CommunicateChannel");
    }
    
    public void release() {
        terminate();
        super.closeSocket();
        isClosed = true;
    }

    public boolean isClosed(){
        return super.isClosed();
        //return isClosed;
    }

    private boolean isBeeperHost(){
        return TokenInformation.isBeeperHost;
    }

    private boolean handoverBeeper(){
        TokenInformation.setBeeperHost(false);
        return true;
    }

    private HashSet synchronizeServers(HashSet serverAddrs){
        String[] serverHosts = new String[serverAddrs.size()];
        serverHosts = (String[])serverAddrs.toArray(serverHosts);
        //debuger.printMsg("synchronized from client: " +serverAddrs.toString(),false);
        communicator.connectToServers(serverHosts);
        //debuger.printMsg("synchronized to client: " + communicator.getClients().keySet().toString(),false);
        return new HashSet(communicator.getClients().keySet());
    }

    private BP getBP(){
        return communicator.getLocalBeeper().getBP();
    }

    private BP getCurBP(){
        return communicator.getLocalBeeper().getCurBP();
    }

    private boolean isMaster(String guid){
        return communicator.isMaster(guid);
    }

    private boolean handoverMaster(String guid){
        return communicator.handoverMaster(guid);
    }

    private boolean sychronizeLogs(String guid, TreeMap curLogs){
        if (curLogs == null || curLogs.size() == 0)
            return true;
        return communicator.sychronizeLogs(guid, curLogs);
    }

    private FyMetaData getMetaData(String guid){
        return communicator.getMetaData(guid);
    }

    private HashMap getDataProps(String guid){
        return communicator.getDataProps(guid);
    }

    private boolean beginFetch(String guid){
        return communicator.beginFetch(guid, clientAddr);
    }

    private HashMap batchFetch(String guid, int batchSize){
        return communicator.batchFetch(guid,batchSize,clientAddr);
    }

    private void responseClient(){
        if (isClosed()){
            isClosed = true;
            terminate();
            return;
        }
        int msgType = CommUtility.byteArrayToInt(readBytes());
        if (msgType == 0)
            return;
        switch(msgType){
            case Consts.ISBEEPERHOST:{
                writeBytes(isBeeperHost()?new byte[]{0x01}:new byte[]{(byte)0xFF});
                break;
            }
            case Consts.HANDOVERBEEPER:{
                writeBytes(handoverBeeper()?new byte[]{0x01}:new byte[]{(byte)0xFF});
                break;
            }
            case Consts.SYNCHRONIZESERVERS:{
                HashSet serverNames = (HashSet)readObject();
                writeObject(synchronizeServers(serverNames));
                break;
            }
            case Consts.GETBP:{
                writeObject(getBP());
                break;
            }
            case Consts.GETCURBP:{
                writeObject(getCurBP());
                break;
            }
            case Consts.ISMASTER:{
                String guid = (String)readObject();
                writeBytes(isMaster(guid)?new byte[]{0x01}:new byte[]{(byte)0xFF});
                break;
            }
            case Consts.HANDOVERMASTER:{
                String guid = (String)readObject();
                writeBytes(handoverMaster(guid)?new byte[]{0x01}:new byte[]{(byte)0xFF});
                break;
            }
            case Consts.SYNCHRONIZELOGS:{
                String guid = (String)readObject();
                TreeMap curLogs = (TreeMap)readObject();
                writeBytes(sychronizeLogs(guid, curLogs)?new byte[]{0x01}:new byte[]{(byte)0xFF});
                break;
            }
            case Consts.GETMETADATA:{
                String guid = (String)readObject();
                writeObject(getMetaData(guid));
                break;
            }
            case Consts.GETDATAPROPS:{
                String guid = (String)readObject();
                writeObject(getDataProps(guid));
                break;
            }
            case Consts.BEGINFETCH:{
                String guid = (String)readObject();
                writeBytes(beginFetch(guid)?new byte[]{0x01}:new byte[]{(byte)0xFF});
                break;
            }
            case Consts.BATCHFETCH:{
                String guid = (String)readObject();
                int batchSize = CommUtility.byteArrayToInt(readBytes());
                writeObject(batchFetch(guid,batchSize));
                break;
            }
            case Consts.BYEBYE:{ // byebye
                super.closeSocket();
                terminate();
                break;
            }
        }
    }

    public void start() {
        running = true;
        if (!super.isAlive())
            super.start();
    }

    public void run() {
        do {
            if (isClosed()){
                isClosed = true;
                terminate();
            }
            try {
                //if (!requireChannel())
                //    continue;
                responseClient();
                //releaseChannel();
                try {
                    sleeping = true;
                    Thread.sleep(1);
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
