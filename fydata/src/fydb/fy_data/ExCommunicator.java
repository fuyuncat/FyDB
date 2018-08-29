/*
 * @(#)ExCommunicator.java	0.01 11/06/22
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_data;

import fydb.fy_comm.CommUtility;
import fydb.fy_comm.Debuger;
import fydb.fy_comm.FyMetaData;
import fydb.fy_comm.InitParas;
import fydb.fy_comm.Tracer;

import fydb.fy_main.Manager;

import java.io.File;
import java.io.FileInputStream;

import java.io.IOException;

import java.net.InetAddress;
import java.net.Socket;

import java.net.UnknownHostException;

import java.security.KeyStore;
import java.security.SecureRandom;

import java.util.HashMap;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import java.util.Set;

import java.util.TreeMap;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

public class ExCommunicator extends Thread {
    private HashMap servers;   // IP Address:server
    private HashMap clients;   // IP Address:server

    private SSLServerSocket serverSocket;
    private boolean initialized =false;
    private boolean running = true;
    private boolean sleeping = false;
    private Map dataSets;
    private Manager db;
    private CommunicationClient beeperServer;
    
    protected InitParas paras;
    protected Tracer dtrace;
    protected Debuger debuger;

    public ExCommunicator(InitParas paras, Tracer dtrace, Debuger debuger, Map dataSets, Manager db) {
        this.paras = paras;
        this.dtrace = dtrace;
        this.debuger = debuger;

        this.servers = new HashMap();
        this.clients = new HashMap();
        this.dataSets = dataSets;
        this.db = db;
    }

    public SSLServerSocket buildServerGate(){
        try {
            //SSLContext ctx = SSLContext.getInstance("TLS"); // get tls context
            SSLContext sslContext = SSLContext.getInstance("SSLv3"); // get tls context

            KeyManagerFactory keyManager =
                KeyManagerFactory.getInstance("SunX509");
            TrustManagerFactory trustManager =
                TrustManagerFactory.getInstance("SunX509");

            KeyStore keyStore = KeyStore.getInstance("JKS");
            KeyStore trustKeyStore = KeyStore.getInstance("JKS");

            //load keystore
            keyStore
            .load(new FileInputStream((String)paras.getParameter("baseDir") + File.separator + (String)paras.getParameter("imKeyFile")),
                          ((String)paras.getParameter("imPassword")).toCharArray());
            trustKeyStore
            .load(new FileInputStream((String)paras.getParameter("baseDir") + File.separator +
                                                   (String)paras.getParameter("imTrustFile")),
                               ((String)paras.getParameter("imPassword")).toCharArray());

            keyManager.init(keyStore, ((String)paras.getParameter("imPassword")).toCharArray());
            trustManager.init(trustKeyStore);

            sslContext
            .init(keyManager.getKeyManagers(), trustManager.getTrustManagers(),
                            new SecureRandom());

            SSLServerSocket sslServerGate =
                (SSLServerSocket)sslContext.getServerSocketFactory()
                .createServerSocket((Integer)paras.getParameter("imGatePort"),50,(InetAddress)paras.getParameter("_imHostAddr"));
            sslServerGate.setNeedClientAuth(true); // require authorization

            return sslServerGate;
        } catch (Exception e) {
            dtrace.trace(2009);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return null;
        }
    }

    // initialize the session leader
    public boolean initialize(){
        if (initialized)
            return true;
        serverSocket = buildServerGate();
        if (serverSocket == null)
            return false;
        initialized = true;
        return initialized;
    }
    
    public Beeper getLocalBeeper(){
        return db.getBeeper();
    }

    public boolean isMaster(String guid){
        return db.isMaster(guid);
    }

    public boolean handoverMaster(String guid){
        return db.handoverMaster(guid);
    }

    public boolean sychronizeLogs(String guid, TreeMap curLogs){
        return db.sychronizeLogs(guid, curLogs);
    }

    public FyMetaData getMetaData(String guid){
        return db.getMetaData(guid);
    }

    public HashMap getDataProps(String guid){
        return db.getDataProps(guid);
    }

    public boolean beginFetch(String guid, String clientAddr){
        return db.beginFetch(guid, clientAddr);
    }

    public HashMap batchFetch(String guid, int batchSize, String clientAddr){
        return db.batchFetch(guid, batchSize, clientAddr);
    }

    // decide the beeper host. We choose the 1st lauched host as beeper host
    public CommunicationClient getBeeperHost(){
        if (beeperServer!=null && !beeperServer.isClosed())
            return beeperServer;
        Iterator it = clients.keySet().iterator();
        beeperServer = null;
        boolean foundBeeper = false;
        while (it.hasNext()){
            String hostAddr = (String)it.next();
            CommunicationClient client = (CommunicationClient)clients.get(hostAddr);
            if (client.isBeeperHost()){
                if (!foundBeeper){
                    beeperServer = client;
                    foundBeeper = true;
                }else { // duplicated beeper
                    client.handoverBeeper();
                }
            }
        }
        TokenInformation.setBeeperHost(beeperServer == null);
        return beeperServer;
    }
    
    public boolean hasRemoteBeeper(){
        return (beeperServer == null || beeperServer.isClosed());
    }

    // connect to a buddy
    private boolean connectToServer(InetAddress serverAddr){
        try {
            CommunicationClient client = (CommunicationClient)clients.get(serverAddr.getHostAddress());
            if (client != null && !client.isClosed()){
                //dtrace.trace(2010);
                return false;
            }
            debuger.printMsg(serverAddr.toString(),false);
            dtrace.trace("connecting to buddy "+serverAddr.getHostName());
            SSLContext sslContext = SSLContext.getInstance("SSLv3");
            KeyManagerFactory keyManager = KeyManagerFactory.getInstance("SunX509");
            TrustManagerFactory trustManager = TrustManagerFactory.getInstance("SunX509");
            KeyStore keyStore = KeyStore.getInstance("JKS");
            KeyStore trustKeyStore = KeyStore.getInstance("JKS");

            keyStore
            .load(new FileInputStream((String)paras.getParameter("baseDir") + File.separator + (String)paras.getParameter("dbKeyFile")),
                          ((String)paras.getParameter("imPassword")).toCharArray());
            trustKeyStore
            .load(new FileInputStream((String)paras.getParameter("baseDir") + File.separator + (String)paras.getParameter("dbTrustFile")),
                               ((String)paras.getParameter("imPassword")).toCharArray());
    
            keyManager.init(keyStore, ((String)paras.getParameter("imPassword")).toCharArray());
            trustManager.init(trustKeyStore);
            sslContext.init(keyManager.getKeyManagers(), trustManager.getTrustManagers(), new SecureRandom());
            SSLSocketFactory gateSocketFactory = sslContext.getSocketFactory();
            Socket gateSocket = gateSocketFactory.createSocket(serverAddr, (Integer)paras.getParameter("imGatePort"));
            client = new CommunicationClient(paras, dtrace, debuger, gateSocket);
            if (!client.isClosed()){
                clients.put(serverAddr.getHostAddress(), client);
                dtrace.trace(serverAddr.getHostName()+" connected.");
                return true;
            }else
                return false;
        } catch (Exception e) {
            dtrace.trace(serverAddr.getHostName());
            dtrace.trace(2013);
            //e.printStackTrace();
            return false;
        }
    }
    
    // connect to a buddy
    private boolean connectToServer(String serverName){
        try {
            if (isMyself(serverName))
                return true;
            InetAddress serverAddr =  InetAddress.getByName(serverName);
            return connectToServer(serverAddr);
        } catch (Exception e) {
            dtrace.trace(serverName);
            dtrace.trace(2013);
            //e.printStackTrace();
            return false;
        }
    }

    public String[] sychronizeServers(){
        HashSet serverAddrs = new HashSet(clients.keySet());
        String[] newAddrs = new String[0];
        //debuger.printMsg("synchronize to clients: " +serverAddrs.toString(),false);
        serverAddrs.addAll(servers.keySet());
        for (Object client: clients.values()){
            HashSet sychServers = ((CommunicationClient)client).synchronizeServers(serverAddrs);
            //debuger.printMsg("synchronized from client: " + sychServers.toString(),false);
            for (Object addr: sychServers){
                if (!serverAddrs.contains((String)addr)){
                    if (connectToServer((String)addr)){
                        serverAddrs.add((String)addr);
                        newAddrs = (String[])CommUtility.appendToArray(newAddrs, (String)addr);
                    }
                }
            }
        }
        return newAddrs;
    }

    private boolean isMyself(InetAddress serverAddr){
        try {
            for (InetAddress myAddr: InetAddress.getAllByName(InetAddress.getLocalHost().getHostName()))
                if (myAddr.equals(serverAddr))
                    return true;
        }catch(UnknownHostException e){
            return false;
        }
        return false;
    }

    private boolean isMyself(String serverName){
        try {
            for (InetAddress remoteAddr: InetAddress.getAllByName(serverName))
                for (InetAddress myAddr: InetAddress.getAllByName(InetAddress.getLocalHost().getHostName()))
                    if (myAddr.equals(remoteAddr))
                        return true;
        }catch(UnknownHostException e){
            return false;
        }
        return false;
    }

    // connect to buddies
    // return connected number.
    public int connectToServers(String[] serverHosts){
        int connected = 0;
        for (int i=0;i<serverHosts.length;i++)
            if (!isMyself(serverHosts[i]) && connectToServer(serverHosts[i]))
                connected++;
        return connected;
    }
    
    // connect to buddies
    // return connected number.
    public int connectToServers(InetAddress[] serverHosts){
        int connected = 0;
        try {
            InetAddress localAddr = InetAddress.getLocalHost();
            for (int i=0;i<serverHosts.length;i++)
                if (!serverHosts[i].equals(localAddr) && connectToServer(serverHosts[i]))
                    connected++;
        }catch(UnknownHostException e){
            return connected;
        }
        return connected;
    }
    
    public HashMap getServers(){
        return servers;
    }

    public HashMap getClients(){
        return clients;
    }
    
    public void interrupt(){
        running = false;
        // disconnect all sessions
        Iterator it = servers.values().iterator();
        while(it.hasNext()){
            CommunicationServer server = (CommunicationServer)it.next();
            server.terminate();
        }
        servers.clear();
        it = clients.values().iterator();
        while(it.hasNext()){
            CommunicationClient client = (CommunicationClient)it.next();
            client.terminate();
        }
        clients.clear();
        super.interrupt();
        try{
            serverSocket.close();
        }catch(IOException e){
        }
    }

    public void terminate(){
        running = false;
        while (sleeping){
            try{
                sleep(1);
            }catch(InterruptedException e){
                interrupt();
            }
        }
        interrupt();
    }

    // override start(), require to check initialize status
    public void start(){
        if (!initialized){
            dtrace.trace(35);
            return;
        }
        if (!super.isAlive())
            super.start();
    }

    public void run(){
        while (running) {
            try {
                Socket connSocket = serverSocket.accept();
                InetAddress serverAddr = connSocket.getInetAddress();
                if (!isMyself(serverAddr)){
                    dtrace.trace("recieve new buddy "+serverAddr.getHostName());
                    String ipAddress = serverAddr.getHostAddress();
                    CommunicationServer server = (CommunicationServer)servers.get(ipAddress);
                    if (server == null || !server.isAlive() || server.isClosed()) {
                        server = new CommunicationServer(paras, dtrace, debuger, connSocket, dataSets, ipAddress, this);
                    }
                    CommunicationClient client = (CommunicationClient)clients.get(serverAddr.getHostAddress());
                    if (client == null || client.isClosed()) 
                        connectToServer(serverAddr);
                    if (!server.isClosed()){
                        dtrace.trace("buddy accepted.");
                        server.start();
                        servers.put(serverAddr.getHostAddress(), server);
                    }
                }
                try {
                    sleeping = true;
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    dtrace.trace(36);
                } finally {
                    sleeping = false;
                }
            } catch (IOException e) {
                dtrace.trace(2015);
                if (debuger.isDebugMode())
                    e.printStackTrace();
            } catch (Exception e) {
                dtrace.trace(10);
                if (debuger.isDebugMode())
                    e.printStackTrace();
            } finally {
                sleeping = false;
            }
        }
        sleeping = false;
    }
}
