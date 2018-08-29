/**
 * @(#)SessionLeader.java	0.01 11/05/17
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_main;

import fydb.fy_comm.Debuger;
import fydb.fy_comm.InitParas;
import fydb.fy_comm.Tracer;

import fydb.fy_data.Beeper;

import java.io.File;
import java.io.FileInputStream;

import java.net.InetAddress;
import java.net.Socket;

import java.security.KeyStore;
import java.security.SecureRandom;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.TrustManagerFactory;

public class SessionLeader extends Thread{
    protected InitParas paras;
    protected Tracer dtrace;
    protected Debuger debuger;
    protected Beeper beeper;
    private Map dataSets;
    private Manager db;

    private Map sessions;
    private int sessionNumber = 0;
    private SSLServerSocket gateSocket;
    private boolean initialized = false;
    private boolean running = true;
    private boolean sleeping = false;

    public SessionLeader(Map dataSets, InitParas paras, Tracer dtrace,
                         Debuger debuger, Beeper beeper, Manager db) {
        this.dataSets = dataSets;
        this.paras = paras;
        this.dtrace = dtrace;
        this.debuger = debuger;
        this.beeper = beeper;
        this.db = db;

        sessions = new HashMap();
    }

    // build a ssl server socket as server gate
    private SSLServerSocket buildServerGate() {
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
            .load(new FileInputStream((String)paras.getParameter("baseDir") + File.separator + (String)paras.getParameter("dbKeyFile")),
                          ((String)paras.getParameter("dbPassword")).toCharArray());
            trustKeyStore
            .load(new FileInputStream((String)paras.getParameter("baseDir") + File.separator +
                                                   (String)paras.getParameter("dbTrustFile")),
                               ((String)paras.getParameter("dbPassword")).toCharArray());

            keyManager.init(keyStore, ((String)paras.getParameter("dbPassword")).toCharArray());
            trustManager.init(trustKeyStore);

            sslContext
            .init(keyManager.getKeyManagers(), trustManager.getTrustManagers(),
                            new SecureRandom());

            SSLServerSocket sslServerGate =
                (SSLServerSocket)sslContext.getServerSocketFactory()
                .createServerSocket((Integer)paras.getParameter("dbGatePort"), 50, (InetAddress)paras.getParameter("_dbHostAddr"));
            sslServerGate.setNeedClientAuth(true); // require authorization

            return sslServerGate;
        } catch (Exception e) {
            dtrace.trace(2004);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return null;
        }
    }

    // initialize the session leader
    public boolean initialize(){
        if (initialized)
            return true;
        gateSocket = buildServerGate();
        if (gateSocket == null)
            return false;
        initialized = true;
        return initialized;
    }

    // allocate a new session, if allocate a local session, sessionSocket is null
    private SessionServer allocateSession(Socket sessionSocket) {
        if (!initialized){
            dtrace.trace(35);
            return null;
        }

        if (sessionNumber < (Integer)paras.getParameter("maxSessionNumber")) {
            sessionNumber++;
            SessionServer newSession;
            if (sessionSocket == null) {// allocate a local session
                newSession =
                    new SessionServer(sessionNumber, dataSets, paras, dtrace,
                                               debuger, beeper, db);
            } else { // allocate a server session
                try {
                    newSession =
                        new SessionServer(sessionNumber, dataSets, paras,
                                                   dtrace, debuger, beeper,
                                                   sessionSocket, db);
                    newSession.start();
                } catch (Exception e) {
                    dtrace.trace(2005);
                    if (debuger.isDebugMode())
                        e.printStackTrace();
                    return null;
                }
            }
            sessions.put(sessionNumber, newSession);
            return newSession;
        } else {
            dtrace.trace(34);
            return null;
        }
    }

    // allocate a local session
    public SessionServer allocateSession() {
        return allocateSession(null);
    }

    // dellocate a session
    public void dellocateSession(int sessionNumber) {
        if (!initialized){
            dtrace.trace(35);
            return;
        }
        SessionServer session = (SessionServer)sessions.get(sessionNumber);
        if (session != null)
            session.release();
        sessions.remove(sessionNumber);
    }

    private void recycleSessions(){
        Iterator it = sessions.values().iterator();
        while (it.hasNext()){
            SessionServer session = (SessionServer)it.next();
            if (session.isClosed())
                sessions.remove(new Integer(session.getSessionID()));
        }
    }
    
    public void interrupt() {
        running = false;
        super.interrupt();
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
        int recycleTimeCounter = 0;
        while (running) {
            try {
                Socket sessionSocket = gateSocket.accept();
                allocateSession(sessionSocket);
                recycleTimeCounter++;
                if (recycleTimeCounter >= (Integer)paras.getParameter("_recycleSessionInterval")){
                    recycleSessions();
                    recycleTimeCounter = 0;
                }
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
        }
        sleeping = false;
    }
}
