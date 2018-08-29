/**
 * @(#)SessionLeader.java	0.01 11/05/17
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_client;

import fydb.fy_comm.Debuger;
import fydb.fy_comm.InitParas;
import fydb.fy_comm.Tracer;

import fydb.fy_client.SessionClient;

import java.io.File;
import java.io.FileInputStream;

import java.net.Socket;

import java.security.KeyStore;
import java.security.SecureRandom;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
//import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

public class SessionLeader{
    public class PoolSession {
        public boolean assigned = false; // if sessions put in pool, this flag indicate if it has been asigned, otherwise meaningless
        public SessionClient session;    //
        
        public PoolSession(){
            this.assigned = false;
            this.session = null;
        }
        
        public PoolSession(SessionClient session, boolean assigned){
            this.session= session;
            this.assigned=assigned;
        }
    }

    protected InitParas paras;
    protected Tracer dtrace;
    protected Debuger debuger;

    private Map sessions;
    private int sessionID = 0;
    private int sessionNum = 0;
    private int maxSessionNumber = 100;  // maximum session number, default is 100
    private String server;               // server address/name
    private int port;                    // server port
    private boolean sessionInPool = false; // if pre-allocate sessions and put them in pool.
    private SSLSocketFactory gateSocketFactory;
    private boolean connected = false;

    public SessionLeader(int maxSessionNumber) {
        this.paras = new InitParas();
        this.dtrace = new Tracer(true);
        this.debuger = new Debuger();
        debuger.setDebugMode("YES".equalsIgnoreCase((String)paras.getParameter("debug")));;
        this.maxSessionNumber = maxSessionNumber;

        sessions = new HashMap();
    }

    public SessionLeader(InitParas paras, Tracer dtrace, Debuger debuger, int maxSessionNumber) {
        this.paras = paras;
        this.dtrace = dtrace;
        this.debuger = debuger;
        this.maxSessionNumber = maxSessionNumber;

        sessions = new HashMap();
    }

    /** 
     * connect to server. If connected successfully, return ture.
     * @param server hostname or ip address of server
     * @param port port of server
     * @param password ssl password of server
    */
    public boolean connectToServer(String server, int port, String password, boolean sessionInPool) {
        connected = false;
        this.server = server;
        this.port = port;
        this.sessionInPool = sessionInPool;
        try {
            SSLContext sslContext = SSLContext.getInstance("SSLv3");
            KeyManagerFactory keyManager = KeyManagerFactory.getInstance("SunX509");
            TrustManagerFactory trustManager = TrustManagerFactory.getInstance("SunX509");
            KeyStore keyStore = KeyStore.getInstance("JKS");
            KeyStore trustKeyStore = KeyStore.getInstance("JKS");

            keyStore
            .load(new FileInputStream((String)paras.getParameter("baseDir") + File.separator + (String)paras.getParameter("dbKeyFile")),
                          password.toCharArray());
            trustKeyStore
            .load(new FileInputStream((String)paras.getParameter("baseDir") + File.separator +(String)paras.getParameter("dbTrustFile")),
                               password.toCharArray());

            keyManager.init(keyStore, password.toCharArray());
            trustManager.init(trustKeyStore);
            sslContext.init(keyManager.getKeyManagers(), trustManager.getTrustManagers(), new SecureRandom());
            gateSocketFactory = sslContext.getSocketFactory();
            connected = true;
            if (sessionInPool){ // in pool mode, pre-allocate sessions and put them in pool
                for (int i=0;i<maxSessionNumber;i++){
                    PoolSession pooledSession = new PoolSession();
                    pooledSession.session = connectSession();
                    pooledSession.assigned = false;
                    sessions.put(sessionID, pooledSession);
                }
            }
        } catch (Exception e) {
            dtrace.trace(2006);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return connected;
        }
        return connected;
    }

    public void disconnect(){
        if (!connected){
            dtrace.trace(5001);
            return;
        }

        connected = false;
        Iterator it = sessions.values().iterator();
        while(it.hasNext()){
            PoolSession pooledSession = (PoolSession)it.next();
            if (pooledSession.session != null)
                pooledSession.session.release();
        }
        sessions.clear();
    }

    // connect a client session to server
    private SessionClient connectSession() {
        if (!connected){
            dtrace.trace(5001);
            return null;
        }

        if (sessionNum < maxSessionNumber) {
            sessionID++;
            sessionNum++;
            SessionClient newSession;
            try {
                //SSLSocket gateSocket = (SSLSocket) gateSocketFactory.createSocket(server, port);
                Socket gateSocket = gateSocketFactory.createSocket(server, port);
                if (gateSocket == null)
                    return null;
                //gateSocket.startHandshake();
                //gateSocket.bind(null);
                newSession = new SessionClient(sessionID, paras, dtrace, debuger, gateSocket);
            } catch (Exception e) {
                dtrace.trace(2007);
                if (debuger.isDebugMode())
                    e.printStackTrace();
                return null;
            }
            return newSession;
        } else {
            dtrace.trace(34);
            return null;
        }
    }

    /** allocate a new client session
     * 
     * */
    public SessionClient allocateSession() {
        if (!connected){
            dtrace.trace(5001);
            return null;
        }
        
        if (sessionInPool){
            Iterator it = sessions.values().iterator();
            while(it.hasNext()){
                PoolSession pooledSession = (PoolSession)it.next();
                if (!pooledSession.assigned && pooledSession.session != null)
                    return pooledSession.session;
            }
            dtrace.trace(5012);
            return null;
        }else{
            SessionClient client = connectSession();
            PoolSession session = new PoolSession(client,true);
            if (session!=null)
                sessions.put(new Integer(client.getSessionID()),session);
            return client;
        }
    }

    /** delocate a client session
     * @param sessionID session ID
     * */
    public void dellocateSession(int sessionID) {
        if (!connected){
            dtrace.trace(5001);
            return;
        }

        PoolSession pooledSession = (PoolSession)sessions.get(Integer.valueOf(sessionID));
        if (pooledSession!=null){
            if (sessionInPool)
                pooledSession.assigned = false;
            else{
                if (pooledSession.session != null)
                    pooledSession.session.release();
                sessions.remove(sessionID);
            }
            sessionNum--;
        }
    }
}
