/**
 * @(#)Manager.java	0.01 11/04/19
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_main;

//import com.vladium.utils.IObjectProfileNode;
//import com.vladium.utils.ObjectProfiler;

//import fydb.fy_comm.CommUtility;
import fydb.fy_comm.Consts;
import fydb.fy_comm.Debuger;
//import fydb.fy_comm.FyDataEntry;
import fydb.fy_comm.FyMetaData;
import fydb.fy_comm.InitParas;
//import fydb.fy_comm.Prediction;
//import fydb.fy_comm.Prediction;
import fydb.fy_comm.Tracer;

import fydb.fy_data.Beeper;
import fydb.fy_data.CommunicationClient;
import fydb.fy_data.ExCommunicator;
import fydb.fy_data.FyDataSet;

import fydb.fy_data.FyIndex;
//import fydb.fy_data.MemBaseData;

//import fydb.fy_data.MemHashKVData;

import fydb.fy_data.LogLeader;
import fydb.fy_data.MemBaseData;

//import fydb.fy_data.MemHashKVData;
//import fydb.fy_data.MemTreeKVData;

import fydb.fy_data.PhyHashKVData;

import fydb.fy_data.SysMetaData;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

//import java.security.KeyStore;
//import java.security.SecureRandom;

//import java.net.InetAddress;

//import java.net.UnknownHostException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.HashMap;
//import java.util.HashSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

//import javax.net.ssl.KeyManagerFactory;
//import javax.net.ssl.SSLContext;
//import javax.net.ssl.SSLServerSocket;
//import javax.net.ssl.TrustManagerFactory;

import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import org.xml.sax.SAXException;

//import java.sql.DatabaseMetaData;
//import java.io.FileNotFoundException;
//import java.util.HashSet;
//import org.w3c.dom.Element;


public class Manager {
    private InitParas paras = new InitParas();
    private Tracer dtrace = new Tracer(true);
    private Debuger debuger = new Debuger();
    private Beeper beeper = null;

    private final String version = "Alpha 0.01";

    private LogLeader logLeader = null;
    private SessionLeader sessionLeader = null;
    private ExCommunicator communicator = null;

    private static SysMetaData sysMetaData = null;
    private static boolean dbOpened = false;
    //private static boolean isLocal;  // if is running in local mode
    private static Map dataSets = null;

    protected long size = 0;

    public Manager() {
        dbOpened = false;
        //this.isLocal = isLocal;
        debuger.setDebugMode("YES".equalsIgnoreCase((String)paras.getParameter("debug")));
    }

    public static void main(String[] args) {
        Manager miniDB = new Manager();
        boolean running = true;
        if (miniDB.launch()) {
            // running as remote server mode
            while (running) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    //dtrace.trace(36);
                }
            }
            miniDB.shutdown();
        }
    }

    public boolean isOpened() {
        return dbOpened;
    }

    // start all core threads
    private void startCoreThreads() {
        if (logLeader != null && !logLeader.isAlive())
            logLeader.start();
        if (sessionLeader != null)
            sessionLeader.start();
        if (communicator != null)
            communicator.start();
    }

    // terminate all core threads
    private void terminateCoreThreads() {
        if (logLeader != null)
            logLeader.terminate();
        if (sessionLeader != null)
            sessionLeader.terminate();
        if (communicator != null)
            communicator.terminate();
    }

    // launch fydb system
    public boolean launch() {
        if (isOpened()) {
            dtrace.trace(27);
            return dbOpened;
        }
        if (paras.loadParameters()) {
            dtrace.trace("launching FyDB server ...");
            if (sysMetaData == null)
                sysMetaData = new SysMetaData(paras, dtrace, debuger);
            if (dataSets == null)
                dataSets = new HashMap();
            dtrace.trace("loading system meta data ...");
            if (sysMetaData.loadMetaData()) {
                if (communicator == null){
                    dtrace.trace("building communicator ...");
                    communicator = new ExCommunicator(paras, dtrace, debuger, dataSets, this);
                }
                dtrace.trace("Initializing communicator ...");
                if (!communicator.initialize()){
                    dtrace.trace("failed!");
                    return false;
                }
                dtrace.trace("Connecting to buddies ...");
                communicator.connectToServers((String[])paras.getParameter("buddyServers"));
                communicator.start();
                String[] newAddrs = communicator.sychronizeServers();
                if (newAddrs != null && newAddrs.length > 0)
                    communicator.connectToServers(newAddrs);
                communicator.getBeeperHost();
                //TokenInformation.setBeeperHost(beeperAddr == null);
                //CommunicationClient beeperServer = (CommunicationClient)communicator.getClients().get(beeperAddr);
                if (beeper == null)
                    beeper = new Beeper(sysMetaData.lastBP, paras, dtrace, communicator);
                if (logLeader == null)
                    logLeader = new LogLeader(dataSets, paras, dtrace, debuger, beeper, sysMetaData);
                dtrace.trace("Initializing session leader ...");
                if (sessionLeader == null)
                    sessionLeader = new SessionLeader(dataSets, paras, dtrace, debuger, beeper, this);
                if (!sessionLeader.initialize()){
                    dtrace.trace("failed!");
                    return false;
                }
            }else{
                dtrace.trace("failed!");
                return false;
            }
            dtrace.trace("loading datasets ...");
            loadDataSetsFromXml();
            startCoreThreads();
            dbOpened = true;
            dtrace.trace("FyDB server is running ...");
        }
        return dbOpened;
    }

    public Beeper getBeeper(){
        return beeper;
    }

    public String getVersion(){
        return version;
    }

    public boolean promote(String password){
        String superPassword = (String)paras.getParameter("superPassword");
        if (superPassword == null)
            return true;
        else{
            if (superPassword.equals(password))
                return true;
            else{
                dtrace.trace(129);
                return false;
            }
        }
    }

    private boolean loadDataSetFromXml(Node dsNode, Map dataProperties, HashMap indexes) {
        // read dataset porperties
        for (Node dsPropNode = dsNode.getFirstChild();dsPropNode != null;dsPropNode = dsPropNode.getNextSibling()) {
            if (dsPropNode.getNodeType() == Node.ELEMENT_NODE) {
                if (dsPropNode.getNodeName().equals("keyColumns")) {// read key columns
                    ArrayList keyColumns = new ArrayList();
                    for (Node colNode=dsPropNode.getFirstChild();colNode != null;colNode = colNode.getNextSibling()) {
                        if ("column".equalsIgnoreCase(colNode.getNodeName()))
                            keyColumns.add(colNode.getFirstChild().getNodeValue().trim());
                    }
                    dataProperties.put("keyColumns", keyColumns);
                }else if (dsPropNode.getNodeName().equals("indexes")) {// read index
                    ArrayList indColumns = new ArrayList();
                    for (Node indNode=dsPropNode.getFirstChild();indNode != null;indNode = indNode.getNextSibling()) {
                        if (indNode.getNodeType() != Node.ELEMENT_NODE)
                            continue;
                        int indexType = Consts.UNKNOWN;
                        int lastIndexId = 1; // 0 is kept for key
                        if ("index".equalsIgnoreCase(indNode.getNodeName())){
                            for (Node indProperties=indNode.getFirstChild();indProperties != null;indProperties = indProperties.getNextSibling()) {
                                if (indProperties.getNodeType() != Node.ELEMENT_NODE)
                                    continue;
                                if ("indexType".equalsIgnoreCase(indProperties.getNodeName().trim())){
                                    if ("HashMap".equalsIgnoreCase(indProperties.getFirstChild().getNodeValue().trim())){
                                        indexType = Consts.HASHMAP;
                                    }else if ("SortedMap".equalsIgnoreCase(indProperties.getFirstChild().getNodeValue().trim())){
                                        indexType = Consts.SORTEDMAP;
                                    }else if ("TreeMap".equalsIgnoreCase(indProperties.getFirstChild().getNodeValue().trim())){
                                        indexType = Consts.TREEMAP;
                                    }
                                } else if ("columns".equalsIgnoreCase(indProperties.getNodeName().trim())){
                                    for (Node colNode=indProperties.getFirstChild();colNode != null;colNode = colNode.getNextSibling())
                                        if ("column".equalsIgnoreCase(colNode.getNodeName()))
                                            indColumns.add(colNode.getFirstChild().getNodeValue().trim());
                                }
                            }
                            if (indexType == Consts.UNKNOWN){
                                dtrace.trace(115);
                                return false;
                            }
                            // here just construct the defination of the index. Will reconstruct them when initially loading data
                            // because we should assign the comparator to sorted index, which require metadata, and we can not get such informantion at here
                            FyIndex index = new FyIndex(dtrace, indColumns, indexType); 
                            indexes.put(Integer.valueOf(lastIndexId),index);
                            lastIndexId++;
                        }
                    }
                }else
                    dataProperties.put(dsPropNode.getNodeName().trim(),
                                       dsPropNode.getFirstChild().getNodeValue().trim());
            }
        }
        return true;
    }

    private boolean loadDataSetsFromXml() {
        //get a dom factory instance 
        String xmlFilePath = (String)paras.getParameter("baseDir") + File.separator + "dataSets.xml";
        try {
            DocumentBuilderFactory domFactory =
                DocumentBuilderFactory.newInstance();
            File dummmyfile = new File(xmlFilePath);
            if (!dummmyfile.exists()) {
                dtrace.trace(xmlFilePath);
                dtrace.trace(30);
                return true;
            }
            //get a dom builder
            DocumentBuilder domBuilder = domFactory.newDocumentBuilder();
            InputStream xmlFileStream = new FileInputStream(xmlFilePath);
            //parse xml
            Document doc = domBuilder.parse(xmlFileStream);
            //get child nodes (datasets)
            NodeList dsNodes = doc.getElementsByTagName("dataSet");

            if (dsNodes != null) {
                for (int i = 0; i < dsNodes.getLength(); i++) {
                    Node dsNode = dsNodes.item(i);
                    if (dsNode.getNodeType() == Node.ELEMENT_NODE) {
                        Map dataProperties = new HashMap();
                        //get name
                        String dataSetName = dsNode.getAttributes().getNamedItem("name").getNodeValue();
                        Integer memType = Integer.valueOf(dsNode.getAttributes().getNamedItem("memType") ==
                                                         null?-1:Consts.valueOf(dsNode.getAttributes().getNamedItem("memType").getNodeValue().trim()));
                        dataProperties.put("memType", memType);
                        Integer phyType = Integer.valueOf(dsNode.getAttributes().getNamedItem("phyType") ==
                                                        null?-1:Consts.valueOf(dsNode.getAttributes().getNamedItem("phyType").getNodeValue().trim()));
                        dataProperties.put("phyType", phyType);
                        HashMap indexes = new HashMap();
                        // read properties from xml file
                        loadDataSetFromXml(dsNode, dataProperties,indexes);
                        // load dataset according the properties
                        loadDataSet(dataSetName, dataProperties, indexes);
                    }
                }
            }
            xmlFileStream.close();
        } catch (ParserConfigurationException e) {
            dtrace.trace(xmlFilePath);
            dtrace.trace(31);
            return false;
        } catch (SAXException e) {
            dtrace.trace(xmlFilePath);
            dtrace.trace(31);
            return false;
        } catch (IOException e) {
            dtrace.trace(xmlFilePath);
            dtrace.trace(32);
            return false;
        } catch (Exception e) {
            dtrace.trace(10);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return false;
        }
        return true;
    }

    // check if the dataset duplicated. For each dataset, guid could identify the data source properties
    private boolean isDataSetDuplicated(String guid) {
        if (guid == null) // we do not detect the null value
            return true;
        Iterator it = dataSets.values().iterator();
        while (it.hasNext()) {
            if (guid.equalsIgnoreCase(((FyDataSet)it.next()).getGuid()))
                return true;
        }
        return false;
    }

    public boolean isMaster(String guid){
        Iterator it = dataSets.values().iterator();
        while (it.hasNext()) {
            FyDataSet ds = (FyDataSet)it.next();
            if (guid.equalsIgnoreCase(ds.getGuid()))
                return ds.isMaster(true);
        }
        return false;
    }

    public boolean handoverMaster(String guid){
        Iterator it = dataSets.values().iterator();
        while (it.hasNext()) {
            FyDataSet ds = (FyDataSet)it.next();
            if (guid.equalsIgnoreCase(ds.getGuid())){
                ds.setMaster(false);
                return true;
            }
        }
        return false;
    }

    public boolean sychronizeLogs(String guid, TreeMap curLogs){
        Iterator it = dataSets.values().iterator();
        while (it.hasNext()) {
            FyDataSet ds = (FyDataSet)it.next();
            if (guid.equalsIgnoreCase(ds.getGuid())){
                ds.synchronizeLogsLocal(curLogs);
                return true;
            }
        }
        return false;
    }

    // dataSetName should be unique. if dataSetName is null, will use the generated guid string as name.
    public String loadDataSet(String dataSetName, Map dataProperties, HashMap indexes) {
        //if (!isOpened()) {
        //    dtrace.trace(20);
        //    return null;
        //}
        //if (dataSetName == null) {
        //    dtrace.trace(dataSetName);
        //    dtrace.trace(221);
        //    return false;
        //}
        int memType = ((Integer)dataProperties.get("memType")).intValue();
        if (memType == -1) {
            dtrace.trace(108);
            return null;
        }
        int phyType = ((Integer)dataProperties.get("phyType")).intValue();
        if (phyType == -1) {
            dtrace.trace(109);
            return null;
        }
        FyDataSet dataSet = null;

        String tableName = (String)dataProperties.get("tableName");
        if (tableName == null || tableName.trim().equals("")) {
            dtrace.trace(111);
            return null;
        }
        HashMap clients = communicator.getClients();

        String guid = tableName.toUpperCase();
        if (phyType >= 1 && phyType <= 10) { // rdbms
            ArrayList keyColumns = (ArrayList)dataProperties.get("keyColumns");
            if (keyColumns == null || keyColumns.size() == 0) {
                dtrace.trace(110);
                return null;
            }
            String connString = (String)dataProperties.get("connString");
            if (connString == null || connString.trim().equals("")) {
                dtrace.trace(112);
                return null;
            }
            String dbUser = (String)dataProperties.get("dbUser");
            if (dbUser == null || dbUser.trim().equals("")) {
                dtrace.trace(113);
                return null;
            }
            String dbPassword = (String)dataProperties.get("dbPassword");
            if (dbPassword == null || dbPassword.trim().equals("")) {
                dtrace.trace(114);
                return null;
            }
            Connection dbconn = null;
            int dbid = 0;
            String schema = dbUser;
            if (phyType == Consts.DB_ORACLE) {
                try {
                    DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
                    dbconn = DriverManager.getConnection(connString, dbUser, dbPassword);
                    //PreparedStatement ps = dbconn.prepareStatement("select dbid, sys_context('USERENV', 'CURRENT_SCHEMA') schema from v$database");
                    //ResultSet rset = ps.executeQuery();
                    //rset.next();
                    //dbid = rset.getInt(1);
                    //schema = rset.getString(2);
                    //rset.close();
                    //ps.close();
                    // generate a unique identify string
                    // DB_Oracle/dbid/schema/tablename
                    //guid = ("DB_Oracle/" + String.valueOf(dbid) + "/" + schema + "/" +tableName).toUpperCase();
                    guid = (connString + "/" + schema + "/" +tableName).toUpperCase();
                    //Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:edgar","aaa","bbb");
                } catch (SQLException e) {
                    //e.printStackTrace();
                    dtrace.trace(connString);
                    dtrace.trace(204);
                    return null;
                } catch (Exception e) {
                    dtrace.trace(connString);
                    dtrace.trace(255);
                    if (debuger.isDebugMode())
                        e.printStackTrace();
                    return null;
                }
            }else if (phyType == Consts.DB_MYSQL) {
                try {
                    DriverManager.registerDriver(new com.mysql.jdbc.Driver());
                    dbconn = DriverManager.getConnection(connString, dbUser, dbPassword);
                    guid = (connString + "/" + schema + "/" +tableName).toUpperCase();
                    //Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:edgar","aaa","bbb");
                } catch (SQLException e) {
                    if (debuger.isDebugMode())
                        e.printStackTrace();
                    dtrace.trace(connString);
                    dtrace.trace(204);
                    return null;
                } catch (Exception e) {
                    dtrace.trace(connString);
                    dtrace.trace(255);
                    if (debuger.isDebugMode())
                        e.printStackTrace();
                    return null;
                }
            }else if (phyType == Consts.DB_MSSQL) {
                try {
                    DriverManager.registerDriver(new com.microsoft.sqlserver.jdbc.SQLServerDriver());
                    dbconn = DriverManager.getConnection(connString, dbUser, dbPassword);
                    guid = (connString + "/" + schema + "/" +tableName).toUpperCase();
                    //Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:edgar","aaa","bbb");
                } catch (SQLException e) {
                    if (debuger.isDebugMode())
                        e.printStackTrace();
                    dtrace.trace(connString);
                    dtrace.trace(204);
                    return null;
                } catch (Exception e) {
                    dtrace.trace(connString);
                    dtrace.trace(255);
                    if (debuger.isDebugMode())
                        e.printStackTrace();
                    return null;
                }
            }else if (phyType == Consts.DB_SYBASE) {
                try {
                    //DriverManager.registerDriver(new com.sybase.jdbc2.jdbc.SybDriver());
                    Class.forName("com.sybase.jdbc3.jdbc.SybDriver");
                    dbconn = DriverManager.getConnection(connString, dbUser, dbPassword);
                    guid = (connString + "/" + schema + "/" +tableName).toUpperCase();
                    //Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:edgar","aaa","bbb");
                } catch (SQLException e) {
                    if (debuger.isDebugMode())
                        e.printStackTrace();
                    dtrace.trace(connString);
                    dtrace.trace(204);
                    return null;
                } catch (Exception e) {
                    dtrace.trace(connString);
                    dtrace.trace(255);
                    if (debuger.isDebugMode())
                        e.printStackTrace();
                    return null;
                }
            }else if (phyType == Consts.DB_DB2) {
                try {
                    //DriverManager.registerDriver(new com.ibm.db2.jdbc.net.DB2Driver());
                    Class.forName("com.ibm.db2.jdbc.net.DB2Driver");
                    dbconn = DriverManager.getConnection(connString, dbUser, dbPassword);
                    guid = (connString + "/" + schema + "/" +tableName).toUpperCase();
                    //Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:edgar","aaa","bbb");
                } catch (SQLException e) {
                    if (debuger.isDebugMode())
                        e.printStackTrace();
                    dtrace.trace(connString);
                    dtrace.trace(204);
                    return null;
                } catch (Exception e) {
                    dtrace.trace(connString);
                    dtrace.trace(255);
                    if (debuger.isDebugMode())
                        e.printStackTrace();
                    return null;
                }
            }

            //dataSetName = dataSetName == null || dataSetName.trim().equals("") ? guid : dataSetName;
            dataSetName = guid;
            if (dataSets.containsKey(dataSetName)){// check if dataset name duplicated
                dtrace.trace(dataSetName);
                dtrace.trace(205);
                return null;
            }
            if (isDataSetDuplicated(guid)){// check if dataset duplicated
                dtrace.trace(dataSetName);
                dtrace.trace(249);
                return null;
            }
            
            dataSet = new FyDataSet(phyType, memType, dbconn, dbid, schema,
                                    tableName, keyColumns, indexes, guid, paras,
                                    dtrace, debuger, beeper, clients);
            dataSet.setWhere((String)dataProperties.get("where"));
        // initialize file source
        }else if(phyType >= 11 && phyType <= 20){ // load file dataset
            //dataSetName = dataSetName == null || dataSetName.trim().equals("") ? guid : dataSetName;
            dataSetName = guid;
            if (dataSets.containsKey(dataSetName)){// check if dataset name duplicated
                dtrace.trace(dataSetName);
                dtrace.trace(205);
                return null;
            }
            if (isDataSetDuplicated(guid)){// check if dataset duplicated
                dtrace.trace(dataSetName);
                dtrace.trace(249);
                return null;
            }

            dataSet = new FyDataSet(phyType, memType, null, -1, null,
                                    tableName, null, indexes, guid, paras,
                                    dtrace, debuger, beeper, clients);
            dataSet.setStoreLocal("YES".equalsIgnoreCase((String)dataProperties.get("storeLocal")));
        }
        dataSetName = dataSetName.toUpperCase();

        if (!loadData(dataSet))
            return null;

        dataSets.put(new String(dataSetName.toUpperCase()), dataSet);
        debuger.printMsg(dataSetName+" loaded.",true);
        
        return dataSetName;
    }
    
    private boolean loadData(FyDataSet dataSet){
        // check if dataSet has been loaded in net, the 1st server who loaded data is the master
        // if no server loaded dataset yet, local server is the master, and it should load data from physical data source directly
        // otherwise, it should ship the data from the master server
        if (dataSet.isMaster(false)){
            if (!dataSet.loadData(sysMetaData.lastBP))
                return false;
            dataSet.setStoreLocal(true);
        }else{
            // retrieve data from master server, push in local
            try{
                HashMap clients = communicator.getClients();
                String guid = dataSet.getGuid();
                CommunicationClient remoteServer = (CommunicationClient)clients.get(dataSet.getMasterAddr());
                if (remoteServer!=null){
                    FyMetaData metaData = remoteServer.getMetaData(guid);
                    HashMap dataProps = remoteServer.getDataProps(guid);
                    if (metaData == null || dataProps == null)
                        return false;
                    if (!dataSet.preparePush(metaData, dataProps))
                        return false;

                    int batchSize = 1000;
                    // begin fetch, wait untill master initialized and data loaded
                    while (!remoteServer.beginFetch(guid))
                        Thread.sleep(3000);
                    Map tmpData = remoteServer.batchFetch(guid,batchSize);  // 
                    while(tmpData.size()>0){
                        dataSet.pushData(tmpData);
                        tmpData = remoteServer.batchFetch(guid,batchSize); 
                    }
                    dataSet.completePush();
                }
            }catch(InterruptedException e){
                return false;
            }
        }
        return true;
    }

    // copy dataset to local file
    private boolean copyDatasetToFile(String dataSetName, int fileModNum, int fileToken, int blockSize, String fileName, String encoding){
        if (!isOpened()) {
            dtrace.trace(20);
            return false;
        }
        FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
        if (dataSet == null)
            return false;
        else {
            PhyHashKVData tmpPhy = new PhyHashKVData(dtrace,debuger,paras);
            HashMap dataProps = new HashMap();
            dataProps.put("fileModNum",new Integer(fileModNum));
            dataProps.put("fileToken",new Integer(fileModNum));
            dataProps.put("blockSize",new Integer(blockSize));
            dataProps.put("encoding",encoding);
            String fileDir = (String)paras.getParameter("dataDir")+File.separator+fileName;
            dataProps.put("fileDir",fileDir);
            dataProps.put("fileName",fileName);
            tmpPhy.assignMetaData(dataSet.getMetaData());
            tmpPhy.init(fileDir, fileName,Consts.DISK);
            tmpPhy.assignDataProps(dataProps);
            tmpPhy.prepareCopy();
            dtrace.trace("begin copy file");
            tmpPhy.fullCopyToFiles(dataSet.getMemData());
            dtrace.trace("copy file completed.");
        }
        return true;
    }

    public boolean copyDatasetToFile(String dataSetName, String fileName){
        return copyDatasetToFile(dataSetName,1,1,8192,fileName,"UTF-8");
    }

    // release dataset, including data logs and other resources
    public boolean releaseDataset(String dataSetName){
        if (!isOpened()) {
            dtrace.trace(20);
            return false;
        }
        FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
        if (dataSet == null)
            return false;
        else {
            return dataSet.release();
        }
    }

    // reload dataset
    public boolean reloadDataset(String dataSetName){
        if (!isOpened()) {
            dtrace.trace(20);
            return false;
        }
        FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
        if (dataSet == null)
            return false;
        else {
            return loadData(dataSet);
        }
    }

    public boolean buildDataSetIndexe(String dataSetName, ArrayList index, int indexType){
        if (!isOpened()) {
            dtrace.trace(20);
            return false;
        }
        FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
        if (dataSet == null) {
            dtrace.trace(206);
            return false;
        }
        return dataSet.buildIndex(index, indexType);
    }

    public long calculateSize(String dataSetName) {
        if (!isOpened()) {
            dtrace.trace(20);
            return -1;
        }
        long size = 0;
        // if dataSetName is null, calculate all datasets, means db, size
        if (dataSetName == null){
            Iterator it = dataSets.values().iterator();
            while (it.hasNext()){
                FyDataSet dataSet = (FyDataSet)it.next();
                //IObjectProfileNode p = ObjectProfiler.profile(dataSets);
                //size += p.size();
                size += dataSet.sizeOf();
            }
        }else{
            FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
            if (dataSet == null)
                size = 0;
            else {
                //IObjectProfileNode p = ObjectProfiler.profile(dataSets);
                size = dataSet.sizeOf();
                //debuger.printMsg(String.valueOf(size), false);
            }
        }
        return size;
    }

    // allocate a new local seesion
    public SessionServer newSession() {
        return sessionLeader.allocateSession();
    }

    // release a local seesion
    public void releaseSession(SessionServer session) {
        if (session == null)
            return;
        else
            sessionLeader.dellocateSession(session.getSessionID());
    }
    
    // for test purpose
    public MemBaseData getMemData(String dataSetName){
        if (!isOpened()) {
            dtrace.trace(20);
            return null;
        }
        FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
        if (dataSet == null)
            return null;
        else {
            return dataSet.getMemData();
        }
    }

    // 
    public FyMetaData getMetaData(String dataSetName){
        if (!isOpened()) {
            dtrace.trace(20);
            return null;
        }
        FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
        if (dataSet == null)
            return null;
        else {
            return dataSet.getMetaData();
        }
    }

    public HashMap getDataProps(String dataSetName){
        if (!isOpened()) {
            dtrace.trace(20);
            return null;
        }
        FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
        if (dataSet == null)
            return null;
        else {
            return dataSet.getDataProps();
        }
    }

    public boolean beginFetch(String dataSetName, String clientAddr){
        if (!isOpened()) {
            dtrace.trace(20);
            return false;
        }
        FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
        if (dataSet == null)
            return false;
        else {
            return dataSet.beginFetch(clientAddr);
        }
    }

    public HashMap batchFetch(String dataSetName, int batchSize, String clientAddr){
        if (!isOpened()) {
            dtrace.trace(20);
            return new HashMap();
        }
        FyDataSet dataSet = (FyDataSet)dataSets.get(dataSetName.toUpperCase());
        if (dataSet == null)
            return new HashMap();
        else {
            return dataSet.batchFetch(clientAddr, batchSize);
        }
    }

    /*// implement logs to physical data source. Internal call
    private void implementLogs(){
        if (!isOpened()){
            dtrace.trace(20);
            return;
        }
        Iterator it = dataSets.values().iterator();
        while (it.hasNext())
        {
            FyDataSet dataSet = (FyDataSet)it.next();
            if (dataSet == null){
                continue;
            }
            dataSet.implementLogs(beeper.getCurBP());
        }
    }//*/

    // shutdown fydb system
    public boolean shutdown() {
        if (!isOpened()) {
            dtrace.trace(20);
            return false;
        }
        dtrace.trace("Saving system data ...");
        paras.saveParameters();
        sysMetaData.saveMetaData();
        dtrace.trace("Releasing datasets ...");
        Iterator it = dataSets.values().iterator();
        while (it.hasNext()) {
            FyDataSet dataSet = (FyDataSet)it.next();
            if (dataSet != null)
                dataSet.release();
        }
        dataSets = null;
        dtrace.trace("Terminating processes/threads ...");
        terminateCoreThreads();

        beeper = null;
        logLeader = null;
        //sessionLeader = null;
        communicator = null;
        sysMetaData = null;

        dbOpened = false;
        dtrace.trace("FyDB is shutted down.");
        return true;
    }
}
