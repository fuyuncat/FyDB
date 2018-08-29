package demo;

import fydb.fy_comm.InitParas;
import fydb.fy_comm.InitParas.*;
import fydb.fy_main.Manager;
import fydb.fy_data.FyDataSet;
import fydb.fy_data.BP;

import fydb.fy_comm.CommUtility;
import fydb.fy_comm.Consts;
import fydb.fy_comm.Debuger;
import fydb.fy_comm.FyDataEntry;
import fydb.fy_comm.Prediction;

import fydb.fy_comm.StatementParser;

import fydb.fy_comm.Tracer;

import fydb.fy_data.FyBaseLogEntry;
import fydb.fy_data.FyDataLogEntry;

import fydb.fy_data.FyIndexLogEntry;
import fydb.fy_data.FyLogData;

import fydb.fy_data.MemBaseData;
import fydb.fy_data.MemHashKVData;
import fydb.fy_data.MemTreeKVData;
import fydb.fy_data.PhyHashKVData;

import fydb.fy_main.SessionServer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.io.OutputStreamWriter;

import java.net.InetAddress;
import java.net.ServerSocket;

import java.net.Socket;

import java.nio.charset.Charset;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import java.sql.Statement;

import java.sql.Timestamp;
import java.sql.Types;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

public class Main {
    public Main() {
    }
    
    public void test1(){
        ///*
        ServerSocket srv;
        boolean run = true;
        if (run){
            try {
                File f = new File("c:\\temp\\os.out");
                f.delete();
                f.createNewFile();
                //ObjectOutputStream fo = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(f)));
                //fo.writeObject(new String("aaa"));fo.flush();
                ObjectInputStream fi = new ObjectInputStream(new BufferedInputStream(new FileInputStream(f)));
                String aa = (String)fi.readObject();
                System.out.println(aa);///
            
                srv = new ServerSocket(9989,1,InetAddress.getByName("localhost"));
                Socket cSocket = srv.accept();
                ObjectOutputStream oo = new ObjectOutputStream(new BufferedOutputStream(cSocket.getOutputStream()));
                oo.writeObject(new String("accepted"));oo.flush();
                ObjectInputStream oi = new ObjectInputStream(new BufferedInputStream(cSocket.getInputStream()));
                String shakeHandMsg = (String)oi.readObject();

                while ("start".equals(shakeHandMsg)) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        break;
                        //dtrace.trace(36);
                    }
                }
                SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
                SSLSocket s = (SSLSocket) factory.createSocket("localhost", 8888);
        
                ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(s.getOutputStream()));
                ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(s.getInputStream()));
        
                String message = (String) ois.readObject();//????
                System.out.println("aaa:" + message);
        
                message=null;
                oos.writeObject(message);//????
                 oos.flush();
                 while (true) {
                     try {
                         Thread.sleep(1);
                     } catch (InterruptedException e) {
                         e.printStackTrace();
                         break;
                         //dtrace.trace(36);
                     }
                 }
                oos.close();
                ois.close();
                s.close();
            }
            catch (Exception e) {
                e.printStackTrace();
                return;
            } 
            //return;
        }
        if (run){
            BufferedWriter bWriter;
            BufferedReader bReader;
            try{ // log file of a db source data: logDir/dbid/schema/<tablename>_<seqNum>.log. Where guid contains connectstring info and table info.
                File dummmyfile = new File("abc.txt");
                if (!dummmyfile.exists())
                {
                    //dummmyfile.getParentFile().mkdirs();
                    dummmyfile.createNewFile();
                }
                //logWriter = new ObjectOutputStream(new FileOutputStream(curLogName, true));
                //bWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dummmyfile.getCanonicalPath(), true)));
                bWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dummmyfile.getCanonicalPath(), false)));
                bReader = new BufferedReader(new InputStreamReader(new FileInputStream(dummmyfile.getCanonicalPath())),2);
                char carray[] = new char[10];
                bWriter.write("abc");
                //System.out.println(bReader.readLine());
                bWriter.write("def");
                bWriter.write("ghi");
                bWriter.flush();
                bReader.read(carray,0,3);
                System.out.println("1:"+String.valueOf(carray));
                bReader.mark(1);
                bReader.read(carray,0,5);
                System.out.println("2:"+String.valueOf(carray));
                bReader.reset();
                bReader.read(carray,0,10);
                System.out.println("3:"+String.valueOf(carray));
                //bWriter.close();
                bReader.reset();
                bWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dummmyfile.getCanonicalPath(), false)));
                bWriter.write("ABCDEFGHI");
                bWriter.flush();
                bReader.reset();
                bReader.read(carray,0,10);
                System.out.println("4:"+String.valueOf(carray));
                //System.out.println(bReader.readLine());
            }
            catch (Exception e)
            {
                e.printStackTrace();
                return;
            }
            //return;
        }
        if (run){
            try{
                DriverManager.registerDriver (new oracle.jdbc.driver.OracleDriver());
                Connection dbconn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:edgar","demo","demo");
                //PreparedStatement ps = dbconn.prepareStatement("update t_test1 set SUBOBJECT_NAME=? where owner=? and object_id=?");
                //ps.setString(1, "");ps.setString(2, null);ps.setNull( 3, 0);
                PreparedStatement ps = dbconn.prepareStatement("update t_test1 set SUBOBJECT_NAME=? where ((SECONDARY is null or ? is null) or SECONDARY=?)");
                ps.setString(1, "BBB");ps.setNull(2, 0);ps.setNull(3, 0);
                ps.addBatch();
                ps.executeBatch();
                //Statement stmt = dbconn.createStatement(); 
                //ResultSet rset = stmt.executeQuery ("select * from t_test2");
                //rset.next();
                //stmt.executeUpdate("update t_test2 set TABLESPACE_NAME='SYSTEM1' where owner = 'SYSTEM' and table_name = 'AQ$_INTERNET_AGENTS'");
                //stmt.addBatch("update t_test2 set TABLESPACE_NAME='SYSTEM1' where owner = 'SYSTEM' and table_name = 'AQ$_INTERNET_AGENTS'");
                //stmt.executeBatch();
                //rset.next();
            }catch (SQLException e)
            {
                e.printStackTrace();
            }
            //return;
        }
        TreeMap tTreeMap = new TreeMap(Collections.reverseOrder());
        long ll = 0;
        tTreeMap.put(ll,String.valueOf(ll++));
        tTreeMap.put(ll,String.valueOf(ll++));
        tTreeMap.put(ll,String.valueOf(ll++));
        tTreeMap.put(ll,String.valueOf(ll++));
        tTreeMap.put(ll,String.valueOf(ll++));
        tTreeMap.put(ll,String.valueOf(ll++));
        tTreeMap.put(ll,String.valueOf(ll++));
        ll = 3;
        SortedMap subTree = tTreeMap.headMap(ll);
        Iterator it = subTree.values().iterator(); 
        while(it.hasNext()) {
            String s = (String)it.next();
            System.out.println(s);
        }
        
        HashMap tHashMap1 = new HashMap();
        HashMap tHashMap2 = new HashMap();
        tHashMap1.put(new Integer(1), "aaa");
        tHashMap1.put(new Integer(2), "bbb");
        tHashMap2.put(new Integer(2), "bbb");
        tHashMap2.put(new Integer(1), "aaa");
        if (tHashMap1.equals(tHashMap2))
            System.out.println("They are equal");
        else
            System.out.println("They are not equal");

        BP testBP = new BP(0,1,2);
        try{
            String curLogName = new String("L123456.log");
            File dummmyfile = new File(curLogName);
            if (!dummmyfile.exists())
                dummmyfile.createNewFile();
            ObjectOutputStream logWriter = new ObjectOutputStream(new FileOutputStream(curLogName, true));
            SimpleDateFormat tempDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
            String datetime = tempDate.format(new java.util.Date());
            System.out.println("write log begin: "+datetime);
            logWriter.write("0,1,10,1:DBSNMP,2:MGMT_BSLN_THRESHOLD_PARMS,3:SYSTEM:SYSTEM1".getBytes());
            logWriter.flush();
            datetime = tempDate.format(new java.util.Date());
            System.out.println("write log end: "+datetime);
        }
        catch (Exception e)
        {
          e.printStackTrace();
          return;
        }
        if (run) return;
        try{
            ObjectInputStream logReader = new ObjectInputStream(new FileInputStream("L403809811.log"));
            String data = (String)logReader.readObject();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return;
        }
         Set<String> charsetNames = Charset.availableCharsets().keySet();  
         int cnum=1;
         for (it = charsetNames.iterator(); it.hasNext();)  
         {  
             String charsetName = (String) it.next();   
             System.out.println("public final static int "+charsetName+"="+cnum);  
             cnum++;
         }  
    }
    
    public void test2(){
        byte[] bbb = CommUtility.intToByteArray(15);
        String[] tokens = StatementParser.getAllTokens(" owner = :1 and ( owner = :2 or table_name = :3)", "[A-Za-z]+[A-Za-z0-9$_]*");
         String abc = StatementParser.removeSpace(" owner = :1 and ( owner = :2 or table_name = :3)",null);
         //System.out.println(abc);
        String predStr = "((owner =\"AA\" or owner =\"ad\") and (table_name=\"asdfas\" or tablespace_name> \"dadsa\")) and status != :5 or status <= 'ABC'";
        //String predStr = "owner=\"AA\" OR owner=\"ad\" AND (table_name=\"asdfas\" OR tablespace_name>\"dadsa\")";
        System.out.println(predStr);
        predStr = StatementParser.removeSpace(predStr,null);
        StatementParser.buildPrediction(predStr);
        Timestamp t1 = Timestamp.valueOf("2007-03-15 22:00:15.0");
        Timestamp t2 = Timestamp.valueOf("2007-03-15 22:01:00");
        int aa = t2.compareTo(t1);
        int bb = t1.compareTo(t2);
        FyLogData lgTest = new FyLogData();
        HashMap kkk = new HashMap(); kkk.put("abc","cba");
        HashMap vvv = new HashMap(); kkk.put("ttt","ddd");
        FyBaseLogEntry dataEntry = new FyDataLogEntry(1,kkk,vvv);
        FyBaseLogEntry indEntry = new FyIndexLogEntry();
        lgTest.put("1",dataEntry);
        lgTest.put("2",indEntry);
        Iterator iii = lgTest.values().iterator();
        while (iii.hasNext()){
            FyBaseLogEntry outLog = (FyBaseLogEntry)iii.next();
            if (outLog.logType == 1){
                outLog = (FyDataLogEntry)outLog;
            }else{
                outLog = (FyIndexLogEntry)outLog;
            }
            String aaa = "break";
        }
    }
    
    public void test3(Manager miniDB){
        Debuger debuger = new Debuger();
        Tracer dtrace = new Tracer(true);
        InitParas paras = new InitParas();
        PhyHashKVData testPhy = new PhyHashKVData(dtrace,debuger,paras);
        ///*
        HashMap dataProps = new HashMap();
        dataProps.put("fileModNum",new Integer(1));
        dataProps.put("encoding","UTF-8");
        dataProps.put("blockSize",8192);
        testPhy.assignMetaData(miniDB.getMetaData("DB_Oracle/1712582900/DEMO/T_TEST2"));
        testPhy.init((String)paras.getParameter("dataDir")+File.separator+"test", "test",Consts.DISK);
        testPhy.assignDataProps(dataProps);
        debuger.printMsg("begin write file", true);
        testPhy.fullCopyToFiles(miniDB.getMemData("DB_Oracle/1712582900/DEMO/T_TEST2"));
        debuger.printMsg("end write file", true);

        {
        HashMap phyKey = new HashMap();
        phyKey.put(new Integer(0), "DEMO");
        phyKey.put(new Integer(1), "BIGTAB");
        //HashMap phyVal = (HashMap)miniDB.getMemData("DB_Oracle/1712582900/DEMO/T_TEST2").get(phyKey);
        FyDataEntry phyData = testPhy.physicalReadEntry(phyKey);
        testPhy.dump();
        }
        //*/

        ///*
        debuger.printMsg("begin read file", true);
        testPhy.init((String)paras.getParameter("dataDir")+File.separator+"test", "test",Consts.DISK);
        debuger.printMsg("end read file", true);

        MemBaseData testMem = new MemHashKVData(dtrace);
        //testPhy.beginLoadingData();
        FyDataEntry row = testPhy.next(null);
        int entryCounter=0;
        while(row != null){
            if (testMem.containsKey(row.key)) { // detect duplicated key
                dtrace.trace(210);
                //memData.releaseAll();
                //metaData.releaseData();
                //quiesced = false;
                //return false;
            }
            entryCounter++;
            testMem.add(row);
            //insertRowToIndexes(null, row, null, true);
            if (entryCounter == 484){
                int bbr = 0;
            }
            row = testPhy.next(null);
        }
        HashMap phyKey = new HashMap();
        phyKey.put(new Integer(0), "DEMO");
        phyKey.put(new Integer(1), "NONE");
        FyDataEntry phyData = testPhy.physicalReadEntry(phyKey);
        phyKey.clear();
        phyKey.put(new Integer(0), "DEMO");
        phyKey.put(new Integer(1), "BIGTAB");
        //HashMap phyVal = (HashMap)miniDB.getMemData("DB_Oracle/1712582900/DEMO/T_TEST2").get(phyKey);
        HashMap phyVal = (HashMap)testMem.get(phyKey);
        phyData = testPhy.physicalReadEntry(phyKey);
        testMem = testPhy.fullRead();
        testPhy.dump();
        //*/

        testPhy.release();
    }
    
    public void test4(Manager miniDB){
        Debuger debuger = new Debuger();
        //localSession = miniDB.newSession();

        // (0(OWNER) == DEMO and 5(STATUS) == VALID) or 2(TABLESPACE_NAME)!=AAA
        Prediction nodel = new Prediction(1,0,"BBS"); // 0(OWNER) == BBS
        Prediction noder = new Prediction(1,5,"VALID"); // 5(STATUS) == VALID
        Prediction br = new Prediction(1,nodel,noder);        // (0(OWNER) == DEMO and 5(STATUS) == VALID)
        //nodel = new Prediction(4,2,"AAA");  // 2(TABLESPACE_NAME)!=AAA
        //nodel = new Prediction(3,24,"30");  // 24(AVG_ROW_LEN)<30
        nodel = new Prediction(2,32,"2007-03-01 13:15:00");  // 32(LAST_ANALYZED)>2007-03-01 13:15:00
        noder = new Prediction(6,32,"2007-03-15 22:00:16");  // 32(LAST_ANALYZED)<=2007-03-15 22:00:16
        Prediction bl = new Prediction(1,nodel,noder); // 32(LAST_ANALYZED)>2007-03-01 13:15:00 and 32(LAST_ANALYZED)<2007-03-15 10:00:00
        Prediction prediction = new Prediction(2,bl,br);  // or
        //nodel = new Prediction(1,0,"DEMO");
        //noder = new Prediction(1,1,"T_TEST1");
        //filter = new Prediction(1,nodel,noder);  // hit key. 0(OWNER) == DEMO and 1(TABLE_NAME) == "T_TEST1"
        SessionServer localSession = miniDB.newSession();
        //general test
        String predStr = "((owner =\"AA\" or owner =\"ad\") and (table_name=\"asdfas\" or tablespace_name> \"dadsa\")) and status != :5 or status <= 'ABC'";
        //key get ????
        predStr = "((status != \"INVALID\" or owner =\"ABC\") and (table_name=\"asdfas\" or tablespace_name> \"dadsa\")) and table_name=\"T_TEST1\" and table_name=\"T_TEST2\" AND owner = \"DEMO\" AND (owner <= \"ABC\" OR TABLESPACE_NAME != EDGAR)";
        //key get ????
        predStr = "(status != \"INVALID\" or tablespace_name > \"RING\") and table_name = \"T_TEST1\" and table_name = \"T_TEST1\" AND owner = \"DEMO\"";
        predStr = "((owner=\"DEMO\" and (pct_free = 0 or ini_trans = 1)) and (STATUS=\"VALID\" or INITIAL_EXTent = 7)) and (TABLE_NAME=\"T_TEST1\" or TABLESPACE_NAME=\"RING\")";
        predStr = "((owner=\"DEMO\" and (pct_free = 0 or ini_trans = 1)) and (STATUS=\"VALID\" or INITIAL_EXTent = 7)) or (TABLE_NAME=\"T_TEST1\" or TABLESPACE_NAME=\"RING\")";
        // index (hashmap: TABLESPACE_NAME, NUM_ROWS) get
        predStr = "((owner=\"DEMO\" and (pct_free = 0 or ini_trans = 1)) or (STATUS=\"VALID\" or INITIAL_EXTent = 7)) and (NUM_ROWS=0 and TABLESPACE_NAME=\"RING\")";
        // full data scan
        predStr = "LAST_ANALYZED>\"2007-03-01 13:15:00\" and LAST_ANALYZED<=\"2007-03-15 22:01:00\"";
        // full key scan
        predStr = "(status != \"INVALID\" or tablespace_name > \"RING\") and pct_free = 1000 and table_name = \"T_TEST1\"";
        // full index (hashmap: TABLESPACE_NAME, NUM_ROWS) scan
        predStr = "((owner=\"DEMO\" and (pct_free = 0 or ini_trans = 1)) or (STATUS=\"VALID\" or INITIAL_EXTent = 7)) and (TABLESPACE_NAME=\"RING\")";
        // full data scan when no index hitted, full index(LAST_ANALYZED, TABLE_NAME, OWNER) scan
        predStr = "LAST_ANALYZED>\"2007-03-01 13:15:00\" and LAST_ANALYZED<=\"2007-03-15 22:01:00\"";
        // part index(LAST_ANALYZED, TABLE_NAME, OWNER) scan
        predStr = "LAST_ANALYZED=\"2007-03-15 22:00:15\" and TABLE_Name >= \"M7B\" and tablespace_name = \"EDGARDEMO\"";
        // part index(LAST_ANALYZED, TABLE_NAME, OWNER) scan
        predStr = "LAST_ANALYZED=\"2007-03-15 22:00:15\" and owner >= \"DEMO\"";
        // get all data
        predStr = "";
        debuger.printMsg("start parsing", true);
        prediction = StatementParser.buildPrediction(predStr);
        debuger.printMsg("end parsing", true);
        ArrayList result = localSession.searchData("DB_Oracle/1712582900/DEMO/T_TEST2",prediction,true);
        while (result == null && localSession.getLastAsynState() != Consts.CLEAR){
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                //dtrace.trace(36);
            }
            if (localSession.getLastAsynState() == Consts.COMPLETED)
                result = (ArrayList)localSession.getAsynchronizedResult();
            else if (localSession.getLastAsynState() == Consts.FAILED)
                System.out.println("call failed");
        }

        ArrayList readResult = null;
        try {
            File f = new File("c:\\temp\\os.out");
            f.delete();
            f.createNewFile();
            ObjectOutputStream fo = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(f)));
            fo.writeObject(result);fo.flush();fo.close();
            ObjectInputStream fi = new ObjectInputStream(new BufferedInputStream(new FileInputStream(f)));
            readResult = (ArrayList)fi.readObject();
         } catch (Exception e) {
             //dtrace.trace(36);
         }

    }
    
    public void test5(Manager miniDB,String dataSetName){
        Debuger debuger = new Debuger();

        ArrayList rdKeys = new ArrayList();
        {
            ArrayList k = new ArrayList();
            k.add("DEMO");k.add("T_TEST1");rdKeys.add(k);
            k = new ArrayList();k.add("DBSNMP");k.add("MGMT_BASELINE_SQL");rdKeys.add(k);
            k = new ArrayList();k.add("DBSNMP");k.add("MGMT_BSLN_BASELINES");rdKeys.add(k);
            k = new ArrayList();k.add("DBSNMP");k.add("MGMT_BSLN_DATASOURCES");rdKeys.add(k);
            k = new ArrayList();k.add("DBSNMP");k.add("MGMT_BSLN_INTERVALS");rdKeys.add(k);
            k = new ArrayList();k.add("DBSNMP");k.add("MGMT_BSLN_METRICS");rdKeys.add(k);
            k = new ArrayList();k.add("DBSNMP");k.add("MGMT_BSLN_RAWDATA");rdKeys.add(k);
            k = new ArrayList();k.add("DBSNMP");k.add("MGMT_BSLN_STATISTICS");rdKeys.add(k);
            k = new ArrayList();k.add("DBSNMP");k.add("MGMT_BSLN_THRESHOLD_PARMS");rdKeys.add(k);
            k = new ArrayList();k.add("DBSNMP");k.add("MGMT_CAPTURE");rdKeys.add(k);
            k = new ArrayList();k.add("DBSNMP");k.add("MGMT_CAPTURE_SQL");rdKeys.add(k);
            k = new ArrayList();k.add("DBSNMP");k.add("MGMT_HISTORY");rdKeys.add(k);
            k = new ArrayList();k.add("DBSNMP");k.add("MGMT_HISTORY_SQL");rdKeys.add(k);
            k = new ArrayList();k.add("DBSNMP");k.add("MGMT_LATEST");rdKeys.add(k);
            k = new ArrayList();k.add("DBSNMP");k.add("MGMT_LATEST_SQL");rdKeys.add(k);
            k = new ArrayList();k.add("DBSNMP");k.add("MGMT_SNAPSHOT_SQL");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("AQ$_SCHEDULES");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("DEF$_CALLDEST");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("DEF$_DEFAULTDEST");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("DEF$_DESTINATION");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("DEF$_ERROR");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("DEF$_LOB");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("DEF$_ORIGIN");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("DEF$_PROPAGATOR");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("DEF$_PUSHED_TRANSACTIONS");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("DEF$_TEMP$LOB");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNRC_DBNAME_UID_MAP");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNRC_GSII");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNRC_GTCS");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNRC_GTLO");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNRP_CTAS_PART_MAP");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_AGE_SPILL$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_ATTRCOL$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_ATTRIBUTE$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_CCOL$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_CDEF$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_COL$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_COLTYPE$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_DICTIONARY$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_DICTSTATE$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_ERROR$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_FILTER$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_HEADER1$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_HEADER2$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_ICOL$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_IND$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_INDCOMPART$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_INDPART$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_INDSUBPART$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_LOB$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_LOBFRAG$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_LOG$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_OBJ$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_PARAMETER$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_PROCESSED_LOG$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_RESTART_CKPT$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_RESTART_CKPT_TXINFO$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_SESSION_EVOLVE$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_SPILL$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_TAB$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_TABCOMPART$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_TABPART$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_TABSUBPART$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_TS$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_TYPE$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_UID$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGMNR_USER$");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGSTDBY$APPLY_MILESTONE");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGSTDBY$APPLY_PROGRESS");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGSTDBY$EVENTS");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGSTDBY$HISTORY");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGSTDBY$PARAMETERS");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGSTDBY$PLSQL");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGSTDBY$SCN");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGSTDBY$SKIP");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGSTDBY$SKIP_SUPPORT");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("LOGSTDBY$SKIP_TRANSACTION");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_AJG");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_BASETABLE");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_CLIQUE");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_ELIGIBLE");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_EXCEPTIONS");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_FILTER");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_FILTERINSTANCE");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_FJG");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_GC");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_INFO");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_JOURNAL");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_LEVEL");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_LOG");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_OUTPUT");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_PARAMETERS");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_PLAN");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_PRETTY");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_ROLLUP");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_SQLDEPEND");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_TEMP");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("MVIEW$_ADV_WORKLOAD");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("REPCAT$_AUDIT_ATTRIBUTE");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("REPCAT$_AUDIT_COLUMN");rdKeys.add(k);
            k = new ArrayList();k.add("SYSTEM");k.add("REPCAT$_COLUMN_GROUP");rdKeys.add(k);
        }

        //SessionServer localSession;
        SessionServer localSession = miniDB.newSession();
        
        Map dsProps = new HashMap();
        dsProps.put("memType", Consts.HASHMAP);
        dsProps.put("phyType", Consts.DB_ORACLE);
        dsProps.put("tableName", "T_TEST1");
        ArrayList keyColumns = new ArrayList();
        keyColumns.add("OWNER");keyColumns.add("OBJECT_NAME");
        dsProps.put("keyColumns", keyColumns);
        dsProps.put("connString", "jdbc:oracle:thin:@localhost:1521:edgar");
        dsProps.put("dbUser", "demo");
        dsProps.put("dbPassword", "demo");
        //miniDB.loadDataSet(dsProps, "T_TEST1~10G");

        dsProps = new HashMap();
        dsProps.put("memType", Consts.HASHMAP);
        dsProps.put("phyType", Consts.DB_ORACLE);
        dsProps.put("tableName", "T_TEST2");
        keyColumns = new ArrayList();
        keyColumns.add("OWNER");keyColumns.add("TABLE_NAME");
        dsProps.put("keyColumns", keyColumns);
        dsProps.put("connString", "jdbc:oracle:thin:@huanged2:1523:ora11r2");
        dsProps.put("dbUser", "demo");
        dsProps.put("dbPassword", "demo");
        //miniDB.loadDataSet(dataSetName, dsProps, null);

        //if (miniDB.isOpened())
        //    miniDB.shutdown();
        Random r = new Random();
        ArrayList keyData = (ArrayList)rdKeys.get(r.nextInt(100));
        HashMap val;
        System.out.println(keyData.get(0)+";"+keyData.get(1)+":");
        //val = miniDB.singleRead(dataSetName, miniDB.buildKeyWithName("T_TEST1~10G", keyColumns, key));
        //it = val.values().iterator(); 
        //while(it.hasNext()) {
        //    String s = (String)it.next();
        //    System.out.print(s);
        //}
        /*            val = miniDB.singleRead(dataSetName, miniDB.buildKeyWithName(dataSetName, keyColumns, keyData));
           if (val == null)
               System.out.println("no data found");
           else {
               Iterator it = val.values().iterator(); 
               while(it.hasNext()) {
                   String s = (String)it.next();
                   System.out.print(s+",");
               }
               System.out.println();
           }
        //*/
        ArrayList colNames = new ArrayList();
        ArrayList newValues = new ArrayList();
        colNames.add("TABLESPACE_NAME");
        colNames.add("OWNER");
        colNames.add("TABLE_NAME");
        newValues.add("SYSTEM1");
        newValues.add("DEMO");
        newValues.add("T_TEST1");
        localSession.insertDataS(dataSetName,colNames,newValues);
        localSession.insertDataS(dataSetName,colNames,newValues);
        localSession.deleteDataByKey(dataSetName,localSession.buildKeyWithName(dataSetName,colNames,newValues));
        
        HashSet<HashMap> keys = new HashSet();
        newValues.clear();
        newValues.add("DEMO");
        newValues.add("T_TEST1");
        keys.add(localSession.buildKeyWithName(dataSetName,keyColumns,newValues));
        newValues.clear();
        newValues.add("SYSTEM");
        newValues.add("MVIEW$_ADV_BASETABLE");
        keys.add(localSession.buildKeyWithName(dataSetName,keyColumns,newValues));
        newValues.clear();
        newValues.add("DEMO");
        newValues.add("T_TEST3");
        keys.add(localSession.buildKeyWithName(dataSetName,keyColumns,newValues));
        newValues.clear();
        newValues.add("SYSTEM");
        newValues.add("MVIEW$_ADV_BASETABLE");
        keys.add(localSession.buildKeyWithName(dataSetName,keyColumns,newValues));
        newValues.clear();
        newValues.add("DEMO");
        newValues.add("T_TEST5");
        keys.add(localSession.buildKeyWithName(dataSetName,keyColumns,newValues));
        newValues.clear();
        newValues.add("DEMO");
        newValues.add("MVIEW$_ADV_PRETTY");
        keys.add(localSession.buildKeyWithName(dataSetName,keyColumns,newValues));
        newValues.clear();
        newValues.add("SYSTEM");
        newValues.add("MVIEW$_ADV_TEMP");
        keys.add(localSession.buildKeyWithName(dataSetName,keyColumns,newValues));
        newValues.clear();
        newValues.add("SYSTEM");
        newValues.add("MVIEW$_ADV_FILTER");
        keys.add(localSession.buildKeyWithName(dataSetName,keyColumns,newValues));
        
        int delNum = localSession.batchDeleteData(dataSetName, keys, false);
        System.out.println(delNum+" entries deleted!");

        ///*
        ArrayList datas = new ArrayList();
        //datas.add(newValues);
        newValues = new ArrayList();
        newValues.add("SYSTEM2");
        newValues.add("DEMO");
        newValues.add("ACE2");
        datas.add(newValues);
        newValues = new ArrayList();
        newValues.add("SYSTEM3");
        newValues.add("DEMO");
        newValues.add("ACE3");
        datas.add(newValues);
        newValues = new ArrayList();
        newValues.add("SYSTEM4");
        newValues.add("DEMO");
        newValues.add("ACE4");
        datas.add(newValues);
        newValues = new ArrayList();
        newValues.add("SYSTEM5");
        newValues.add("DEMO");
        newValues.add("ACE5");
        datas.add(newValues);
        newValues = new ArrayList();
        newValues.add("SYSTEM6");
        newValues.add("DEMO");
        newValues.add("ACE6");
        datas.add(newValues);
        newValues = new ArrayList();
        newValues.add("SYSTEM7");
        newValues.add("DEMO");
        newValues.add("ACE7");
        datas.add(newValues);
        localSession.batchInsertData(dataSetName, colNames, datas, false);
        
        HashMap dataSetKey = localSession.buildKeyWithName(dataSetName, keyColumns, keyData);
        localSession.modifyDataByKeyS(dataSetName, dataSetKey,colNames,newValues);
        val = localSession.singleRead(dataSetName, dataSetKey);
        if (val == null)
           System.out.println("no data found");
        else {
           Iterator it = val.values().iterator(); 
           while(it.hasNext()) {
               String s = (String)it.next();
               System.out.print(s+",");
           }
           System.out.println();
        }
        keys = new HashSet();
        for (int i=0; i<10; i++){
           keyData = (ArrayList)rdKeys.get(r.nextInt(100));
           keys.add(localSession.buildKeyWithName(dataSetName, keyColumns, keyData));
        }
        localSession.batchModifyMemDataByKey(dataSetName,keys,colNames,newValues, true);
        dataSetKey = localSession.buildKeyWithName(dataSetName, keyColumns, keyData);
        localSession.modifyDataByKeyS(dataSetName, dataSetKey,colNames,newValues);
        val = localSession.singleRead(dataSetName, dataSetKey);
        if (val == null)
           System.out.println("no data found");
        else {
           Iterator it = val.values().iterator(); 
           while(it.hasNext()) {
               String s = (String)it.next();
               System.out.print(s+",");
           }
           System.out.println();
        }
        newValues.remove(newValues.size()-1);
        newValues.add("AQ$_QUEUES");
        dataSetKey = localSession.buildKeyWithName(dataSetName, keyColumns, keyData);
        localSession.modifyDataByKeyS(dataSetName, dataSetKey,colNames,newValues);
        val = localSession.singleRead(dataSetName, dataSetKey);
        if (val == null)
           System.out.println("no data found");
        else {
           Iterator it = val.values().iterator(); 
           while(it.hasNext()) {
               String s = (String)it.next();
               System.out.print(s+",");
           }
           System.out.println();
        }
        //newValues.remove(newValues.size()-1);
        //colNames.add("OWNER");
        //newValues.add("DEMO");
        keys = new HashSet();
        for (int i=0; i<10; i++){
           keyData = (ArrayList)rdKeys.get(r.nextInt(100));
           keys.add(localSession.buildKeyWithName(dataSetName, keyColumns, keyData));
        }
        localSession.batchModifyMemDataByKey(dataSetName,keys,colNames,newValues,false);
        newValues.remove(newValues.size()-1);
        newValues.add("AQ$_QUEUES1");
        dataSetKey = localSession.buildKeyWithName(dataSetName, keyColumns, keyData);
        localSession.modifyDataByKeyS(dataSetName, dataSetKey,colNames,newValues);
        val = localSession.singleRead(dataSetName, dataSetKey);
        if (val == null)
           System.out.println("no data found");
        else {
           Iterator it = val.values().iterator(); 
           while(it.hasNext()) {
               String s = (String)it.next();
               System.out.print(s+",");
           }
           System.out.println();
        }
    }

    public void test6(){
        //miniDB.implementLogs();
        //*/
        /*
        DriverManager.registerDriver (new oracle.jdbc.driver.OracleDriver());
        Connection dbconn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:edgar","demo","demo");
        DatabaseMetaData dbMeta = dbconn.getMetaData();
        System.out.println(dbconn.getCatalog());
        System.out.println(dbconn.toString());
        System.out.println(dbMeta.getDatabaseProductName());
        System.out.println(dbMeta.getURL());
        System.out.println(dbMeta.getUserName());
        if (true)
            return;
        
        ArrayList keyColumns = new ArrayList();
        keyColumns.add("OWNER");keyColumns.add("OBJECT_NAME");
        FyDataSet testData = new FyDataSet(phyTypes.DB_Oracle, memTypes.HashMap, dbconn, "T_TEST1", keyColumns);
        testData.loaddata();
        SimpleDateFormat tempDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
        String datetime = tempDate.format(new java.util.Date());
        System.out.println("memget begin: "+datetime);
        Random r = new Random();
        for (int i=0; i<1000; i++)
        {
            ArrayList key = (ArrayList)rdKeys.get(r.nextInt(100));
            ArrayList val = testData.getMemDataByKey(key);
        }
        datetime = tempDate.format(new java.util.Date());
        System.out.println("memget end: "+datetime);

        Statement stmt = dbconn.createStatement(); 
        datetime = tempDate.format(new java.util.Date());
        System.out.println("dbget begin: "+datetime);
        for (int i=0; i<1000; i++)
        {
            ArrayList key = (ArrayList)rdKeys.get(r.nextInt(100));
            PreparedStatement ps = dbconn.prepareStatement("select * from t_test1 where owner=? and object_name=?");
            ps.setString(1, (String)key.get(0));ps.setString(2, (String)key.get(1));
            ResultSet rset = ps.executeQuery ();
            rset.next();
            rset.close();
            ps.close();
        }
        datetime = tempDate.format(new java.util.Date());
        System.out.println("dbget end: "+datetime);
        //for (int i=0; i< val.size(); i++)
        //    System.out.print(val.get(i)+";");
        //System.out.println();
        }catch (SQLException e)
        {
        e.printStackTrace();
        }
        */
    }

    public static void main(String[] args) {
        Main main = new Main();

        //main.test1();
        //main.test2();
        Manager miniDB = new Manager();
        if (!miniDB.launch())
            return;
        //main.test3(miniDB);
        //main.test4(miniDB);
        //main.test5(miniDB, "T_TEST2");
        main.test5(miniDB, "DB_Oracle/1712582900/DEMO/T_TEST2");
        //main.test6();

        miniDB.shutdown();
    }
}
