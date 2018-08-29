/**
 * @(#)InitParas.java	0.01 11/04/19
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_comm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.HashMap;
import java.util.HashSet;

public class InitParas {
    private static HashMap paras = new HashMap();

    private static HashSet intParas = new HashSet();

    //public static long _spinTime;
    //public static long _bpSpinTime;
    //public static int  _recycleSessionInterval; // recycle closed sessions interval time
    //public static long _logApplyInteval;  // log implement interval
    //public static long logFileSize;      // log file size limitation
    //public static int  logNumber;        // log file number
    //public static int  maxSessionNumber; // maximum session number
    //public static String baseDir;
    //public static String traceDir;
    //public static String logDir;
    //public static String dataDir;
    //public static String dbKeyFile;     // ssl key file name;
    //public static String dbTrustFile;   // ssl trust file name;
    //public static String dbPassword;    // ssl password;
    //public static String dbHostAddr;    // ssl host ip address;
    //public static InetAddress _dbHostAddr;   // ssl host address;
    //public static int dbGatePort;          // server gate port

    //public static String imKeyFile;     // ssl key file name;
    //public static String imTrustFile;   // ssl trust file name;
    //public static String imPassword;    // ssl password;
    //public static String imHostAddr;    // ssl host ip address;
    //public static InetAddress _imHostAddr;    // ssl host address;
    //public static int imGatePort;          // server gate port
    //public static String[] buddyServers;   // buddy server list
    
    //private static long _logApplyInteval;  // internal limitation of log implement interval
    //private static long _logFileSize;      // internal limitation of log file size limitation
    //private static int  _logNumber;        // internal limitation of log file number
    //private static int  _maxSessionNumber; // internal limitation of maximum session number
    //public static int  _blockSize;         // block size of data file physical source
    //public static int  _minBlockSize;      // minimum block size of data file physical source
    //public static int  _multiReadBlockNum; // multiple read blocks number
    //public static String _defaultEncoding; // default encoding
    //public static String _dataFileExtention; // data file extention
    //public static int _flushNumber;       // block numbers to synchronize to disk

    private static boolean initialized = false;

    public InitParas() {
        if (initialized)
            return;

        intParas.add("_spinTime".toUpperCase());
        intParas.add("_bpSpinTime".toUpperCase());
        intParas.add("_spinTime".toUpperCase());
        intParas.add("_logApplyInteval".toUpperCase());
        intParas.add("_recycleSessionInterval".toUpperCase());
        intParas.add("logFileSize".toUpperCase());
        intParas.add("logNumber".toUpperCase());
        intParas.add("maxSessionNumber".toUpperCase());

        intParas.add("dbGatePort".toUpperCase());
        intParas.add("imGatePort".toUpperCase());
        intParas.add("_blockSize".toUpperCase());
        intParas.add("_minBlockSize".toUpperCase());
        intParas.add("_flushNumber".toUpperCase());
        intParas.add("_multiReadBlockNum".toUpperCase());
        
        loadParameters();

        initialized = true;
    }

    private void putParameter(String paraName, Object paraValue){
        paras.put(paraName.toUpperCase(),paraValue);
    }

    public Object getParameter(String paraName){
        return paras.get(paraName.toUpperCase());
    }

    public void setParameter(String paraName, String paraValue){
        if (intParas.contains(paraName.toUpperCase())){
            try{
                int intVal = Integer.parseInt(paraValue);
                putParameter(paraName, new Integer(intVal));
            }catch(NumberFormatException e){
            }
        }else if ("buddyServers".equalsIgnoreCase(paraName)){
            String[] buddyServers = CommUtility.splitString(paraValue,',',true);
            if (buddyServers != null){
                for (int i=0;i<buddyServers.length;i++)
                    buddyServers[i] = new String(buddyServers[i].trim());
                putParameter("buddyServers",buddyServers);
            }
        }else
            putParameter(paraName,paraValue);

        if ("dbHostAddr".equalsIgnoreCase(paraName) )
            putParameter("_dbHostAddr",CommUtility.getLocalAddrByIP((String)getParameter("imHostAddr")));
        else if ("imHostAddr".equalsIgnoreCase(paraName) )
            putParameter("_imHostAddr",CommUtility.getLocalAddrByIP((String)getParameter("imHostAddr")));
    }
    
    private void setDefault(){
        //--------------------- internal parameters begin-----------------------------------------
        // locker enqueue spin time
        if (getParameter("logFileSize")==null)
            putParameter("logFileSize",new Integer(5*1024*1024)); // 5M
        if (getParameter("logNumber")==null)
            putParameter("logNumber",new Integer(9));
        if (getParameter("maxSessionNumber")==null)
            putParameter("maxSessionNumber",new Integer(360));
        if (getParameter("baseDir")==null)
            putParameter("baseDir",System.getProperty("user.dir")); // default is application dir
        if (getParameter("traceDir")==null)
            putParameter("traceDir",(String)getParameter("baseDir") + File.separator + "trace"); // default is application dir
        if (getParameter("logDir")==null)
            putParameter("logDir",(String)getParameter("baseDir") + File.separator + "log"); // default is application dir
        if (getParameter("dataDir")==null)
            putParameter("dataDir", (String)getParameter("baseDir") + File.separator + "data"); // default is application dir
        
        if (getParameter("dbKeyFile")==null)
            putParameter("dbKeyFile","sslKey");
        if (getParameter("dbTrustFile")==null)
            putParameter("dbTrustFile","sslTrust");
        if (getParameter("dbPassword")==null)
            putParameter("dbPassword", "");
        if (getParameter("dbHostAddr")==null){
            putParameter("dbHostAddr","127.0.0.1");
            putParameter("_dbHostAddr",CommUtility.getLocalAddrByIP((String)getParameter("dbHostAddr")));
        }
        if (getParameter("dbGatePort")==null)
            putParameter("dbGatePort",new Integer(6636));

        if (getParameter("imKeyFile")==null)
            putParameter("imKeyFile","imKey");
        if (getParameter("imTrustFile")==null)
            putParameter("imTrustFile","imTrust");
        if (getParameter("imPassword")==null)
            putParameter("imPassword","");
        if (getParameter("imHostAddr")==null){
            putParameter("imHostAddr","127.0.0.1");
            putParameter("_imHostAddr",CommUtility.getLocalAddrByIP((String)getParameter("imHostAddr")));
        }
        if (getParameter("imGatePort")==null)
            putParameter("imGatePort",new Integer(6366));

        if (getParameter("_spinTime")==null)
            putParameter("_spinTime",new Integer(100)); // 100 micro-seconds
        if (getParameter("_bpSpinTime")==null)
            putParameter("_bpSpinTime",new Integer(1)); // 1 micro-seconds
        if (getParameter("_logApplyInteval")==null)
            putParameter("_logApplyInteval",new Integer(3000)); // 3 seconds
        if (getParameter("_recycleSessionInterval")==null)
            putParameter("_recycleSessionInterval",new Integer(3000)); // 3 seconds
        if (getParameter("_blockSize")==null)
            putParameter("_blockSize",new Integer(8192));
        if (getParameter("_minBlockSize")==null)
            putParameter("_minBlockSize",new Integer(2048));
        if (getParameter("_defaultEncoding")==null)
            putParameter("_defaultEncoding", "UTF-8");
        if (getParameter("_dataFileExtention")==null)
            putParameter("_dataFileExtention","dat");
        if (getParameter("_flushNumber")==null)
            putParameter("_flushNumber",new Integer(16));
        if (getParameter("_multiReadBlockNum")==null)
            putParameter("_multiReadBlockNum",new Integer(8));
        //--------------------- internal parameters end-----------------------------------------

        if (getParameter("imGatePort")==null){
            String[] buddyServers = new String[0];
            putParameter("buddyServers",buddyServers);
        }
    }

    // load paramters from file
    public boolean loadParameters(){
        boolean loadedFromFile = false;
        try{ // log file of a db source data: logDir/dbid/schema/<tablename>_<seqNum>.log. Where guid contains connectstring info and table info.
            File initFile = new File("parameters.ini");
            if (!initFile.exists()){
                initFile.getParentFile().mkdirs();
                initFile.createNewFile();
            }
            BufferedReader initReader = new BufferedReader(new InputStreamReader(new FileInputStream(initFile.getCanonicalPath())));
            String paraStr = initReader.readLine();
            while (paraStr!=null){
                if (!paraStr.trim().startsWith("#")){
                    String[] paraInfo = CommUtility.splitString(paraStr,'=',true);
                    if (paraInfo != null && paraInfo.length==2)
                        setParameter(new String(paraInfo[0].trim()),new String(paraInfo[1].trim()));
                }
                paraStr = initReader.readLine();
            }
            initReader.close();
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
        setDefault();
        return true;
    }
    
    // save parameters to file
    public void saveParameters(){
    }
    
    public void initialize(){
    }
}
