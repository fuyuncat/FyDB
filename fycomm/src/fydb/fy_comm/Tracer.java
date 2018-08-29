/*
 * @(#)Tracer.java	0.01 11/04/19
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_comm;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

import java.text.SimpleDateFormat;

import java.util.HashMap;
import java.util.Map;

public class Tracer {
    private class errinfo{
        public String msg;
        public int level; // 1: info; 2: warning; 3:app error; 4: sys error; 5:fatal
    }

    protected InitParas paras = new InitParas();
    
    private int lastErrNo = 0;
    private String lastAddMsg = "";
    
    private boolean writeToFile;
    private static BufferedWriter tracefile;
    private static Map <Integer, errinfo> errarray = new HashMap();
    private static boolean initialized = false;

    public Tracer(boolean writeToFile) {
        if (initialized)
            return;
        this.writeToFile = writeToFile;
        errinfo err = new errinfo();
        err.msg = "";err.level=1;errarray.put(new Integer(0),err);
        err = new errinfo();err.msg = "not find initial parameter file";err.level=5;errarray.put(new Integer(1),err);
        err = new errinfo();err.msg = "called an incorrect dataset constructor for physical data";err.level=4;errarray.put(new Integer(2),err);
        err = new errinfo();err.msg = "called an incorrect initlaize process of physical data";err.level=4;errarray.put(new Integer(5),err);
        err = new errinfo();err.msg = "finalize memData failed";err.level=4;errarray.put(new Integer(6),err);
        err = new errinfo();err.msg = "create log file failed";err.level=4;errarray.put(new Integer(7),err);
        err = new errinfo();err.msg = "internal un handled error!";err.level=4;errarray.put(new Integer(10),err);
        err = new errinfo();err.msg = "internal error, acceccer number less than zero!";err.level=4;errarray.put(new Integer(11),err);
        err = new errinfo();err.msg = "internal error, consistet get null!";err.level=4;errarray.put(new Integer(12),err);
        err = new errinfo();err.msg = "internal error, bp enqueue spin failed";err.level=4;errarray.put(new Integer(13),err);
        err = new errinfo();err.msg = "load system metadata file failed";err.level=4;errarray.put(new Integer(14),err);
        err = new errinfo();err.msg = "System metadata file not exist, will create a new one";err.level=2;errarray.put(new Integer(15),err);
        err = new errinfo();err.msg = "Log file not exist, will create a new one";err.level=2;errarray.put(new Integer(16),err);
        err = new errinfo();err.msg = "metadata read failed, will adopt the defualt value";err.level=2;errarray.put(new Integer(17),err);
        err = new errinfo();err.msg = "metadata write failed";err.level=4;errarray.put(new Integer(18),err);
        err = new errinfo();err.msg = "Fydb System initialize failed!";err.level=5;errarray.put(new Integer(19),err);
        err = new errinfo();err.msg = "Fydb System is not opened!";err.level=4;errarray.put(new Integer(20),err);
        err = new errinfo();err.msg = "Log control file not exist, will create a new one";err.level=2;errarray.put(new Integer(21),err);
        err = new errinfo();err.msg = "Log control data read failed, will adopt the defualt value";err.level=2;errarray.put(new Integer(22),err);
        err = new errinfo();err.msg = "load Log control file failed";err.level=4;errarray.put(new Integer(23),err);
        err = new errinfo();err.msg = "read log entry failed";err.level=4;errarray.put(new Integer(24),err);
        err = new errinfo();err.msg = "log file not exist";err.level=4;errarray.put(new Integer(25),err);
        err = new errinfo();err.msg = "log file corrupted!";err.level=4;errarray.put(new Integer(26),err);
        err = new errinfo();err.msg = "Fydb System is opened already!";err.level=4;errarray.put(new Integer(27),err);
        err = new errinfo();err.msg = "Log leader sleep failed!";err.level=4;errarray.put(new Integer(28),err);
        err = new errinfo();err.msg = "Log applying failed!";err.level=4;errarray.put(new Integer(29),err);
        err = new errinfo();err.msg = "Dataset initial xml file not found, no dataset will be loaded automaticly";err.level=2;errarray.put(new Integer(30),err);
        err = new errinfo();err.msg = "Parse dataset initial xml file failed";err.level=4;errarray.put(new Integer(31),err);
        err = new errinfo();err.msg = "Read dataset initial xml file failed";err.level=4;errarray.put(new Integer(32),err);
        err = new errinfo();err.msg = "Log writer is quiesced";err.level=4;errarray.put(new Integer(33),err);
        err = new errinfo();err.msg = "Excceed maximum session number limitation";err.level=4;errarray.put(new Integer(34),err);
        err = new errinfo();err.msg = "Session leader is not initialized";err.level=4;errarray.put(new Integer(35),err);
        err = new errinfo();err.msg = "Thread sleep failed";err.level=3;errarray.put(new Integer(36),err);
        err = new errinfo();err.msg = "Unknown message type from client";err.level=4;errarray.put(new Integer(37),err);

        err = new errinfo();err.msg = "locker enqueue spin failed";err.level=4;errarray.put(new Integer(101),err);
        err = new errinfo();err.msg = "object is quiesed, can not load data";err.level=4;errarray.put(new Integer(102),err);
        err = new errinfo();err.msg = "quiesce awake spin failed";err.level=4;errarray.put(new Integer(103),err);
        err = new errinfo();err.msg = "dataset is not initialized";err.level=4;errarray.put(new Integer(104),err);
        err = new errinfo();err.msg = "dataset release failed";err.level=4;errarray.put(new Integer(105),err);
        err = new errinfo();err.msg = "physical data release failed";err.level=4;errarray.put(new Integer(106),err);
        err = new errinfo();err.msg = "memory data release failed";err.level=4;errarray.put(new Integer(107),err);
        err = new errinfo();err.msg = "memory data type not assigned";err.level=4;errarray.put(new Integer(108),err);
        err = new errinfo();err.msg = "physical data type not assigned";err.level=4;errarray.put(new Integer(109),err);
        err = new errinfo();err.msg = "key column(s) not assigned";err.level=4;errarray.put(new Integer(110),err);
        err = new errinfo();err.msg = "physical database table name not assigned";err.level=4;errarray.put(new Integer(111),err);
        err = new errinfo();err.msg = "physical database connect string not assigned";err.level=4;errarray.put(new Integer(112),err);
        err = new errinfo();err.msg = "physical database username not assigned";err.level=4;errarray.put(new Integer(113),err);
        err = new errinfo();err.msg = "physical database password not assigned";err.level=4;errarray.put(new Integer(114),err);
        err = new errinfo();err.msg = "Index type invalid";err.level=4;errarray.put(new Integer(115),err);
        err = new errinfo();err.msg = "The column(s) have been indexed in such type index";err.level=4;errarray.put(new Integer(116),err);
        err = new errinfo();err.msg = "Index is not initialized, it will be removed from memory";err.level=4;errarray.put(new Integer(117),err);
        err = new errinfo();err.msg = "Prediction tree is invalid, root should contain left node";err.level=4;errarray.put(new Integer(118),err);
        err = new errinfo();err.msg = "Prediction tree is invalid, a branch should contain 2 leaves";err.level=4;errarray.put(new Integer(119),err);
        err = new errinfo();err.msg = "Specified column id does not exist in dataset";err.level=4;errarray.put(new Integer(120),err);
        err = new errinfo();err.msg = "Invalid filter node type";err.level=4;errarray.put(new Integer(121),err);
        err = new errinfo();err.msg = "Invalid index type for part scaning";err.level=4;errarray.put(new Integer(122),err);
        err = new errinfo();err.msg = "Physical data file not exist";err.level=4;errarray.put(new Integer(123),err);
        err = new errinfo();err.msg = "Physical data source not initialized";err.level=4;errarray.put(new Integer(124),err);
        err = new errinfo();err.msg = "Physical data is loading";err.level=4;errarray.put(new Integer(125),err);
        err = new errinfo();err.msg = "Physical data should be re-initialized for loading";err.level=4;errarray.put(new Integer(126),err);
        err = new errinfo();err.msg = "BMB block info missed";err.level=4;errarray.put(new Integer(127),err);
        err = new errinfo();err.msg = "DB meta data not matched with master server";err.level=4;errarray.put(new Integer(128),err);
        err = new errinfo();err.msg = "Password incorrect";err.level=3;errarray.put(new Integer(129),err);
        err = new errinfo();err.msg = "Insufficient privilege";err.level=3;errarray.put(new Integer(130),err);

        err = new errinfo();err.msg = "pysical data type format not supported";err.level=4;errarray.put(new Integer(202),err);
        err = new errinfo();err.msg = "memory data type format not supported";err.level=4;errarray.put(new Integer(203),err);
        err = new errinfo();err.msg = "connectting to physical data source failed";err.level=4;errarray.put(new Integer(204),err);
        err = new errinfo();err.msg = "duplicated dataset name detected";err.level=3;errarray.put(new Integer(205),err);
        err = new errinfo();err.msg = "specified dataset name is not found";err.level=3;errarray.put(new Integer(206),err);
        err = new errinfo();err.msg = "inputted key is null";err.level=2;errarray.put(new Integer(207),err);
        err = new errinfo();err.msg = "data is not loaded";err.level=3;errarray.put(new Integer(208),err);
        err = new errinfo();err.msg = "dataset is quiesced";err.level=3;errarray.put(new Integer(209),err);
        err = new errinfo();err.msg = "duplicated key detected";err.level=3;errarray.put(new Integer(210),err);
        err = new errinfo();err.msg = "writting log failed, data changed in memeory will be lost. Strongly suggest to reload data from physcial storage";err.level=3;errarray.put(new Integer(211),err);
        err = new errinfo();err.msg = "the changing key is duplicated ";err.level=3;errarray.put(new Integer(212),err);
        err = new errinfo();err.msg = "column number does not match value number";err.level=3;errarray.put(new Integer(213),err);
        err = new errinfo();err.msg = "column to be modified not assigned";err.level=3;errarray.put(new Integer(214),err);
        err = new errinfo();err.msg = "no value assigned to modifying column";err.level=3;errarray.put(new Integer(215),err);
        err = new errinfo();err.msg = "key column names are not assigned";err.level=3;errarray.put(new Integer(216),err);
        err = new errinfo();err.msg = "key values are not assigned";err.level=3;errarray.put(new Integer(217),err);
        err = new errinfo();err.msg = "key value number does not match key column number";err.level=3;errarray.put(new Integer(218),err);
        err = new errinfo();err.msg = "key column number is incorrect";err.level=3;errarray.put(new Integer(219),err);
        err = new errinfo();err.msg = "one or more columns is not key column";err.level=3;errarray.put(new Integer(220),err);
        err = new errinfo();err.msg = "dataset name can not be null";err.level=3;errarray.put(new Integer(221),err);
        err = new errinfo();err.msg = "metadata missed";err.level=3;errarray.put(new Integer(222),err);
        err = new errinfo();err.msg = "invalid long data";err.level=3;errarray.put(new Integer(223),err);
        err = new errinfo();err.msg = "invalid double data";err.level=3;errarray.put(new Integer(224),err);
        err = new errinfo();err.msg = "invalid date data, format must be yyyy-mm-dd hh:mm:ss.fffffffff";err.level=3;errarray.put(new Integer(225),err);
        err = new errinfo();err.msg = "invalid boolean data";err.level=3;errarray.put(new Integer(226),err);
        err = new errinfo();err.msg = "Not supportted data type";err.level=3;errarray.put(new Integer(227),err);
        err = new errinfo();err.msg = "No data found to be updated";err.level=3;errarray.put(new Integer(228),err);
        err = new errinfo();err.msg = "No key found to be updated";err.level=3;errarray.put(new Integer(229),err);
        err = new errinfo();err.msg = "No data found to be inserted";err.level=3;errarray.put(new Integer(230),err);
        err = new errinfo();err.msg = "No key found to be inserted";err.level=3;errarray.put(new Integer(231),err);
        err = new errinfo();err.msg = "No data found to be deleted";err.level=3;errarray.put(new Integer(232),err);
        err = new errinfo();err.msg = "No key found to be deleted";err.level=3;errarray.put(new Integer(233),err);
        err = new errinfo();err.msg = "insert statement is not generated";err.level=3;errarray.put(new Integer(234),err);
        err = new errinfo();err.msg = "delete statement is not generated";err.level=3;errarray.put(new Integer(235),err);
        err = new errinfo();err.msg = "key column IDs are null";err.level=3;errarray.put(new Integer(236),err);
        err = new errinfo();err.msg = "can not insert null key";err.level=3;errarray.put(new Integer(237),err);
        err = new errinfo();err.msg = "No data assigned to be inserted";err.level=3;errarray.put(new Integer(238),err);
        err = new errinfo();err.msg = "column to be inserted not assigned";err.level=3;errarray.put(new Integer(239),err);
        err = new errinfo();err.msg = "no value assigned to inserting column";err.level=3;errarray.put(new Integer(240),err);
        err = new errinfo();err.msg = "can not insert duplicated key";err.level=3;errarray.put(new Integer(241),err);
        err = new errinfo();err.msg = "key column IDs are not assigned";err.level=3;errarray.put(new Integer(242),err);
        err = new errinfo();err.msg = "Inpute key column number does not match input key value number";err.level=3;errarray.put(new Integer(243),err);
        err = new errinfo();err.msg = "key of data to be deleted is null";err.level=3;errarray.put(new Integer(244),err);
        err = new errinfo();err.msg = "no data will be inserted";err.level=2;errarray.put(new Integer(245),err);
        err = new errinfo();err.msg = "no data will be deleted";err.level=2;errarray.put(new Integer(246),err);
        err = new errinfo();err.msg = "values number to be inserted does not match columns number";err.level=3;errarray.put(new Integer(247),err);
        err = new errinfo();err.msg = "column not allow null values";err.level=3;errarray.put(new Integer(248),err);
        err = new errinfo();err.msg = "duplicated dataset detected";err.level=3;errarray.put(new Integer(249),err);
        err = new errinfo();err.msg = "Private current logs should be allocated";err.level=3;errarray.put(new Integer(250),err);
        err = new errinfo();err.msg = "invalid Integer data";err.level=3;errarray.put(new Integer(251),err);
        err = new errinfo();err.msg = "invalid String data";err.level=3;errarray.put(new Integer(252),err);
        err = new errinfo();err.msg = "Join columns number not matched";err.level=4;errarray.put(new Integer(253),err);
        err = new errinfo();err.msg = "Join columns data types not matched";err.level=4;errarray.put(new Integer(254),err);
        err = new errinfo();err.msg = "Load RDBMS driver failed!";err.level=4;errarray.put(new Integer(255),err);

        err = new errinfo();err.msg = "initializing external db table failed";err.level=4;errarray.put(new Integer(501),err);
        err = new errinfo();err.msg = "geting data from db table failed";err.level=4;errarray.put(new Integer(502),err);
        err = new errinfo();err.msg = "one or more specified key column not exist in db table";err.level=4;errarray.put(new Integer(503),err);
        err = new errinfo();err.msg = "db column data type not supportted";err.level=4;errarray.put(new Integer(504),err);
        err = new errinfo();err.msg = "column does not exist";err.level=4;errarray.put(new Integer(505),err);
        err = new errinfo();err.msg = "synchronize data to data source db failed";err.level=4;errarray.put(new Integer(506),err);

        err = new errinfo();err.msg = "log operation type error";err.level=4;errarray.put(new Integer(1001),err);
        err = new errinfo();err.msg = "log sequence error";err.level=4;errarray.put(new Integer(1002),err);

        err = new errinfo();err.msg = "Set socket failed";err.level=4;errarray.put(new Integer(2001),err);
        err = new errinfo();err.msg = "Reading net message failed";err.level=4;errarray.put(new Integer(2002),err);
        err = new errinfo();err.msg = "Writing net message failed";err.level=4;errarray.put(new Integer(2003),err);
        err = new errinfo();err.msg = "Build server gate socket failed";err.level=4;errarray.put(new Integer(2004),err);
        err = new errinfo();err.msg = "Allocate session socket failed";err.level=4;errarray.put(new Integer(2005),err);
        err = new errinfo();err.msg = "Build client gate socket factory failed";err.level=4;errarray.put(new Integer(2006),err);
        err = new errinfo();err.msg = "Build client gate socket failed";err.level=4;errarray.put(new Integer(2007),err);
        err = new errinfo();err.msg = "close socket failed";err.level=4;errarray.put(new Integer(2008),err);
        err = new errinfo();err.msg = "Build communicate server gate socket failed";err.level=4;errarray.put(new Integer(2009),err);
        err = new errinfo();err.msg = "Communicate client has connected";err.level=4;errarray.put(new Integer(2010),err);
        err = new errinfo();err.msg = "Buddy not found";err.level=2;errarray.put(new Integer(2011),err);
        err = new errinfo();err.msg = "Synchronize data to other servers failed!";err.level=2;errarray.put(new Integer(2012),err);
        err = new errinfo();err.msg = "Connect to buddy failed";err.level=2;errarray.put(new Integer(2013),err);
        err = new errinfo();err.msg = "Network disconnected";err.level=2;errarray.put(new Integer(2014),err);
        err = new errinfo();err.msg = "Network exception";err.level=2;errarray.put(new Integer(2015),err);

        err = new errinfo();err.msg = "Did not connect to server";err.level=4;errarray.put(new Integer(5001),err);
        err = new errinfo();err.msg = "Session is running in synchronized mode";err.level=4;errarray.put(new Integer(5002),err);
        err = new errinfo();err.msg = "Session is running in local and synchronized mode";err.level=4;errarray.put(new Integer(5003),err);
        err = new errinfo();err.msg = "Thread is running";err.level=4;errarray.put(new Integer(5004),err);
        err = new errinfo();err.msg = "No asynchronized call is running";err.level=2;errarray.put(new Integer(5005),err);
        err = new errinfo();err.msg = "Asynchronized call is running";err.level=2;errarray.put(new Integer(5006),err);
        err = new errinfo();err.msg = "Asynchronized call is parparing";err.level=2;errarray.put(new Integer(5007),err);
        err = new errinfo();err.msg = "Asynchronized call is failed";err.level=2;errarray.put(new Integer(5008),err);
        err = new errinfo();err.msg = "Asynchronized call parameter number wrong";err.level=4;errarray.put(new Integer(5009),err);
        err = new errinfo();err.msg = "Asynchronized call parameter error";err.level=4;errarray.put(new Integer(5010),err);
        err = new errinfo();err.msg = "Unknown asynchronized call";err.level=4;errarray.put(new Integer(5011),err);
        err = new errinfo();err.msg = "No session available in pool";err.level=4;errarray.put(new Integer(5012),err);

        if (tracefile == null && writeToFile){
            try{
                File dummmyfile = new File((String)paras.getParameter("traceDir")+File.separator+"fydb.trc");
                if (!dummmyfile.exists()) {
                    dummmyfile.getParentFile().mkdirs();
                    dummmyfile.createNewFile();
                }
                tracefile = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dummmyfile.getCanonicalPath(), true)));
            }
            catch (Exception e){
                System.out.println(getCurTimestamp() + ": " + "Warining: creating trace file failed!");
                e.printStackTrace();
                return;
            }
        }
        
        initialized = true;
    }
    
    private String getCurTimestamp(){
        SimpleDateFormat tempDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
        return tempDate.format(new java.util.Date());
    }
    
    // 1: info; 2: warning; 3:app error; 4:sys error; 5:fatal
    private String decodeLevel(int level){
        switch (level){
        case 1:
            return "info";
        case 2:
            return "warning";
        case 3:
            return "App error";
        case 4:
            return "Sys error";
        case 5:
            return "fatal";
        default:
            return "";
        }
    }

    public void trace(int errno){
        Integer intObj = new Integer(errno);
        errinfo err = errarray.get(intObj);
        if (err == null)
            return;
        lastErrNo = errno;
        //System.out.println(decodeLevel(err.level) + ": " + err.msg);
        String msg = getCurTimestamp() + "(" +decodeLevel(err.level) + "): " + err.msg;
        if (err.level>2) {
            if (err.level>3 && writeToFile) {
                try{
                    tracefile.write(getCurTimestamp() + "(" +decodeLevel(err.level) + "): " + err.msg + "\n");
                    tracefile.flush();
                }
                catch (Exception e){
                    System.out.println(getCurTimestamp() + ": " + "Warining: trace file not created!");
                    e.printStackTrace();
                    return;
                }
            }
            Exception ex = new Exception(msg);
            ex.printStackTrace();
        }else{
            System.out.println(msg);
        }
    }

    public void trace(String msg){
        lastAddMsg = msg;
        System.out.println(msg);
        if (writeToFile)
            try{
                tracefile.write(getCurTimestamp() + ": " + msg + "\n");
                tracefile.flush();
            }
            catch (Exception e){
                System.out.println(getCurTimestamp() + ": " + "Warining: trace file not created!");
                e.printStackTrace();
                return;
            }
    }
    
    public String getLastErrorMsg(){
        Integer intObj = new Integer(lastErrNo);
        errinfo err = errarray.get(intObj);
        if (err == null)
            return null;
        else 
            return err.msg;
    }
}
