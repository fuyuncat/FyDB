/**
 * @(#)SysMetaData.java	0.01 11/04/19
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_data;

import fydb.fy_comm.Debuger;
import fydb.fy_comm.InitParas;
import fydb.fy_comm.Tracer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import java.util.Date;


public final class SysMetaData {
    public static String version;
    public static BP lastBP = new BP();

    private Tracer dtrace;
    private InitParas paras;
    private Debuger debuger;

    private String sysMetaDataFile;
   //private static boolean fileLoaded = false;
    private int bufLen = 16;

    public SysMetaData(InitParas paras, Tracer dtrace, Debuger debuger) {
        this.paras = paras;
        this.dtrace = dtrace;
        this.debuger = debuger;
        sysMetaDataFile = (String)paras.getParameter("baseDir") + File.separator + "fydb.sys";
    }

    public static void assignLastBp(BP bp) {
        lastBP = new BP(bp);
    }

    /*  1~16(1): version
     * 17~32(2): lastBP.level0
     * 33~48(3): lastBP.level1
     * 49~64(4): lastBP.level2
     * 4 sections so far
     */
    public boolean loadMetaData() {
        int maxSection = 4;
        int readSection = 0;
        char dataEntry[] = new char[bufLen];
        try {
            // read metadata from file.
            version = "0.0.1";
            // default
            long level0 = 0;
            long level1 = 0;
            Date date = new Date();
            long level2 = date.getTime();
            lastBP = new BP(level0, level1, level2);
            File dummmyfile = new File(sysMetaDataFile);
            if (!dummmyfile.exists()) {
                dtrace.trace(sysMetaDataFile);
                dtrace.trace(15);
                return true;
            }
            BufferedReader dataReader =
                new BufferedReader(new InputStreamReader(new FileInputStream(dummmyfile
                                                                                                     .getCanonicalPath())));
            while (dataReader.read(dataEntry, 0, bufLen) > -1 &&
                   readSection < maxSection) {
                readSection++;
                switch (readSection) {
                case 1 :
                    // version
                    version = String.valueOf(dataEntry).trim();
                    break;
                case 2 :
                    // lastBP.level0
                    lastBP.level0 =
                        Long.parseLong(String.valueOf(dataEntry).trim());
                    break;
                case 3 :
                    // lastBP.level1
                    lastBP.level1 =
                        Long.parseLong(String.valueOf(dataEntry).trim());
                    break;
                case 4 :
                    // lastBP.level2
                    lastBP.level2 =
                        Long.parseLong(String.valueOf(dataEntry).trim());
                    break;
                }
            }
            dataReader.close();
        } catch (Exception e) {
            dtrace.trace(17);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return true;
        }
        return true;
    }

    // save meta data to file
    public boolean saveMetaData() {
        try {
            // initialize reader and writer of metadata file.
            File dummmyfile = new File(sysMetaDataFile);
            if (!dummmyfile.exists()) {
                dtrace.trace(sysMetaDataFile);
                dtrace.trace(15);
                dummmyfile.getParentFile().mkdirs();
                dummmyfile.createNewFile();
            }
            BufferedWriter dataWriter =
                new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dummmyfile
                                                                                                       .getCanonicalPath(),
                                                                                                       false)));
            char dataEntry[] = new char[bufLen * 4];
            version.getChars(0, version.length(), dataEntry, 0);
            String tmpStr = String.valueOf(lastBP.level0);
            tmpStr.getChars(0, tmpStr.length(), dataEntry, bufLen * 1);
            tmpStr = String.valueOf(lastBP.level1);
            tmpStr.getChars(0, tmpStr.length(), dataEntry, bufLen * 2);
            tmpStr = String.valueOf(lastBP.level2);
            tmpStr.getChars(0, tmpStr.length(), dataEntry, bufLen * 3);
            dataWriter.write(dataEntry);
            dataWriter.flush();
            dataWriter.close();
            return true;
        } catch (Exception e) {
            dtrace.trace(sysMetaDataFile);
            dtrace.trace(14);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return false;
        }
    }

    protected void finalize() {
        saveMetaData();
    }
}
