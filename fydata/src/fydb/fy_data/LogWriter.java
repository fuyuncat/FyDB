/*
 * @(#)LogWriter.java	0.01 11/05/16
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

public class LogWriter{
    protected InitParas paras;
    protected Tracer dtrace;
    protected Debuger debuger;

    private int actionNum = 0; // number of concurrent action on logwriter 
    private boolean quiesced = false;

    private File logFile;
    private BufferedWriter bufWriter;

    private byte[] lock = new byte[0]; // used for synchronize

    public LogWriter(InitParas paras, Tracer dtrace, Debuger debuger) {
        this.paras = paras;
        this.dtrace = dtrace;
        this.debuger = debuger;
    }
    
    private boolean requireAction(){
        synchronized(lock){
            while (quiesced){ // if quiesced, no new action allow
                try{
                    Thread.sleep((Integer)paras.getParameter("_spinTime"));
                }
                catch (InterruptedException e) {
                    dtrace.trace(e.getMessage());
                    dtrace.trace(101);
                    return false;
                }
            }
            actionNum++;
        }
        return true;
    }

    private boolean releaseAction(){
        synchronized(lock){
            actionNum--;
        }
        return true;
    }

    private boolean quiece(){
        if (quiesced) {
            dtrace.trace(33);
            return false;
        }

        quiesced = true;
        while (actionNum > 0){
            try{
                Thread.sleep((Integer)paras.getParameter("_spinTime"));
            }
            catch (InterruptedException e) {
                dtrace.trace(e.getMessage());
                dtrace.trace(101);
                quiesced = false;
                return false;
            }
        }
        return true;
    }

    private void unquiece(){
        quiesced = false;
    }

    // open a log file for writing. it should be called once. if want to open another file, call switchWriter
    public boolean openWriter(String fileName){
        if (!quiece())
            return false;
        if (bufWriter != null && logFile != null) // log file has been opened
            return true;
        boolean newFile = false;
        try{ // log file of a db source data: logDir/dbid/schema/<tablename>_<seqNum>.log. Where guid contains connectstring info and table info.
            logFile = new File(fileName);
            if (!logFile.exists()){
                dtrace.trace(fileName);
                dtrace.trace(16);
                logFile.getParentFile().mkdirs();
                logFile.createNewFile();
                newFile = true;
            }
            bufWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logFile.getCanonicalPath(), true)));
            if (newFile){
                char dataEntry[] = new char[256]; // keep a head
                bufWriter.write(dataEntry);
                bufWriter.flush();
            }
        }catch (Exception e){
            dtrace.trace(fileName);
            dtrace.trace(7);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return false;
        }finally{
            unquiece();
        }
        return true;
    }

    public boolean switchWriter(String fileName){
        if (!quiece())
            return false;
        try{ // log file of a db source data: logDir/dbid/schema/<tablename>_<seqNum>.log. Where guid contains connectstring info and table info.
            bufWriter.flush();
            bufWriter.close();
            logFile = new File(fileName);
            if (!logFile.exists()){
                dtrace.trace(fileName);
                dtrace.trace(16);
                logFile.getParentFile().mkdirs();
                logFile.createNewFile();
            }
            // over write existing file
            bufWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logFile.getCanonicalPath(), false)));
            char dataEntry[] = new char[256]; // keep a head
            bufWriter.write(dataEntry);
            bufWriter.flush();
        }catch (Exception e){
            dtrace.trace(fileName);
            dtrace.trace(7);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return false;
        }finally{
            unquiece();
        }
        return true;
    }
    
    public long getFileSize(){
        if (logFile != null)
            return logFile.length();
        else
            return 0;
    }

    public boolean write(String strBuf) throws IOException{
        if (requireAction()){
            bufWriter.write(strBuf);
            releaseAction();
            return true;
        }else
            return false;
    }

    public boolean write(char[] chBuf) throws IOException{
        if (requireAction()){
            bufWriter.write(chBuf);
            releaseAction();
            return true;
        }else
            return false;
    }

    public boolean flush() throws IOException{
        if (requireAction()){
            bufWriter.flush();
            releaseAction();
            return true;
        }else
            return false;
    }

    public boolean close() throws IOException{
        if (requireAction()){
            bufWriter.close();
            releaseAction();
            return true;
        }else
            return false;
    }
}
