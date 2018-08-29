/**
 * @(#)NetThread.java	0.01 11/05/16
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_comm;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.net.Socket;
import java.net.SocketAddress;

/**
 * @(#)NetThread.java	0.01 11/05/16
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
public class

NetThread extends Thread {
    private Socket netSocket;
    private ObjectInputStream netReader;
    private ObjectOutputStream netWriter;

    private static boolean working = false;

    protected InitParas paras;
    protected Tracer dtrace;
    protected Debuger debuger;

    protected boolean running = true;
    protected boolean sleeping = false;

    public NetThread(InitParas paras, Tracer dtrace, Debuger debuger) {
        this.paras = paras;
        this.dtrace = dtrace;
        this.debuger = debuger;
    }

    public boolean setServerSocket(Socket netSocket){
        this.netSocket = netSocket;
        try{
            netWriter = new ObjectOutputStream(new BufferedOutputStream(netSocket.getOutputStream()));
            netWriter.writeObject(new String("accepted")); // to create the ObjectInputStream in other side, it requires to send an object first
            netWriter.flush();
            netReader = new ObjectInputStream(new BufferedInputStream(netSocket.getInputStream()));
            String shakeHandMsg = (String)netReader.readObject();
            return true;
        }catch (Exception e){
            dtrace.trace(2001);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return false;
        }
    }

    public boolean setClientSocket(Socket netSocket){
        this.netSocket = netSocket;
        try{
            netReader = new ObjectInputStream(new BufferedInputStream(netSocket.getInputStream()));
            String shakeHandMsg = (String)netReader.readObject();
            netWriter = new ObjectOutputStream(new BufferedOutputStream(netSocket.getOutputStream()));
            netWriter.writeObject(new String("start")); // to create the ObjectInputStream in other side, it requires to send an object first
            netWriter.flush();
            return true;
        }catch (Exception e){
            dtrace.trace(2001);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return false;
        }
    }

    public void closeSocket(){
        try{
            if (netSocket != null)
                netSocket.close();
        }catch (IOException e){
            dtrace.trace(2008);
            return;
        }
    }
    
    public void interrupt(){
        running = false;
        closeSocket();
        super.interrupt();
    }

    // safely terminate the thread
    public void terminate() {
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

    // detect if net closed
    public boolean isClosed(){
        if (netSocket.isClosed())
            return true;
        int avaRead = 0;
        try {
            avaRead = netSocket.getInputStream().available();
        }catch(IOException e){
            return true;
        }
        return false;
    }
    
    // concurrent control
    public boolean requireChannel(){
        while(working){
            try{
                sleep(1);
            }catch(InterruptedException e){
                return false;
            }
        }
        working = true;
        return true;
    }

    // concurrent control
    public void releaseChannel(){
        working = false;
    }

    // read a object
    public Object readObject(){
        if (isClosed()){
            dtrace.trace(2014);
            return null;
        }
        try{
            return netReader.readObject();
        }catch (Exception e){
            dtrace.trace(2002);
            if (debuger.isDebugMode())
                e.printStackTrace();
            try{
                netSocket.close();
            }catch (IOException e1){
            }
            return null;
        }
    }

    // message exchange in byte[] stream
    // 1st byte is length of following stream, from 0~253; 
    // 254 means we should recieve 253 bytes and then continue recieve message;
    // 255 is saved
    // in loop
    // example: [02][031A]
    // example: [FE][031A...B2][03][128CDA91]
    public byte[] readBytes(){
        if (isClosed()){
            dtrace.trace(2014);
            return null;
        }
        try{
            byte[] bBuf;
            ByteArrayOutputStream tmpStream = new ByteArrayOutputStream(); // temp stream for storing message
            int len=1;
            bBuf = new byte[len];
            netReader.read(bBuf); // read length flag
            len = bBuf[0] & 0xFF;
            while (len == 254) { // continue reading intermedial content
                bBuf = new byte[253];
                netReader.read(bBuf); // read intermedial content
                tmpStream.write(bBuf,0,253);
                bBuf = new byte[1];
                netReader.read(bBuf); // read length flag
                len = bBuf[0] & 0xFF;
            }
            if (len > 0){
                bBuf = new byte[len];
                netReader.read(bBuf); // read the last content
                tmpStream.write(bBuf,0,len);
            }
            return tmpStream.toByteArray();
        }catch (IOException e){
            dtrace.trace(2002);
            if (debuger.isDebugMode())
                e.printStackTrace();
            try{
                netSocket.close();
            }catch (IOException e1){
            }
            return null;
        }
    }

    // write a object
    public void writeObject(Object obj){
        if (isClosed()){
            dtrace.trace(2014);
        }
        try{
            netWriter.writeObject(obj);
            netWriter.flush();
        }catch (Exception e){
            dtrace.trace(2003);
            if (debuger.isDebugMode())
                e.printStackTrace();
            try{
                netSocket.close();
            }catch (IOException e1){
            }
            return;
        }
    }

    // message exchange in byte[] stream
    // 1st byte is length of following stream, from 0~253; 
    // 254 means we should recieve 253 bytes and then continue recieve message;
    // 255 is saved
    // in loop
    // example: [02][031A]
    // example: [FE][031A...B2][03][128CDA91]
    public int writeBytes(byte[] message){
        if (isClosed()){
            dtrace.trace(2014);
        }
        try{
            int messageLen = message.length;
            int off = 0;
            byte[] tmpBytes;
            while (off < messageLen-253) { // continue wrting content
                Integer iLen = new Integer(254);
                tmpBytes = new byte[254];
                CommUtility.arrayCopy(new byte[]{iLen.byteValue()},0,tmpBytes,0,1);
                CommUtility.arrayCopy(message,off,tmpBytes,1,253);
                netWriter.write(tmpBytes);
                //netWriter.write(iLen.byteValue()); // write length flag
                //netWriter.write(message, off, 253); // write intermedial content
                off+=253;
            }
            if (messageLen != off){ // write the last content
                Integer iLen = new Integer(messageLen-off);
                tmpBytes = new byte[1+messageLen-off];
                CommUtility.arrayCopy(new byte[]{iLen.byteValue()},0,tmpBytes,0,1);
                CommUtility.arrayCopy(message,off,tmpBytes,1,messageLen-off);
                netWriter.write(tmpBytes);
                //netWriter.write(iLen.byteValue()); // write length flag
                //netWriter.write(message, off, messageLen-off); // write content
            }
            netWriter.flush();
            return messageLen;
        }catch (IOException e){
            dtrace.trace(2003);
            if (debuger.isDebugMode())
                e.printStackTrace();
            try{
                netSocket.close();
            }catch (IOException e1){
            }
            return -1;
        }
    }

    // get remote address
    public String getRemoteAddress(){
        return netSocket.getRemoteSocketAddress().toString();
    }
}
