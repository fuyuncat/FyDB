/**
 * @(#)VirtualFile.java	0.01 11/06/29
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_data;

import fydb.fy_comm.CommUtility;
import fydb.fy_comm.Consts;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.io.SyncFailedException;

import java.util.ArrayList;

public class VirtualFile {
    private class BufferNode{
        public byte[] buffer;
        
        public BufferNode(byte[] b){
            if (b!=null){
                buffer = new byte[b.length];
                CommUtility.arrayCopy(b,0,buffer,0,b.length);
            }
        }

        public BufferNode(byte[] b, int off, int len){
            if (b!=null && b.length-off>=len){
                buffer = new byte[len];
                CommUtility.arrayCopy(b,off,buffer,0,len);
            }
        }
    }

    private RandomAccessFile realFile = null;
    private ArrayList bufferChain;
    private int workMode;
    private int nodeSize;

    private int buffPointer = 0; 
    private int buffLength = 0;
    private boolean isClose = true;

    public VirtualFile() {
    }

    public VirtualFile(int nodeSize) {
        workMode = Consts.BUFFER;
        this.nodeSize = nodeSize;
        this.bufferChain = new ArrayList();
        this.isClose = false;
    }

    public VirtualFile(File file, String mode) throws FileNotFoundException {
        workMode = Consts.DISK;
        realFile = new RandomAccessFile(file,mode);
    }
    
    public long getFilePointer() throws IOException{
        switch(workMode){
            case Consts.DISK:
                return realFile.getFilePointer();
            case Consts.BUFFER:
                return buffPointer;
        }
        return 0;
    }

    public void seek(long pos) throws IOException{
        switch(workMode){
            case Consts.DISK:
                realFile.seek(pos);
                break;
            case Consts.BUFFER:
                buffPointer=(int)pos;
                break;
        }
    }

    public void write(byte[] b, int off, int len) throws IOException{
        switch(workMode){
            case Consts.DISK:{
                realFile.write(b,off,len);
            }
            break;
            case Consts.BUFFER:{
                BufferNode node = new BufferNode(b,off,len);
                // if larger than buffer size, append 
                if (buffPointer>=buffLength){
                    bufferChain.add(node);
                    buffLength=bufferChain.size();
                    buffPointer=buffLength;
                }else{
                    bufferChain.set(buffPointer,node);
                    buffPointer++;
                }
            }
            break;
        }
    }
    
    public int read(byte[] b, int off, int len) throws IOException{
        switch(workMode){
            case Consts.DISK:{
                return realFile.read(b,off,len);
            }
            case Consts.BUFFER:{
                if (buffPointer>=buffLength)
                    return -1;
                else{
                    BufferNode node = (BufferNode)bufferChain.get(buffPointer);
                    if (node == null || node.buffer == null)
                        return -1;
                    else{
                        if (len <= node.buffer.length){
                            CommUtility.arrayCopy(node.buffer,0,b,off,len);
                            return len;
                        }else{
                            CommUtility.arrayCopy(node.buffer,0,b,off,node.buffer.length);
                            return node.buffer.length;
                        }
                    }
                }
            }
            break;
        }
        return -1;
    }

    public void sync() throws IOException, SyncFailedException{
        if (workMode == Consts.DISK)
            realFile.getFD().sync();
    }

    public long length() throws IOException{
        switch(workMode){
            case Consts.DISK:
                return realFile.length();
            case Consts.BUFFER:
                return buffLength;
        }
        return 0;
    }

    public void close() throws IOException{
        switch(workMode){
            case Consts.DISK:
                realFile.close();
                break;
            case Consts.BUFFER:
                this.bufferChain = null;
                this.isClose = true;
                break;
        }
    }
}
