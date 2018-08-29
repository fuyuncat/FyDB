/*
 * @(#)PhyHashData.java	0.01 11/04/19
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_data;

import fydb.fy_comm.CommUtility;
import fydb.fy_comm.Consts;
import fydb.fy_comm.Debuger;
import fydb.fy_comm.FyDataEntry;
import fydb.fy_comm.Tracer;
import fydb.fy_comm.FyMetaData;

import fydb.fy_comm.InitParas;

//import fydb.fy_main.CommunicationClient;
//import fydb.fy_main.BP;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import java.io.OutputStreamWriter;

import java.sql.Connection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;

public class PhyHashKVData<K,V> extends HashMap<K,V> implements PhyBaseData{
    // block format of BMB
    // blockType(4b)token(4b)hashMod(4b)modNum(4b)parentAddr(8b)keepBytes(32b)hashMod1(4)referAddr1(8b)...hashModNum(4)referAddrNum(8b)...
    public class ManagerBlock{
        private int blockType;    // block type
        private long address;     // address of the block
        private int token;        // token number using for generating a hashNum
        private int hashMod;      // hashMod of current block
        private int modNum;       // modNum for separate children, only available in BMB block. 
                                 // value is power of 2.  2 <= modNum <= (blockSize-60)/12
                                 // new BMB block will start with 2.
        private ManagerBlock parentBlock;  // parent block
        private HashMap children; // hashMod:ManagerBlock; children==null means referring to a data block
        //public int vacantSize;   // vacant size of a datablok, just meaningful for data block
        //public int entryNum;     // entry number contained in a datablok, just meaningful for data block

        public void setBlockType(int blockType){
            this.blockType = blockType;
        }

        public int getBlockType(){
            return this.blockType;
        }

        public void setAddress(long address){
            this.address = address;
if (address == 1959936L){
    int bBreak = 1;
}
        }

        public long getAddress(){
            return this.address;
        }

        public void setToken(int token){
            this.token = token;
        }

        public int getToken(){
            return this.token;
        }

        public void setHashMod(int hashMod){
if (this.address == 1959936L){
    int bBreak = 1;
}
            this.hashMod = hashMod;
        }

        public int getHashMod(){
            return this.hashMod;
        }

        public void setModNum(int modNum){
            this.modNum = modNum;
        }

        public int getModNum(){
            return this.modNum;
        }

        public void setParentBlock(ManagerBlock parentBlock){
if (this.address == 1959936L){
    int bBreak = 1;
}
            this.parentBlock = parentBlock;
        }

        public ManagerBlock getParentBlock(){
            return this.parentBlock;
        }
        
        public void reNewChildren(){
            this.children = new HashMap();
        }
        
        public void putChild(Integer childHashMod, ManagerBlock childBlock){
if (this.address == 235520L){
    ManagerBlock test = (ManagerBlock)children.get(Integer.valueOf(34));
    if (test != null && test.address == 1959936L){
        int bBreak = 1;
    }
}
if (childBlock.getAddress() == 1959936L || (this.address == 235520L && childHashMod.intValue() == 34)){
    int bBreak = 1;
}
if (childBlock.getAddress() == 1959936L && childHashMod.intValue() == 34){
    int bBreak = 1;
}
            this.children.put(childHashMod,childBlock);
        }

        public ManagerBlock getChild(Integer childHashMod){
            return (ManagerBlock)this.children.get(childHashMod);
        }

        public HashMap getChildren(){
            return this.children;
        }

        public void removeChild(Integer childHashMod){
            this.children.remove(childHashMod);
        }

        public ManagerBlock(){
            setAddress(-1L);
            //this.hashMod = -1;
            setToken(1);
            setHashMod(-1);
            setParentBlock(null);
            reNewChildren();
            setModNum(2);
        }

        // constuctor 
        public ManagerBlock(int blockType, long address, int hashMod, int token, ManagerBlock parentBlock){
            setBlockType(blockType);
            setAddress(address);
            setHashMod(hashMod);
            setToken(token);
            setParentBlock(parentBlock);
            reNewChildren();
            setModNum(2);
        }

        // seach the data block for special hashCode
        public ManagerBlock getDataBlock(int baseNum){
            // we should never call a datablock's getDataBlock.
            if (blockType == Consts.DATA){
                return null;
            }else{
                //int hashMod = hashCode%(powerBase^token);
                int nodeHashMod = CommUtility.multiHashNum(baseNum, token)%modNum;
                ManagerBlock childManagerBlock = (ManagerBlock)children.get(Integer.valueOf(nodeHashMod));
                // not found allocated data block
                // since we should always assign child for hashMod less than BMP's modNum, we should never get null here
                // we do not pre-allocate data block. set address of empty block as -1L
                if (childManagerBlock == null){
                    //childManagerBlock = new ManagerBlock(Consts.DATA, -1L,hashMod,token+1,this);
                    //children.put(new Integer(hashMod), childManagerBlock);
                    //return childManagerBlock;
                    return null;
                }else if (childManagerBlock.blockType == Consts.DATA)
                    return childManagerBlock;
                else
                    return childManagerBlock.getDataBlock(baseNum);
            }
        }
        
        // update the updated block to its parent. recursive call
        public void updateMe(){
            if (parentBlock == null)
                return;
            else {
                // update parent
                parentBlock.putChild(Integer.valueOf(hashMod),this);
                parentBlock.updateMe();
            }
        }

        // count number of bmb&data block. will not count in the duplicated ones
        // if blockType != UNKNOWN, it will count all
        public int count(int blockType){
            int counter = ((this.blockType==blockType||blockType==Consts.UNKNOWN)&&this.address>=0)?1:0;
            HashSet countedChildren = new HashSet();
            Iterator it = children.values().iterator();
            while (it.hasNext()){
                ManagerBlock child = (ManagerBlock)it.next();
                if (!countedChildren.contains(child.address)){
                    counter+=child.count(blockType);
                    countedChildren.add(child.address);
                }
            }
            return counter;
        }

        public void dump(BufferedWriter dumpFile, String fileName, int deep){
            try{
                if (dumpFile == null){
                    //String tmpDir = System.getProperty("java.io.tmpdir");
                    String tmpDir = System.getProperty("user.dir")+File.separator+"trace";
                    File f = new File(tmpDir+File.separator+fileName+".dmp");
                    if (f.exists())
                        f.delete();
                    f.createNewFile();
                    dumpFile = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f)));
                }
                dumpFile.write("*"+address+": "+token+","+modNum);
                dumpFile.newLine();
                HashSet countedChildren = new HashSet();
                Iterator it = children.keySet().iterator();
                while (it.hasNext()){
                    Integer childHashMod = (Integer)it.next();
                    ManagerBlock child = (ManagerBlock)children.get(childHashMod);
                    if (!countedChildren.contains(child.address)){
                        dumpFile.write("+"+CommUtility.repeatStr("-",deep)+"("+childHashMod+")");
if (child.address == 1959936L){
    int bBreak = 1;
}
                        child.dump(dumpFile,fileName,deep+1);
                        countedChildren.add(child.address);
                    }
                }
                dumpFile.flush();
            }catch(Exception e){
                dtrace.trace(10);
                if (debuger.isDebugMode())
                    e.printStackTrace();
            }
        }
    }

    private Tracer dtrace; // = new Tracer();
    private Debuger debuger;// = new Debuger();
    private InitParas paras;// ;
    private FyMetaData metaData; 
    private MemBaseData memData;

    protected boolean phyinited;
    protected boolean loading;

    private BlockParas blockParas;
    private int sourceLocation;
    private CommunicationClient master;
    //private File[] dataFiles;
    private VirtualFile[] fileAccessers;
    private String fileName;  // base file name
    private String fileDir;   // data file directory
    //private String encoding;  // encoding of file
    //private int blockSize;    // block size
    private int fileModNum;       // number to mode file hashNum
    public int fileToken = 1;    // token number using for generating file hashNum. it's always 1!!!
                                    // it's also the base token of blocks.
    //private int powerBase;        // 
    private ManagerBlock[] rootManagers; // root of manager blocks
    private byte[][][] managerBlocks; // data block manager blocks. [modId][chunkId][contents]
                                      // contents: blockStarAddr(8b) blockFlag(1b)...
    private int writeBlockCount[];    // counter wroten blocks, for synchronize to disk

    private Iterator curEntryIt;      // current iterator point to loaded data entry

    private int curFileId;       // current file id during the full reading 
    private int curChunkId;      // current chunk id during the full reading 
    private int curBlockId;      // current block id during the full reading 
    private byte[] curReadBlock; // current read block during the full reading 
    private int curOffSet;       // current offset in the read block during the full reading 
    private int curEntryNum;     // entry number of current block
    private int curEntryCount;   // read entry count in current block

    private byte[][] lock;       // used for synchronize

    public PhyHashKVData(Tracer dtrace, Debuger debuger, InitParas paras) {
        this.dtrace = dtrace;
        this.debuger = debuger;
        this.paras = paras;
        this.phyinited = false;
        this.loading = false;
        this.blockParas = new BlockParas();
    }

    public FyMetaData init(){
        return null;
    }

    public FyMetaData init(Connection dbconn, String dbTable, ArrayList keyColumns, String where){dtrace.trace(5); return null;}

    public FyMetaData init(String fileDir, String fileName, int workMode){
        this.fileName = fileName;
        this.fileDir = fileDir;

        try{ // data file of a db source data: dataDir/<tablename>_<hashcode%mod>.log.
            boolean readMeta =false;
            if (metaData == null)
                metaData = new FyMetaData(dtrace, fileName);
            else
                readMeta = true;
            int fileMaxMod = 1;

            //this.encoding = encoding;
            File[] existingFiles = CommUtility.getFiles(fileDir, fileName, (String)paras.getParameter("_dataFileExtention"));
            for (int i=0;i<existingFiles.length;i++){
                int tmpMod = -1;
                try{
                    tmpMod =  Integer.parseInt(existingFiles[i].getName().substring(fileName.length(),
                                                          existingFiles[i].getName().length()-((String)paras.getParameter("_dataFileExtention")).length()-1));
                }catch(NumberFormatException e){
                    continue;
                }
                if (tmpMod+1>fileMaxMod)
                    fileMaxMod = tmpMod;
            }
            this.fileModNum = fileMaxMod;
            //this.fileMaxPower = (int)CommUtility.log((double)revisedMod, (double)powerBase);

            blockParas.encoding = (String)paras.getParameter("_defaultEncoding"); // get default encoding, will update in readMetaData
            //blockParas.blockSize = paras._blockSize; // get default size, will update in readMetaData
            blockParas.assignBlockSize((Integer)paras.getParameter("_blockSize"));
            //if (assignedModNum > 0)
            //    blockParas.assignModNum(assignedModNum);

            File[] datasFiles = new File[fileModNum];
            writeBlockCount = new int[fileModNum];
            fileAccessers = new VirtualFile[fileModNum];
            rootManagers = new ManagerBlock[fileModNum];
            lock = new byte[fileModNum][0];
            phyinited = true;
            // verificate existing files, prepare to assign valid file to accesser
            for (int i=0;i<existingFiles.length;i++){
                try{
                    int tmpMod =  Integer.parseInt(existingFiles[i].getName().substring(fileName.length(),
                                                          existingFiles[i].getName().length()-((String)paras.getParameter("_dataFileExtention")).length()-1));
                    datasFiles[tmpMod] = existingFiles[i];
                    fileAccessers[tmpMod] = new VirtualFile(existingFiles[i], "rwd");
                    if (!readMeta)
                        phyinited = readMetaData(tmpMod);
                    if (!readManagerBlocks(tmpMod))
                        phyinited = false;
                }catch(NumberFormatException e){

                }
            }

            // initialize file accessers
            for (int i=0;i<fileModNum;i++){
                if (datasFiles[i] == null || !datasFiles[i].exists()){
                    //datasFiles[i] = new File(fileDir+File.separator+fileName+i+"."+(String)paras.getParameter("_dataFileExtention"));
                    //datasFiles[i].createNewFile();
                    //fileAccessers[i] = new RandomAccessFile(datasFiles[i], "rwd");
                    newFileAccesser(i);
                }
                writeBlockCount[i]=0;
            }

            if (phyinited)
                loadAllData();
            return metaData;
        }catch (Exception e){
            dtrace.trace(fileDir+File.separator+fileName+"*.dat");
            dtrace.trace(10);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return null;
        }
    }
    
    public boolean buildNewFiles(){
        writeBlockCount = new int[fileModNum];
        fileAccessers = new VirtualFile[fileModNum];
        rootManagers = new ManagerBlock[fileModNum];
        lock = new byte[fileModNum][0];
        phyinited = true;
        for (int i=0;i<fileModNum;i++)
            if (!newFileAccesser(i))
                return false;
        return true;
    }

    // create a new data file 
    private boolean newFileAccesser(int fileId){
        try{
            String fName = fileDir+File.separator+fileName+fileId+"."+(String)paras.getParameter("_dataFileExtention");
            File datasFile = new File(fName);
            // bacukup existing files
            if (datasFile.exists()){
                CommUtility.copyFile(fName,fName+".bak",true);
                datasFile.delete();
            }
            datasFile.createNewFile();
            fileAccessers[fileId] = new VirtualFile(datasFile, "rwd");
            byte[][] metaBlocks = buildMetaBlocks();
            // write meta blocks
            for (int j=0;j<metaBlocks.length;j++){
                //fileAccessers[fileId].write(metaBlocks[j],0,blockParas.blockSize);
                writeBlockToFile(fileId,metaBlocks[j]);
            }

            // write 1st manager block buildBlockManagerBlock
            long bmbAddr = fileAccessers[fileId].getFilePointer();
            byte[] bmbBlock = buildBlockManagerBlock(fileId,fileToken,-1L);
            // long dataBlockAddr = bmbAddr+blockParas.blockSize; // since we still not write bmb to file, we should plus a blocksize
            // add 2 dummy data blocks info into root bmb. 
            CommUtility.arrayCopy(CommUtility.intToByteArray(0),0,bmbBlock,60,4);
            CommUtility.arrayCopy(CommUtility.longToByteArray(-1L),0,bmbBlock,64,8);
            CommUtility.arrayCopy(CommUtility.intToByteArray(1),0,bmbBlock,72,4);
            CommUtility.arrayCopy(CommUtility.longToByteArray(-1L),0,bmbBlock,76,8);
            //fileAccessers[fileId].write(buildBlockManagerBlock(fileId,fileToken,-1L),0,blockParas.blockSize);
            //fileAccessers[fileId].getFD().sync();
            writeBlockToFile(fileId,bmbBlock);
            // allocate 2 dummy data blocks for root bmb
            // we pre-allocate one empty data block
            //byte[] block = buildDataBlock(0,fileToken+2,bmbAddr);
            //long dataBlockAddr = writeBlockToFile(fileId,block);

            // create a new rootManager
            rootManagers[fileId] = new ManagerBlock(Consts.BMB,bmbAddr,fileId,fileToken+1,null);
            // update the get dataBlock to root BMB
            rootManagers[fileId].setBlockType(Consts.BMB);
            rootManagers[fileId].reNewChildren();;
            // we do not pre-allocate data block for empty block, just set address as -1L
            ManagerBlock dataBlock = new ManagerBlock(Consts.DATA,-1L,0,fileToken+2,rootManagers[fileId]);
            rootManagers[fileId].putChild(new Integer(0),dataBlock);
            rootManagers[fileId].putChild(new Integer(1),dataBlock);
            //dataBlock = new ManagerBlock(Consts.DATA,dataBlockAddr,1,fileToken+2,rootManagers[fileId]);
            return true;
        }catch(Exception e){
            dtrace.trace(10);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return false;
        }
    }

    // read remain bytes from supplement block
    // blockType(4b)parentAddr(8b)keepBytes(32b)entryNum(4b)dataEntryLen(4b)colId1(4b)colLen1(4b)col1...suppBlockAddr(8b)
    private byte[] readSuppBytes(int fileId, long suppBlockAddr){
        byte[] entryBytes = new byte[0];
        byte[] block = new byte[blockParas.blockSize];
        if (readBlockFromFile(fileId, block, suppBlockAddr, true) == blockParas.blockSize){
            int vacantSize = blockParas.blockSize - 52 - 4; // 4b for dataEntryLen
            int offSet = 48;
            int entryLen = CommUtility.readIntFromBlock(block,offSet);
            offSet+=4;
            // entryLen > vacantSize indicate that it has bytes storing in supplement block
            if (entryLen > vacantSize){
                // get current bytes first
                entryBytes = new byte[vacantSize];
                CommUtility.arrayCopy(block,offSet,entryBytes,0,entryBytes.length);
                suppBlockAddr = CommUtility.readLongFromBlock(block,blockParas.blockSize-8);
                byte[] suppBytes = readSuppBytes(fileId, suppBlockAddr);
                entryBytes = CommUtility.appendToArrayB(entryBytes,suppBytes);
            }else{
                entryBytes = new byte[entryLen];
                CommUtility.arrayCopy(block,offSet,entryBytes,0,entryBytes.length);
            }
        }
        return entryBytes;
    }

    // load data entries from a data block
    // blockType(4b)token(4b)hashMod(4b)parentAddr(8b)keepBytes(32b)entryNum(4b)dataEntryLen(4b)colId1(4b)colLen1(4b)col1...suppBlockAddr(8b)
    private MemBaseData readEntriesFromDataBlock(int fileId, byte[] block){
        MemBaseData entries = new MemHashKVData(dtrace);
        int blockType = CommUtility.readIntFromBlock(block,0);
        int vacantSize = blockParas.blockSize - blockParas.dataBlockHeaderSize - 4; // 4b for dataEntryLen
        if (blockType == Consts.DATA){
            int entryNum = CommUtility.readIntFromBlock(block,52);
//if (entryNum > 1){
//    int bBreak = 1;
//}
            int offSet = 56;
            for (int i=0;i<entryNum;i++){
                int entryLen = CommUtility.readIntFromBlock(block,offSet);
                offSet+=4;
                byte[] entryBytes = new byte[0];
                // entryLen > vacantSize indicate that it has bytes storing in supplement block
                if (entryLen > vacantSize){
                    // get current bytes first
                    entryBytes = new byte[vacantSize];
                    CommUtility.arrayCopy(block,offSet,entryBytes,0,entryBytes.length);
                    long suppBlockAddr = CommUtility.readLongFromBlock(block,blockParas.blockSize-8);
                    byte[] suppBytes = readSuppBytes(fileId, suppBlockAddr);
                    entryBytes = CommUtility.appendToArrayB(entryBytes,suppBytes);
                }else{
                    entryBytes = new byte[entryLen];
                    CommUtility.arrayCopy(block,offSet,entryBytes,0,entryBytes.length);
                }
                FyDataEntry data = new FyDataEntry();
                data.decodeData(entryBytes,0,entryBytes.length,metaData.getKeyColumns().size(),blockParas.encoding);
                offSet+=entryLen;
                entries.set(data.key,data.value);
            }
        }
        return entries;
    }

    // set all file readers point to the 1st data blcok, to load all data
    private void loadAllData(){
        if (!phyinited){
            dtrace.trace(124);
            return;
        }
        if (fileAccessers == null)
            return;
        loading = true;
        curReadBlock = null;
        curFileId = 0;
        curChunkId = 0;
        curBlockId = 0;
        curOffSet = 0;
        curEntryCount = 0;

        memData = new MemHashKVData(dtrace);
        // full read files.
        for (int i=0;i<fileModNum;i++){
            if (fileAccessers[i] == null)
                continue;
            int readByteNum = 0;
            while(readByteNum>=0){
                // read multiple blocks once.
                byte[] multiBlock = new byte[blockParas.blockSize*(Integer)paras.getParameter("_multiReadBlockNum")];
                readByteNum = readBlockFromFile(i,multiBlock);
                // load data entries from data blocks
                for (int j=0;j<readByteNum/blockParas.blockSize;j++){
                    byte[] block = new byte[blockParas.blockSize];
                    CommUtility.arrayCopy(multiBlock,j*blockParas.blockSize,block,0,blockParas.blockSize);
                    MemBaseData entries = readEntriesFromDataBlock(i, block);
                    memData.putAll((MemHashKVData)entries);
                }
            }
        }

        curEntryIt = memData.keySet().iterator();
    }

    // function to write block to file, control to synchronize with disk and others
    // if address<0, write to current pointer, otherwise, seek the address first
    // pointBack indicate that if it should be pointed back to previous address
    // return address that the block writen. -1 means failed
    private long writeBlockToFile(int fileId, byte[] block, int offSet, int len, long address, boolean pointBack){
        if (fileAccessers == null || fileId >= fileAccessers.length || fileAccessers[fileId] == null || block == null)
            return -1L;
        synchronized(lock[fileId]){
            try{
                long curAddress = fileAccessers[fileId].getFilePointer();
                if (address >= 0)
                    fileAccessers[fileId].seek(address);
                long writeAddress = fileAccessers[fileId].getFilePointer();
                fileAccessers[fileId].write(block,offSet,len);
                if (pointBack)
                    setFilePointer(fileId,curAddress);
                writeBlockCount[fileId]++;
                if (writeBlockCount[fileId]>=(Integer)paras.getParameter("_flushNumber")){
                    fileAccessers[fileId].sync();
                    writeBlockCount[fileId] = 0;
                }
                return writeAddress;
            }catch (IOException e){
                dtrace.trace(10);
                if (debuger.isDebugMode())
                    e.printStackTrace();
                return -1L;
            }
        }
    }

    // call writeBlockToFile
    private long writeBlockToFile(int fileId, byte[] block, long address, boolean pointBack){
        if (block == null)
            return -1L;
        return writeBlockToFile(fileId,block,0,block.length,address,pointBack);
    }

    // call writeBlockToFile
    private long writeBlockToFile(int fileId, byte[] block){
        if (block == null)
            return -1L;
        return writeBlockToFile(fileId,block,0,block.length,-1L,false);
    }

    // call writeBlockToFile
    private long appendBlockToFile(int fileId, byte[] block){
        if (block == null)
            return -1L;
        try{
            return writeBlockToFile(fileId,block,0,block.length,fileAccessers[fileId].length(),false);
        }catch (IOException e){
            dtrace.trace(10);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return -1L;
        }
    }

    private void synFiles(){
        for (int i=0;i<fileModNum;i++){
            synchronized(lock[i]){
                try{
                    fileAccessers[i].sync();
                    writeBlockCount[i] = 0;
                }catch(IOException e){
                    continue;
                }
            }
        }
    }

    // function to read block from file, control to synchronize with disk and others
    // if address<0, write to current pointer, otherwise, seek the address first
    // pointBack indicate that if it should be pointed back to previous address
    // @return number of read bytes. -1 mean reached the end of file; -2 means parameters incorrect; -3 is exception
    private int readBlockFromFile(int fileId, byte[] block, int offSet, int len, long address, boolean pointBack){
        if (fileAccessers == null || fileId >= fileAccessers.length || fileAccessers[fileId] == null || block == null)
            return -2;
        synchronized(lock[fileId]){
            try{
                long curAddress = fileAccessers[fileId].getFilePointer();
                if (address >= 0)
                    fileAccessers[fileId].seek(address);
//if (fileAccessers[fileId].getFilePointer() == 76800L){
//int bBreak = 1;
//Exception ex = new Exception();
//ex.printStackTrace();
//}
                int readNum = fileAccessers[fileId].read(block,offSet,len);
                if (pointBack)
                    setFilePointer(fileId,curAddress);
                return readNum;
            }catch (IOException e){
                dtrace.trace(10);
                if (debuger.isDebugMode())
                    e.printStackTrace();
                return -3;
            }
        }
    }

    // call readBlockFromFile
    private int readBlockFromFile(int fileId, byte[] block, long address, boolean pointBack){
        if (block == null)
            return -2;
        return readBlockFromFile(fileId,block,0,block.length,address,pointBack);
    }

    // call readBlockFromFile
    private int readBlockFromFile(int fileId, byte[] block){
        if (block == null)
            return -2;
        return readBlockFromFile(fileId,block,0,block.length,-1L,false);
    }

    // set file pointer address
    // in some senarios (e.g. append mode), we need set the file pointer address
    private boolean setFilePointer(int fileId, long address){
        if (fileAccessers == null || fileId >= fileAccessers.length || fileAccessers[fileId] == null || address < 0)
            return false;
        synchronized(lock[fileId]){
            try{
                fileAccessers[fileId].seek(address);
                return true;
            }catch (IOException e){
                dtrace.trace(10);
                if (debuger.isDebugMode())
                    e.printStackTrace();
                return false;
            }
        }
    }

    // load bmb data from file
    // blockType(4b)token(4b)hashMod(4b)modNum(4b)parentAddr(8b)keepBytes(32b)entryNum(4b)hashMod1(4)referAddr1(8b)...hashModNum(4)referAddrNum(8b)...
    private void loadBMBData(int fileId, ManagerBlock bmbBlock){
        if (bmbBlock.getAddress() < 0)
            return;
if (bmbBlock.getAddress()== 434176L){
    int bBreak = 1;
}
        byte[] block = new byte[blockParas.blockSize];
        readBlockFromFile(fileId,block,bmbBlock.getAddress(),true);
        int blockType = CommUtility.readIntFromBlock(block, 0);
        if (blockType != Consts.BMB)
            return;

        // coz we initial child block as DATA block, we should update its blockType when confirmed.
        bmbBlock.setBlockType(Consts.BMB);
        int modNum = CommUtility.readIntFromBlock(block, 12);
        bmbBlock.setModNum(modNum);
        // get children number
        int childNum = CommUtility.readIntFromBlock(block, 56);
        int offSet = 60;
        // loaded children, to avoid duplicated allocate ManagerBlock
        ArrayList existingChildren = new ArrayList();
        for (int i=0;i<childNum;i++){
            // read child hashMod
            int refHashMod = CommUtility.readIntFromBlock(block, offSet);
            offSet+=4;
            // read child address
            long refAddr = CommUtility.readLongFromBlock(block, offSet);
            offSet+=8;
            // we initialize child blcok as a DATA block, and will update the blockType when loading its data
            ManagerBlock childMBlock = new ManagerBlock(Consts.DATA,refAddr,refHashMod,bmbBlock.getToken()+1,bmbBlock);
            int existingId = -1;
            // check if the referred address exist or not
            for (int j=0;j<existingChildren.size();j++)
                if (((ManagerBlock)existingChildren.get(j)).getAddress() == refAddr){
                    existingId = j;
                    break;
                }
            // block exists, set the new child as dummy, referring to the exiting block
            if (existingId >= 0){
                bmbBlock.putChild(new Integer(refHashMod),(ManagerBlock)existingChildren.get(existingId));
            }else{
                // add as real child to children hashMap
                bmbBlock.putChild(new Integer(refHashMod),childMBlock);
                existingChildren.add(childMBlock);
                // load child BMB data
                loadBMBData(fileId, childMBlock);
            }
        }
    }

    // read root manager block of each data file
    // blockType(4b)token(4b)hashMod(4b)modNum(4b)parentAddr(8b)keepBytes(32b)entryNum(4b)hashMod1(4)referAddr1(8b)...hashModNum(4)referAddrNum(8b)...
    private boolean readManagerBlocks(int fileId){
        if (fileAccessers == null || fileId >=fileAccessers.length || fileAccessers[fileId] == null)
            return false;
        managerBlocks = new byte[fileModNum][0][0];
        byte[] block = new byte[blockParas.blockSize];
        long nextAddr = 0;
        // detect if file is readable
        if (readBlockFromFile(fileId,block)==block.length){
            // get the 1st meta block
            readBlockFromFile(fileId,block,nextAddr,false);
            //fileAccessers[i].seek(nextAddr);
            //fileAccessers[i].read(block,0,blockParas.blockSize);
            long bmbAddr = 0;
            // find the last meta block, and get dataManagerBlcokStartAddr
            while(CommUtility.readIntFromBlock(block,0) == Consts.META){
                nextAddr = CommUtility.readLongFromBlock(block,12);
                if (nextAddr > 0){
                    readBlockFromFile(fileId,block,nextAddr,false);
                    //fileAccessers[i].seek(nextAddr);
                    //fileAccessers[i].read(block,0,blockParas.blockSize);
                }else{ // reached the last meta block
                    //read dataManagerBlcokStartAddr(root bmb addr)
                    bmbAddr = CommUtility.readLongFromBlock(block,blockParas.blockSize-8);
                    rootManagers[fileId] = new ManagerBlock(Consts.BMB,bmbAddr,fileId,fileToken+1,null);
                    break;
                }
            }
            loadBMBData(fileId, rootManagers[fileId]);
        }

        return true;
    }

    // read manager blocks of each chunk in every data file
    // blockStarAddr(8b)keepBytes(32b)blockFlag(1b)...nextManagerBlockAddr(8b)
    // blockFlag indicate the content full percent.
    // contents stroed in managerBlocks: blockStarAddr(8b) blockFlag(1b)...
    private boolean readManagerBlocks2(){
        if (fileAccessers == null)
            return false;
        managerBlocks = new byte[fileModNum][0][0];
        for (int i=0;i<fileAccessers.length;i++){ // test if file contains data
            synchronized(lock[i]){
                byte[] block = new byte[blockParas.blockSize];
                long nextAddr = 0;
                if (fileAccessers[i]!=null && readBlockFromFile(i,block)==block.length){
                    readBlockFromFile(i,block,nextAddr,false);
                    //fileAccessers[i].seek(nextAddr);
                    //fileAccessers[i].read(block,0,blockParas.blockSize);
                    long bmbAddr = 0;
                    // find the last meta block, and get dataManagerBlcokStartAddr
                    while(CommUtility.readIntFromBlock(block,0) == Consts.META){
                        nextAddr = CommUtility.readLongFromBlock(block,12);
                        if (nextAddr > 0){
                            readBlockFromFile(i,block,nextAddr,false);
                            //fileAccessers[i].seek(nextAddr);
                            //fileAccessers[i].read(block,0,blockParas.blockSize);
                        }else{ // reached the last meta block
                            //read dataManagerBlcokStartAddr
                            bmbAddr = CommUtility.readLongFromBlock(block,blockParas.blockSize-8);
                            break;
                        }
                    }
                    // read block flags from manager block
                    while (bmbAddr > 0){
                        block = new byte[blockParas.blockSize];
                        byte[] managerBlock = new byte[blockParas.blockSize - 40];
                        //fileAccessers[i].seek(bmbAddr);
                        //fileAccessers[i].read(block,0,block.length);
                        readBlockFromFile(i,block,bmbAddr,false);
                        // copy dataBlockStartAddr
                        CommUtility.arrayCopy(block,0,managerBlock,0,8);
                        // copy block flags
                        CommUtility.arrayCopy(block,40,managerBlock,8,managerBlock.length-8);
                        byte[][] oldManagerBlocks = managerBlocks[i];
                        managerBlocks[i] = new byte[oldManagerBlocks.length+1][managerBlock.length];
                        CommUtility.mergeArrays(managerBlocks[i], oldManagerBlocks, new byte[][]{managerBlock});
                        // manager block of next chunk
                        bmbAddr = CommUtility.readLongFromBlock(block,blockParas.blockSize-8);
                    }
                }
            }
        }
        return true;
    }

    // catch meta data from block
    // |metaDataLen(4b)|metaDataBytes...|
    private byte[] catchMetaBytes(byte[] block){
        int metaLen = CommUtility.readIntFromBlock(block, 256);
        byte[] bytes = new byte[metaLen];
        CommUtility.arrayCopy(block,260,bytes,0,metaLen);
        return bytes;
    }

    // read Meta data from a data file. a Meta data blcok format:
    //  blockType(4b)PrevBlockAddr(8b)NextBlockAddr(8b)blocksize(4b)modNum(4b)EncodingLen(4b)Encoding keepBytes(256-28-encodingLen b)....
    //  |metaDataLen(4b)|metaDataBytes...|
    //  .....dataManagerBlcokStartAddr(8b)
    //
    // PrevBlockAddr is always 0xFFFFFFFF for the 1st block
    // NextBlockAddr is always 0xFFFFFFFF for the last block
    // Encoding string is stored encoded in ISO-8859-1.
    // dataManagerBlcokStartAddr is available for the last meta blcok, it's 0xFFFFFFFF for other meta blocks
    // maximum length of meta data bytes in a block is blockSize(default 8k)-256-4-8 = 7924 default
    private boolean readMetaData(int fileId){
        if (fileAccessers == null || fileId >=fileAccessers.length || fileAccessers[fileId] == null)
            return false;
        byte[] metaBytes = new byte[0];
        byte[] block = new byte[(Integer)paras.getParameter("_minBlockSize")];
        if (readBlockFromFile(fileId,block)==block.length){
            //blockSize = paras._minBlockSize;
            long nextAddr = 0;
            readBlockFromFile(fileId,block,nextAddr,false);
            //metaReader.seek(nextAddr);
            //metaReader.read(block,0,paras._minBlockSize);
            //blockParas.blockSize = CommUtility.readIntFromBlock(block,20);
            blockParas.assignBlockSize(CommUtility.readIntFromBlock(block,20)); // read block size
            //blockParas.assignModNum(CommUtility.readIntFromBlock(block,24));    // read modNum
            // re-read block after get the size
            block = new byte[blockParas.blockSize];
            readBlockFromFile(fileId,block,nextAddr,false);
            //metaReader.seek(nextAddr);
            //metaReader.read(block,0,blockParas.blockSize);
            //read encoding
            int encodingStrLen = CommUtility.readIntFromBlock(block,28);
            blockParas.encoding = CommUtility.readStringFromBlock(block,32,encodingStrLen,"ISO-8859-1");
            while(CommUtility.readIntFromBlock(block,0) == Consts.META){
                metaBytes = CommUtility.appendToArrayB(metaBytes, catchMetaBytes(block));

                nextAddr = CommUtility.readLongFromBlock(block,12);
                if (nextAddr > 0){
                    readBlockFromFile(fileId,block,nextAddr,false);
                    //metaReader.seek(nextAddr);
                    //metaReader.read(block,0,blockParas.blockSize);
                }else{ // reached the last meta block
                    break;
                }
            }
            metaData.decodeMeta(metaBytes,0,blockParas.encoding);
        }
        return true;
    }

    // build meta block header. Since we involve dynamic modNum, it's meaningless in meta data!!!
    // blockType(4b)PrevBlockAddr(8b)NextBlockAddr(8b)blocksize(4b)modNum(4b)EncodingLen(4b)Encoding keepBytes(256-28-encodingLen b)....
    private byte[] buildMetaHeader(int blockId, boolean toBeContinue){
        if (!phyinited){
            dtrace.trace(124);
            return null;
        }
        if (loading){
            dtrace.trace(125);
            return null;
        }
        byte[] header = new byte[0];
        header = CommUtility.appendToArrayB(header, CommUtility.intToByteArray(Consts.META)); // blockType
        header = CommUtility.appendToArrayB(header, CommUtility.longToByteArray(blockId==0?-1L:(blockId-1)*blockParas.blockSize)); // PrevBlockAddr
        header = CommUtility.appendToArrayB(header, CommUtility.longToByteArray(toBeContinue?(blockId+1)*blockParas.blockSize:-1L)); // NextBlockAddr
        header = CommUtility.appendToArrayB(header, CommUtility.intToByteArray(blockParas.blockSize)); // blocksize
        // Since we involve dynamic modNum, it's meaningless in meta data!!!
        header = CommUtility.appendToArrayB(header, CommUtility.intToByteArray(blockParas.getModNum())); // modNum
        header = CommUtility.appendToArrayB(header, CommUtility.intToByteArray(blockParas.encoding.getBytes().length)); // EncodingLen
        header = CommUtility.appendToArrayB(header, blockParas.encoding.getBytes()); // Encoding
        byte[] keepByte = new byte[256-header.length];
        header = CommUtility.appendToArrayB(header, keepByte);
        return header;
    }

    // build meta blocks
    //  blockType(4b)PrevBlockAddr(8b)NextBlockAddr(8b)blocksize(4b)modNum(4b)EncodingLen(4b)Encoding keepBytes(256-28-encodingLen b)....
    //  |metaDataLen(4b)|metaDataBytes...|
    //  .....dataManagerBlcokStartAddr(8b)
    //
    // PrevBlockAddr is always 0xFFFFFFFF for the 1st block
    // NextBlockAddr is always 0xFFFFFFFF for the last block
    // Encoding string is stored encoded in ISO-8859-1.
    // dataManagerBlcokStartAddr is available for the last meta blcok, it's 0xFFFFFFFF for other meta blocks
    // maximum length of meta data bytes in a block is blockSize(default 8k)-256-4-8 = 7924 default
    private byte[][] buildMetaBlocks(){
        if (!phyinited){
            dtrace.trace(124);
            return null;
        }
        if (loading){
            dtrace.trace(125);
            return null;
        }
        byte[][] blocks = new byte[0][blockParas.blockSize];
        int metaUnitSize = blockParas.blockSize - 256 - 4 -8-4;
        byte[] metaBytes = metaData.encodeMeta();
        // calculate fill size, should less than block size, and decide meta block number
        int metaBlockNum = metaBytes.length/metaUnitSize + (metaBytes.length%metaUnitSize>0?1:0);
        for (int i=0;i<metaBlockNum;i++){
            byte[] block = new byte[blockParas.blockSize];
            byte[] header = buildMetaHeader(i,i<metaBlockNum-1);
            CommUtility.arrayCopy(header,0,block,0,header.length);
            int fillSize = metaUnitSize;
            if (i==metaBlockNum-1){
                fillSize=metaBytes.length%metaUnitSize;
                fillSize=fillSize==0?metaUnitSize:fillSize;
            }
            CommUtility.arrayCopy(CommUtility.intToByteArray(fillSize),0,block,header.length,4);
            CommUtility.arrayCopy(metaBytes,i*metaUnitSize,block,header.length+4,fillSize);
            long bmbAddr = i<metaBlockNum-1?-1L:metaBlockNum*blockParas.blockSize; // dataManagerBlcokStartAddr
            CommUtility.arrayCopy(CommUtility.longToByteArray(bmbAddr),0,block,blockParas.blockSize-8,8);
            byte[][] oldBlocks = blocks;
            blocks = new byte[oldBlocks.length+1][blockParas.blockSize];
            CommUtility.mergeArrays(blocks, oldBlocks, new byte[][]{block});
        }
        return blocks;
    }

    // build block manager block
    // blockStarAddr(8b)keepBytes(32b)blockFlag(1b)...nextManagerBlockAddr(8b)
    // blockFlag indicate the content full percent.
    private byte[] buildBlockManagerBlock2(long blockStartAddr){
        if (!phyinited){
            dtrace.trace(124);
            return null;
        }
        if (loading){
            dtrace.trace(125);
            return null;
        }
        byte[] block = new byte[blockParas.blockSize];
        // set blockStarAddr
        CommUtility.arrayCopy(CommUtility.longToByteArray(blockStartAddr),0,block,0,8);
        byte[] keepBytes = new byte[32];
        CommUtility.arrayCopy(keepBytes,0,block,8,32);
        byte[] zeros = new byte[blockParas.blockSize-48];
        // fill with zeros
        CommUtility.arrayCopy(zeros,0,block,40,zeros.length);
        // set nextManagerBlockAddr
        CommUtility.arrayCopy(CommUtility.longToByteArray(-1L),0,block,blockParas.blockSize-8,8);
        return block;
    }

    public void fullCopyToFiles(MemBaseData memData){
        if (!phyinited){
            dtrace.trace(124);
            return;
        }
        if (loading){
            dtrace.trace(125);
            return;
        }
        Iterator it = memData.keySet().iterator();
        while(it.hasNext()){
            HashMap key = (HashMap)it.next();
            HashMap value = (HashMap)memData.get(key);
            fillDataEntry(new FyDataEntry(key,value));
        }
    }
    
/*
    // format of datablock
    // keepBytes(64b)blockType(4b)entryNum(4b)dataEntryLen(4b)colId1(4b)colLen1(4b)col1...
    // dataEntryLen = vacanctSize+(entryBytes.length-entryOffset)/vacanctSize
    // dataEntryLen - vacanctSize = remains blocks number for this dataEntry
    public void fullCopyToFiles2(MemBaseData memData){
        if (!phyinited){
            dtrace.trace(124);
            return;
        }
        if (loading){
            dtrace.trace(125);
            return;
        }
        byte[][] blocks = new byte[fileModNum][blockParas.blockSize];
        int[] entryNums = new int[fileModNum];
        int[] offSet = new int[fileModNum];
        int[] blockNum = new int[fileModNum];
        int[] flushCounter = new int[fileModNum];
        long[] managerBlockAddr = new long[fileModNum];
        // generate meata blcoks and first manager block
        for (int i=0;i<fileModNum;i++){
            try{
                byte[][] metaBlocks = buildMetaBlocks();
                // write meta blocks
                for (int j=0;j<metaBlocks.length;j++){
                    fileAccessers[i].write(metaBlocks[j],0,blockParas.blockSize);
                }
                // write 1st manager block buildBlockManagerBlock
                managerBlockAddr[i] = fileAccessers[i].getFilePointer();
                fileAccessers[i].write(buildBlockManagerBlock2((long)(metaBlocks.length+1)*blockParas.blockSize),0,blockParas.blockSize);
                fileAccessers[i].getFD().sync();
            }catch(Exception e){
                dtrace.trace(10);
                if (debuger.isDebugMode())
                    e.printStackTrace();
                continue;
            }
            blockNum[i] = 0;
            flushCounter[i] = 0;
            // fill header of the 1st data blocks
            CommUtility.arrayCopy(new byte[64],0,blocks[i],0,64);
            CommUtility.arrayCopy(CommUtility.intToByteArray(Consts.DATA),0,blocks[i],64,4);
            // initial entryNums  offSet
            entryNums[i] = 0;
            offSet[i] = 72;
        }

        Iterator it = memData.keySet().iterator();
        int thresHold = blockParas.blockSize - 64 - 4 - 4;
        long totalSize = 0;
        int blockCount = 0;
        while(it.hasNext()){
            HashMap key = (HashMap)it.next();
            HashMap value = (HashMap)memData.get(key);
            // get the entryBytes, entryBytes length may larger the size of a block can contain data
            byte[] entryBytes = (new FyDataEntry(null,key,value)).encodeData(metaData.getColumns().size());
            totalSize += entryBytes.length;
            int modId = key.hashCode()%fileModNum;
            try{
                int vacanctSize = blockParas.blockSize - offSet[modId] - 4; // 4 byte for entryLen
                // since entryBytes length may larger the size of a block can contain data, 
                // we should set offset for fill into block;
                int entryOffset = 0;
                // if dataEntry size larger than vacanctSize, write the block to file and allocate a new block
                // split long date entry (length > thresHold - headersize(72)) into multiple data Blocks
                while (entryBytes.length - entryOffset > vacanctSize){
                    // wrtie current block into file. only if entryNum>0 should write to file
                    if (entryNums[modId] > 0){
                        // keepBytes(64b)blockType(4b)entryNum(4b)dataEntryLen(4b)colId1(4b)colLen1(4b)col1...
                        // fill header of the 1st data blocks
                        // CommUtility.arrayCopy(new byte[64],0,blocks[modId],0,64);
                        // set blockType
                        CommUtility.arrayCopy(CommUtility.intToByteArray(Consts.DATA),0,blocks[modId],64,4);
                        // set entryNum
                        CommUtility.arrayCopy(CommUtility.intToByteArray(entryNums[modId]),0,blocks[modId],68,4);
                        // update block flag in manager block, offSet[modId] is the actual length of bytes stored in the block
                        byte blockFlag = CommUtility.intToByteArray(offSet[modId]*16/blockParas.blockSize)[3];
                        long curPointer = fileAccessers[modId].getFilePointer();
                        // check if manager block full. If full, current block should be writen as a new manager block.
                        if (blockNum[modId] >= blockParas.blockSize - 48){
                            // generate a new manager block. blockStartAddr is the next block, curPointer now is the address of manager block
                            byte[] managerBlock = buildBlockManagerBlock2(curPointer+(long)blockParas.blockSize);
                            managerBlock[40] = blockFlag;
                            long preManagerBlockAddr = managerBlockAddr[modId];
                            managerBlockAddr[modId] = curPointer;
                            // write a new manager block to file
                            fileAccessers[modId].write(managerBlock,0,blockParas.blockSize);
                            curPointer = fileAccessers[modId].getFilePointer();
                            // update write NextManagerBlockAddr into the previous manager block 
                            // point to previous manager block
                            fileAccessers[modId].seek(preManagerBlockAddr+(blockParas.blockSize-8));
                            // update previous manager block
                            fileAccessers[modId].write(CommUtility.longToByteArray(managerBlockAddr[modId]),0,8);
                            // point to curret data blcok
                            fileAccessers[modId].seek(curPointer);
                            blockNum[modId]=0;
                        }else{
                            // seek the block flag addr
                            fileAccessers[modId].seek(managerBlockAddr[modId]+40+blockNum[modId]);
                            fileAccessers[modId].write(blockFlag);
                            fileAccessers[modId].seek(curPointer);
                            blockNum[modId]++;
                        }
                        // write data block
                        fileAccessers[modId].write(blocks[modId],0,blockParas.blockSize);
                        flushCounter[modId]++;
                        if (flushCounter[modId] >= paras._flushNumber){
                            fileAccessers[modId].getFD().sync();
                            flushCounter[modId] = 0;
                        }

                        // allocate new block
                        blocks[modId] = new byte[blockParas.blockSize];
                        // initial entryNums  offSet
                        entryNums[modId] = 0;
                        offSet[modId] = 72;
                        blockCount++;
                        vacanctSize = blockParas.blockSize - offSet[modId] - 4;
                    }

                    // detect if it's a long entry (length > thresHold - headersize(72), which equal to vacanctSize now) 
                    if (entryBytes.length-entryOffset > vacanctSize){
                        // retrieve fittable bytes and reset offset
                        // dataEntryLen. 
                        // maximum dataEntryLen is vacanctSize, if it larger then it, means the data entry is splited into multiple data blocks
                        // here we set dataEntryLen = vacanctSize+(entryBytes.length-entryOffset)/vacanctSize
                        // dataEntryLen - vacanctSize = remains blocks number for this dataEntry
                        CommUtility.arrayCopy(CommUtility.intToByteArray(vacanctSize+(entryBytes.length-entryOffset)/vacanctSize),0,blocks[modId],offSet[modId],4);
                        offSet[modId]+=4;
                        // data entry
                        CommUtility.arrayCopy(entryBytes,entryOffset,blocks[modId],offSet[modId],vacanctSize);
                        offSet[modId]+=vacanctSize; 
                        entryNums[modId]++;
                        entryOffset += vacanctSize;
                        //vacanctSize = thresHold - offSet[modId];
                        // we filt the block with the part of the long entry, vacanctSize shoule be equal to zero
                        vacanctSize = 0; 
                    }
                }

                // dataEntryLen
                CommUtility.arrayCopy(CommUtility.intToByteArray(entryBytes.length-entryOffset),0,blocks[modId],offSet[modId],4);
                offSet[modId]+=4;
                // data entry
                CommUtility.arrayCopy(entryBytes,entryOffset,blocks[modId],offSet[modId],entryBytes.length-entryOffset);
                offSet[modId]+=(entryBytes.length-entryOffset);
                entryNums[modId]++;
            }catch(Exception e){
                dtrace.trace(10);
                if (debuger.isDebugMode())
                    e.printStackTrace();
                continue;
            }
        }
        // write the last block to file
        // flush remain blocks to file
        for (int i=0;i<fileModNum;i++)
            try{
                if (entryNums[i]> 0)
                    fileAccessers[i].write(blocks[i],0,blockParas.blockSize);
                fileAccessers[i].getFD().sync();
            }catch(Exception e){
                dtrace.trace(10);
                if (debuger.isDebugMode())
                    e.printStackTrace();
                continue;
            }
    }
//*/

    // build block manager block
    // blockType(4b)token(4b)hashMod(4b)modNum(4b)parentAddr(8b)keepBytes(32b)entryNum(4b)hashMod1(4)referAddr1(8b)...hashModNum(4)referAddrNum(8b)...
    // e.g. BMB 0 0 keepBytes 165 1 0x00000A00 2 0x0000E000 3 0x00012000 4 0x00016000 5 0x0002A000 ...
    //
    // powerBase = ceil((blockSize - 4 - 4 - 4 - 8 - 32 -4 )/(4 + 8)). If blockSize = 2048, powerBase <= 165
    //
    // BMB block will never be splitted, coz we always split data block and upgrade it as new bmb block! 
    // ignore below comments!!!
    // if ModNum > powerBase, it requires split this manager block into 2 blocks:
    // 1st: hashModn % 2^(level+1)
    // 2nd: 2^(level+1) + (2^n)
    // n:1~...
    // e.g. 
    // BMB 0 0 2 keepBytes  0 BMB0 1 BMB1
    // BMB0 1 0 82 keepBytes 2(2%(2^1)=0) 0x0000E000 4(2%(2^1)=0) 0x00016000 6(2%(2^1)=0) 0x0002E000 ...
    // BMB1 1 1 83 keepBytes 1(1%(2^1)=1) 0x00000A00 3(3%(2^1)=1) 0x00012000 5(5%(2^1)=1) 0x0002A000 ...
    private byte[] buildBlockManagerBlock(int hashMod, int token, long parentAddr){
        if (!phyinited){
            dtrace.trace(124);
            return null;
        }
        if (loading){
            dtrace.trace(125);
            return null;
        }
        byte[] block = new byte[blockParas.blockSize];
        // set blockType
        CommUtility.arrayCopy(CommUtility.intToByteArray(Consts.BMB),0,block,0,4);
        //byte[] keepBytes = new byte[32];
        //CommUtility.arrayCopy(keepBytes,0,block,8,32);
        //byte[] zeros = new byte[blockSize-48];
        // fill with zeros
        //CommUtility.arrayCopy(zeros,0,block,40,zeros.length);
        // set token
        CommUtility.arrayCopy(CommUtility.intToByteArray(token),0,block,4,4);
        // set hashMod
        CommUtility.arrayCopy(CommUtility.intToByteArray(hashMod),0,block,8,4);
        // set modNum, start with 2
        CommUtility.arrayCopy(CommUtility.intToByteArray(2),0,block,12,4);
        // set entryNum, start with 2
        CommUtility.arrayCopy(CommUtility.intToByteArray(2),0,block,56,4);
        // set parentAddr
        CommUtility.arrayCopy(CommUtility.longToByteArray(parentAddr),0,block,16,8);
        return block;
    }

    // format of datablock
    // blockType(4b)token(4b)hashMod(4b)parentAddr(8b)keepBytes(32b)entryNum(4b)dataEntryLen(4b)colId1(4b)colLen1(4b)col1...suppBlockAddr(8b)
    // suppBlockAddr is only available for those entrySize larger than vacant size of a block. otherwise, is -1L
    // e.g. DATA 2 BMB keepBytes 12 2(key) AVA CBC ... 4(key) ADA aaw ... 6(key) ase ..0xFFFFFFFF
    // 
    // if no vacant space for new entry, it requires splitting the dataBlock
    // e.g. 
    // BMB 0 1 keepBytes 0(2%(2^1)=0) BMB0
    // BMB0 0 1 keepBytes 2(2%(2^1)=0) DATA1
    // BMB1 1 1 keepBytes 1(1%(2^1)=1) DATA2
    // DATA1 2 1 BMB keepBytes 12 2(key) AVA CBC ... 4(key) ADA aaw ... 6(key) ase ..
    // DATA2 2 2 BMB keepBytes 12 2(key) AVA CBC ... 4(key) ADA aaw ... 6(key) ase ..
    // dataEntryLen = vacanctSize+(entryBytes.length-entryOffset)/vacanctSize
    // dataEntryLen - vacanctSize = remains blocks number for this dataEntry
    private byte[] buildDataBlock(int hashMod, int token, long parentAddr){
        if (!phyinited){
            dtrace.trace(124);
            return null;
        }
        if (loading){
            dtrace.trace(125);
            return null;
        }
        byte[] block = new byte[blockParas.blockSize];
        // set blockType
        CommUtility.arrayCopy(CommUtility.intToByteArray(Consts.DATA),0,block,0,4);
        // set token
        CommUtility.arrayCopy(CommUtility.intToByteArray(token),0,block,4,4);
        // set hashMod
        CommUtility.arrayCopy(CommUtility.intToByteArray(hashMod),0,block,8,4);
        // set parentAddr
        CommUtility.arrayCopy(CommUtility.longToByteArray(parentAddr),0,block,12,8);
        // set suppBlockAddr
        CommUtility.arrayCopy(CommUtility.longToByteArray(-1L),0,block,blockParas.blockSize-8,8);
        return block;
    }

    // format of supplement datablock, which contains the remain bytes of long entries.
    // the supplement blocks can just be refered from data blocks, not managed by BMB directly;
    // blockType(4b)parentAddr(8b)keepBytes(32b)entryNum(4b)dataEntryLen(4b)colId1(4b)colLen1(4b)col1...suppBlockAddr(8b)
    // suppBlockAddr is only available for those entrySize larger than vacant size of a block. otherwise, is -1L
    private byte[] buildSuppDataBlock(long parentAddr){
        if (!phyinited){
            dtrace.trace(124);
            return null;
        }
        if (loading){
            dtrace.trace(125);
            return null;
        }
        byte[] block = new byte[blockParas.blockSize];
        // set blockType
        CommUtility.arrayCopy(CommUtility.intToByteArray(Consts.SUPDATA),0,block,0,4);
        // set parentAddr
        CommUtility.arrayCopy(CommUtility.longToByteArray(parentAddr),0,block,4,8);
        // set suppBlockAddr
        CommUtility.arrayCopy(CommUtility.longToByteArray(-1L),0,block,blockParas.blockSize-8,8);
        return block;
    }

    // get vacant space offset from a data block
    // blockType(4b)token(4b)hashMod(4b)parentAddr(8b)keepBytes(32b)entryNum(4b)dataEntryLen(4b)colId1(4b)colLen1(4b)col1...suppBlockAddr(8b)
    private int getDataBlockVacantOffset(byte[] block){
        int entryNum = CommUtility.readIntFromBlock(block, 52);
        int offSet = 56; // offset of first entry
        try{
            while (entryNum > 0){
                int entryLen = CommUtility.readIntFromBlock(block, offSet);
                offSet+=(4+entryLen); //4b for dataEntryLen
                entryNum--;
            }
        }catch(Exception e){
            dtrace.trace(10);
            if (debuger.isDebugMode())
                e.printStackTrace();
        }
        return offSet;
    }

    // update a BMB block, add a new block referring address in it
    // blockType(4b)token(4b)hashMod(4b)modNum(4b)parentAddr(8b)keepBytes(32b)entryNum(4b)hashMod1(4)referAddr1(8b)...hashModNum(4)referAddrNum(8b)...
    private void putReferAddrToBMB(int fileId, ManagerBlock bmbBlock, int bmbMod, int refHashMod, long address){
        if (bmbBlock == null){
            dtrace.trace(127);
            return;
        }

        byte[] block = new byte[blockParas.blockSize];
        // get the bmb block, also check if we should allocate new bmb block
        if (bmbBlock.getAddress() < 0)
            block = buildBlockManagerBlock(bmbMod,bmbBlock.getToken(),bmbBlock.getParentBlock()==null?-1L:bmbBlock.getParentBlock().getAddress());
        else
            readBlockFromFile(fileId,block,bmbBlock.getAddress(),true);

        // update the bmb block, add new refering address and entryNum+1
        int entryNum = CommUtility.readIntFromBlock(block, 56);
        int offSet = 60 + 12*entryNum;
if (entryNum >= 37){
    int bBreak = 1;
}
        CommUtility.arrayCopy(CommUtility.intToByteArray(refHashMod),0,block,offSet,4);
        CommUtility.arrayCopy(CommUtility.longToByteArray(address),0,block,offSet+4,8);
        entryNum++;
        CommUtility.arrayCopy(CommUtility.intToByteArray(entryNum),0,block,56,4);
        writeBlockToFile(fileId,block,bmbBlock.getAddress(),true);
    }

    // recursive call, to get (existing block or new allocated block) to contain a new entry
    // 
    // first check if we got a unallocated block from bmb according to mod of hashCode. 
    //  If unallocate, we should allocate a new block and put the entry into it.
    //  if got an allocated block, we should check if it can contain the new entry
    //   if can contain it, append it directly (we should move all entries forward when deleting, so we just append it at here)
    //   if can't contain it, split current block into 2 blocks, and recursive call the process to insert it into the corresponding child block (map with mod of hashcode)
    //    before spliting a block, we should first check if its parent bmb has 2 or more entrie referring to this block.
    //     if its parent bmb has 2 or more entrie referring to this block, we should have those entires refer to the new blocks
    //     if its parent bmb just has 1 entry referring to this block. we should update its parent bmb's modNum to modNum*2
    //      before updating its parent bmb's modNum, we should check if modNum reached the limitaion of modNum (vacant size of a bmb block/12) = (blocksize - 60)/12
    //       if parent bmb's modNum reached the limitaion, we should upgrade current block as a BMB with new new hash arithmetic. and then generate 2 new data blocks, put entry into the new block
    private boolean putEntryToBlock(int fileId, ManagerBlock dataBlock, FyDataEntry data){
        try{
            if (dataBlock == null)
                return false;
            byte[] block = new byte[blockParas.blockSize];
            byte[] entryBytes = data.encodeData(metaData.getColumns().size());
            //long curAddress = fileAccessers[fileId].getFilePointer();
            int entrySize = entryBytes.length;
            // data block format:
            // blockType(4b)token(4b)hashMod(4b)parentAddr(8b)keepBytes(32b)entryNum(4b)dataEntryLen(4b)colId1(4b)colLen1(4b)col1...suppBlockAddr(8b)
            //fileAccessers[fileId].seek(dataBlock.address);
            //fileAccessers[fileId].read(block);

            // entryNum <= 0 means it's an empty block. we should put the entry into it anyway
            // if new entry larger than vacant size of a single block, we should also allocate supplement block
            // blockType(4b)token(4b)hashMod(4b)parentAddr(8b)keepBytes(32b)entryNum(4b)dataEntryLen(4b)colId1(4b)colLen1(4b)col1...suppBlockAddr(8b)
            // suppBlockAddr is only available for those entrySize larger than vacant size of a block. otherwise, is -1L
            // we do not pre-allocate data block for empty block, just set address as -1L
            if (dataBlock.getAddress() < 0){
                // the dummy address is also the identifier to spot the bothers referring to same dummy block
                long dummyBlockAddr = dataBlock.getAddress(); 
                // allocate a block
                block =  buildDataBlock(dataBlock.getHashMod(),dataBlock.getToken(),dataBlock.getParentBlock().getAddress());
                long baseBlockAddr = fileAccessers[fileId].length();

                // assign a new real ManagerBlock to replace the dummy one
                //dataBlock = new ManagerBlock(Consts.DATA,baseBlockAddr,dataBlock.getHashMod(),dataBlock.getToken(),dataBlock.getParentBlock());
                // assinge new allocated address
                dataBlock.setAddress(baseBlockAddr);

                // update parent bmb block
                //dataBlock.getParentBlock().putChild(Integer.valueOf(dataBlock.getHashMod()), dataBlock);
                // update bmb to file
                // blockType(4b)token(4b)hashMod(4b)modNum(4b)parentAddr(8b)keepBytes(32b)entryNum(4b)hashMod1(4)referAddr1(8b)...hashModNum(4)referAddrNum(8b)...
                byte[] bmbBlock = new byte[blockParas.blockSize];
                readBlockFromFile(fileId,bmbBlock,dataBlock.getParentBlock().getAddress(),false);
                int bmbOffSet = 56;
                // actually, children number shoud always equal to bmb's modNum, no necessary to read here
                //int childNum = CommUtility.readIntFromBlock(bmbBlock,56);
                bmbOffSet+=8; // skip entryNum and 1st hashMod
                for (int i=0;i<dataBlock.getParentBlock().getModNum();i++){
                    long childBlockAddr = CommUtility.readLongFromBlock(bmbBlock,bmbOffSet);
                    //ManagerBlock brotherBlock = (ManagerBlock)dataBlock.getParentBlock().getChild(Integer.valueOf(i));
                    //bmbOffSet+=8; 
                    // check if we should update its address
                    // ???compare object's address, not call equal method
                    if (childBlockAddr == dummyBlockAddr){
                        CommUtility.arrayCopy(CommUtility.longToByteArray(baseBlockAddr),0,bmbBlock,bmbOffSet,8);
                        //break;
                    }

                    bmbOffSet+=12; // skip hashMod & addr
                }
                // write the updated bmb block to file
                writeBlockToFile(fileId,bmbBlock,dataBlock.getParentBlock().getAddress(),false);

                int vacantSize = blockParas.blockSize - blockParas.dataBlockHeaderSize - 4 ; // 4b for dataEntryLen
                int fillSize = entrySize > vacantSize ? vacantSize : entrySize;
                // set entryNum
                CommUtility.arrayCopy(CommUtility.intToByteArray(1),0,block,52,4);
                // set entryLen, if can not contain in a single block, fillSize is vacantSize+1, which indicate is has supplement block
                CommUtility.arrayCopy(CommUtility.intToByteArray(fillSize+(entrySize-fillSize>0?1:0)),0,block,56,4);
                // set entry
                CommUtility.arrayCopy(entryBytes,entryBytes.length-entrySize,block,60,fillSize);
                entrySize -= fillSize;
                // update parent bmb data info
                //int bmbMod = CommUtility.multiHashNum(data.key.hashCode(),dataBlock.getParentBlock().getToken())%blockParas.getModNum();
                //int refHashMod = CommUtility.multiHashNum(data.key.hashCode(),dataBlock.getToken())%blockParas.getModNum();
                //putReferAddrToBMB(fileId,dataBlock.getParentBlock(),bmbMod,refHashMod,dataBlock.getAddress());
                //putReferAddrToBMB(fileId,dataBlock.getParentBlock(),dataBlock.getParentBlock().getHashMod(),dataBlock.getHashMod(),dataBlock.getAddress());
                // check if need allocate supplement block
                long suppBlockAddr = baseBlockAddr+blockParas.blockSize;
                if (entrySize > 0)
                    // set suppBlockAddr. the new block to be writen
                    CommUtility.arrayCopy(CommUtility.longToByteArray(suppBlockAddr),0,block,blockParas.blockSize-8,8);
                // write new block to file
                appendBlockToFile(fileId,block);

                // allocate supplement blocks for long entry. add blocks in append mode
                vacantSize = blockParas.blockSize - 52 - 4; // 4b for dataEntryLen
                while (entrySize > 0){
                    //suppBlockAddr = fileAccessers[fileId].length();
                    fillSize = entrySize > vacantSize ? vacantSize : entrySize;
                    block = buildSuppDataBlock(baseBlockAddr);
                    // blockType(4b)parentAddr(8b)keepBytes(32b)entryNum(4b)dataEntryLen(4b)colId1(4b)colLen1(4b)col1...suppBlockAddr(8b)
                    // set entryNum, if can not contain in a single block, fillSize is vacantSize+1, which indicate is has supplement block
                    CommUtility.arrayCopy(CommUtility.intToByteArray(1),0,block,44,4);
                    // set entryLen
                    CommUtility.arrayCopy(CommUtility.intToByteArray(fillSize+(entrySize-fillSize>0?1:0)),0,block,48,4);
                    // set entry
                    CommUtility.arrayCopy(entryBytes,entryBytes.length-entrySize,block,52,fillSize);
                    entrySize -= fillSize;
                    // new baseblock address is current supplement block address
                    baseBlockAddr = fileAccessers[fileId].getFilePointer();
                    // new supplement supplement address is the next block address
                    suppBlockAddr = baseBlockAddr+blockParas.blockSize;
                    // check if need allocate supplement block
                    if (entrySize > 0)
                        // set suppBlockAddr. the next block of current block to be writen
                        CommUtility.arrayCopy(CommUtility.longToByteArray(suppBlockAddr),0,block,blockParas.blockSize-8,8);
                    // write block to file
                    //fileAccessers[fileId].write(block);.
                    // we have called appendBlockToFile, it has reached the end of file. hence, we do not call appendBlockToFile, avoid seek time
                    writeBlockToFile(fileId,block);
                }
            }else{ // it's an allocated data block. we should check if it can contain the new entry, then consider how to split it, or not
                readBlockFromFile(fileId,block,dataBlock.getAddress(),false);
                int offSet = getDataBlockVacantOffset(block);
                int entryNum = CommUtility.readIntFromBlock(block, 52);
                // check if block can contain this new entry. 8b for suppBlockAddr, 4b for dataEntryLen
                if (entrySize <= blockParas.blockSize - offSet - 8 - 4){
                    // update entryNum
                    entryNum++;
                    CommUtility.arrayCopy(CommUtility.intToByteArray(entryNum),0,block,52,4);
                    // set new entryLen
                    CommUtility.arrayCopy(CommUtility.intToByteArray(entrySize),0,block,offSet,4);
                    // set new entry
                    CommUtility.arrayCopy(entryBytes,0,block,offSet+4,entrySize);
                    // write updated block to file
                    writeBlockToFile(fileId,block,dataBlock.getAddress(),true);
                    //fileAccessers[fileId].seek(dataBlock.getAddress());
                    //fileAccessers[fileId].write(block);
                    // reset file pointer to end
                    //fileAccessers[fileId].seek(curAddress);
                }else{ // it can't contain the new entry, we should split current block
                    // check if there multiple hashMod referring to this block, which will decide we should update the bmb modNum or not.
                    Integer[] dummyHashMods = new Integer[0];
                    Iterator it = dataBlock.getParentBlock().getChildren().keySet().iterator();
                    while (it.hasNext()){
                        Integer hashMOD = (Integer)it.next();
                        ManagerBlock brothBlock = (ManagerBlock)dataBlock.getParentBlock().getChild(hashMOD);;
                        // ???compare the object address instead of data block's address, coz the diff children block may refer -1L
                        if (brothBlock.getAddress() == dataBlock.getAddress())
                            dummyHashMods = (Integer[])CommUtility.appendToArray(dummyHashMods, hashMOD);
                    }

                    //int curToken = CommUtility.readIntFromBlock(block, 4);
                    // int curToken = dataBlock.getToken();
                    offSet = 56; // offset of first entry in the block
                    // get existing entries
                    MemHashKVData existingEntries = (MemHashKVData)readEntriesFromDataBlock(fileId,block);

                    // if we need not update the bmb modNum (duplicatedRefer > 1), 
                    // we should allocate a new block, and update bmb referring address, then put the entries to corresponding block
                    if (dummyHashMods.length <= 1){
                        // if we should update bmb modNum, we should check its limitation first.
                        if (dataBlock.getParentBlock().getModNum()*2 <= blockParas.getModNum()){
                            // if modNum can be updated, we do not allocate new block at here, 
                            // just double bmb modNum & children number, assign the new children as dummy, referring to existing children
                            // and call the process again.
                            byte[] bmbBlock = new byte[blockParas.blockSize];
                            readBlockFromFile(fileId, bmbBlock, dataBlock.getParentBlock().getAddress(), false);
                            int updatedModNum = dataBlock.getParentBlock().getModNum()*2;
                            // update parent children mapping correspondingly. add new children (dummy)
                            // also update parent BMB
                            // blockType(4b)token(4b)hashMod(4b)modNum(4b)parentAddr(8b)keepBytes(32b)entryNum(4b)hashMod1(4)referAddr1(8b)...hashModNum(4)referAddrNum(8b)...
                            int bmbOffSet = 60+12*dataBlock.getParentBlock().getModNum();
                            for (int i=dataBlock.getParentBlock().getModNum();i<updatedModNum;i++){
                                // the new managerblocks are dummy blocks, referring to the old managerblock
                                ManagerBlock odlDataBlock = (ManagerBlock)dataBlock.getParentBlock().getChild(Integer.valueOf(i-dataBlock.getParentBlock().getModNum()));

                                // new dummy managerblocks with address -1L
                                // ManagerBlock newDataBlock = new ManagerBlock(Consts.DATA,-1L,i,dataBlock.getToken(),dataBlock.getParentBlock());

                                //newDataBlock.setBlockType(odlDataBlock.getBlockType());
                                //newDataBlock.setAddress(odlDataBlock.getAddress());
                                //newDataBlock.setHashMod(odlDataBlock.getHashMod());
                                //newDataBlock.setToken(odlDataBlock.getToken());
                                //newDataBlock.setParentBlock(odlDataBlock.getParentBlock());
                                //newDataBlock.reNewChildren();
                                //newDataBlock.setModNum(odlDataBlock.getModNum());
                                dataBlock.getParentBlock().putChild(new Integer(i),odlDataBlock);

                                // set new child
                                CommUtility.arrayCopy(CommUtility.intToByteArray(i), 0, bmbBlock, bmbOffSet, 4);
                                bmbOffSet+=4;
                                CommUtility.arrayCopy(CommUtility.longToByteArray(odlDataBlock.getAddress()), 0, bmbBlock, bmbOffSet, 8);
                                bmbOffSet+=8;
                            }
                            // update modNum
                            dataBlock.getParentBlock().setModNum(updatedModNum);
                            CommUtility.arrayCopy(CommUtility.intToByteArray(updatedModNum), 0, bmbBlock, 12, 4);
                            // update children number
                            CommUtility.arrayCopy(CommUtility.intToByteArray(updatedModNum), 0, bmbBlock, 56, 4);
                            // write updated bmb block to file
                            writeBlockToFile(fileId,bmbBlock,dataBlock.getParentBlock().getAddress(),false);

                            // get the block which the new entry insert into
                            int newHashMod = CommUtility.multiHashNum(data.key.hashCode(),dataBlock.getParentBlock().getToken())%dataBlock.getParentBlock().getModNum();
                            dataBlock = (ManagerBlock)dataBlock.getParentBlock().getChild(Integer.valueOf(newHashMod));

                            // put data into splitted data block again.
                            return putEntryToBlock(fileId, dataBlock, data);
                        }else{
                            // modNum reached the limitation, 
                            // we should update current data as new bmb block (to avoid update its parent)
                            // and allocate a new data block
                            int dataBlockToken = dataBlock.getToken()+1;
                            long bmbAddr = dataBlock.getAddress();
                            // allocate a new block for the new bmb block
                            long dataBlockAddr = fileAccessers[fileId].length();

                            // copy current block content to new data block
                            //byte[] newDataBlock = new byte[blockParas.blockSize];
                            //CommUtility.arrayCopy(block,0,newDataBlock,0,block.length);
                            //int curOffset = 4;
                            //CommUtility.arrayCopy(CommUtility.intToByteArray(dataBlockToken), 0, newDataBlock, curOffset, 4);
                            //curOffset+=4;
                            //int newHashMod = CommUtility.multiHashNum(data.key.hashCode(),dataBlock.getToken()+1)%2;
                            //CommUtility.arrayCopy(CommUtility.intToByteArray(newHashMod), 0, newDataBlock, curOffset, 4);
                            // write new block to file
                            //long dataBlockAddr = fileAccessers[fileId].length();
                            //appendBlockToFile(fileId,newDataBlock);

                            // upgrade current data block to bmb block. its children refer to the new data block
                            // bmb format:
                            // blockType(4b)token(4b)hashMod(4b)modNum(4b)parentAddr(8b)keepBytes(32b)entryNum(4b)hashMod1(4)referAddr1(8b)...hashModNum(4)referAddrNum(8b)...
                            // then we call this process again!
                            byte[] bmbBBlock = buildBlockManagerBlock(dataBlock.getHashMod(),dataBlock.getToken(),dataBlock.getParentBlock().getAddress());
                            int bmbOffSet = 56;
                            // set entryNum
                            CommUtility.arrayCopy(CommUtility.intToByteArray(2), 0, bmbBBlock, bmbOffSet, 4);
                            bmbOffSet+=4;
                            // 1st child entry hashMod
                            CommUtility.arrayCopy(CommUtility.intToByteArray(0), 0, bmbBBlock, bmbOffSet, 4);
                            bmbOffSet+=4;
                            // the child refer to current data block
                            CommUtility.arrayCopy(CommUtility.longToByteArray(dataBlockAddr), 0, bmbBBlock, bmbOffSet, 8);
                            bmbOffSet+=8;
                            // 2nd child entry hashMod
                            CommUtility.arrayCopy(CommUtility.intToByteArray(1), 0, bmbBBlock, bmbOffSet, 4);
                            bmbOffSet+=4;
                            // 2nd child also refer to current data block
                            CommUtility.arrayCopy(CommUtility.longToByteArray(dataBlockAddr), 0, bmbBBlock, bmbOffSet, 8);
                            bmbOffSet+=8;

                            // and update new block's token,hashMod,parentAddr (the image of current block)
                            // blockType(4b)token(4b)hashMod(4b)parentAddr(8b)keepBytes(32b)entryNum(4b)dataEntryLen(4b)colId1(4b)colLen1(4b)col1...suppBlockAddr(8b)
                            // update token
                            CommUtility.arrayCopy(CommUtility.intToByteArray(dataBlockToken), 0, block, 4, 4);
                            // update hashMod
                            CommUtility.arrayCopy(CommUtility.intToByteArray(0), 0, block, 8, 4);
                            // update parentAddr
                            CommUtility.arrayCopy(CommUtility.longToByteArray(bmbAddr), 0, block, 12, 8);

                            // update the memory structure. current ManagerBlock become bmb block.
                            ManagerBlock newManagerBlcok = dataBlock;
                            // update the get dataBlock to a BMB
                            newManagerBlcok.setBlockType(Consts.BMB);
                            newManagerBlcok.setAddress(bmbAddr);
                            newManagerBlcok.setModNum(2);
                            // new block become the child of the current BMB
                            // newManagerBlcok.reNewChildren;
                            dataBlock = new ManagerBlock(Consts.DATA,dataBlockAddr,0,dataBlockToken,newManagerBlcok);
                            newManagerBlcok.putChild(new Integer(0),dataBlock);
                            newManagerBlcok.putChild(new Integer(1),dataBlock);

                            // write updated bmb block to file
                            writeBlockToFile(fileId,bmbBBlock,bmbAddr,false);
                            // allocate new data block
                            appendBlockToFile(fileId,block);

                            // put data into splitted data block again.
                            return putEntryToBlock(fileId, dataBlock, data);
                        }
                    }else{
                        // split into 2 new blocks. 
                        byte[][] newBlocks = new byte[2][0]; // new 2 empty blocks
                        int oldDummyModNum = dataBlock.getParentBlock().getModNum()/dummyHashMods.length;
                        int newDummyModNum = oldDummyModNum*2;
                        int entryHashCode = CommUtility.multiHashNum(data.key.hashCode(),dataBlock.getParentBlock().getToken());
                        //int entryHashMod = CommUtility.multiHashNum(data.key.hashCode(),dataBlock.getParentBlock().getToken())%newDummyModNum;
                        int updateId = (entryHashCode%newDummyModNum-entryHashCode%oldDummyModNum)/(newDummyModNum/2);
                        ManagerBlock[] newDataBlocks = new ManagerBlock[2]; 
                        //newDataBlocks[0] = updateId==0?new ManagerBlock(Consts.DATA,-1L,0,dataBlock.getToken(),dataBlock.getParentBlock()):dataBlock;
                        //newDataBlocks[1] = updateId==1?new ManagerBlock(Consts.DATA,-1L,1,dataBlock.getToken(),dataBlock.getParentBlock()):dataBlock;
                        // not update modNum, parent will not change
                        long parentAddr = dataBlock.getParentBlock().getAddress();

                        // put existing entries to corresponding new block
                        int vacantSize = blockParas.blockSize - blockParas.dataBlockHeaderSize - 4 ; // 4b for dataEntryLen;
                        it = existingEntries.keySet().iterator();
                        // we want to assinge the first existing entry to the existing block, to avoid allocate empty block
                        boolean existingBlockAssigned = false;
                        // if existing entries belong to 2 diff blocks, we should allocate a new block
                        int newBlcokId = -1;
                        int oldBlockId = -1;
                        while (it.hasNext()){
                            HashMap key = (HashMap)it.next();
                            HashMap value = (HashMap)existingEntries.get(key);
                            int dummyHashCode = CommUtility.multiHashNum(key.hashCode(),dataBlock.getParentBlock().getToken());
                            //int dummyHashMod = CommUtility.multiHashNum(key.hashCode(),dataBlock.getParentBlock().getToken())%newDummyModNum;
                            int blockId = (dummyHashCode%newDummyModNum-dummyHashCode%oldDummyModNum)/(newDummyModNum/2);
                            if (newBlocks[blockId] == null || newBlocks[blockId].length == 0){
                                newBlocks[blockId] = buildDataBlock(blockId,dataBlock.getToken(),parentAddr);
                                // copy suppBlockAddr
                                //CommUtility.arrayCopy(block,blockParas.blockSize-8,newBlocks[blockId],blockParas.blockSize-8,8);
                                if (!existingBlockAssigned) {
                                    //newDataBlocks[blockId].setAddress(dataBlock.getAddress());
                                    existingBlockAssigned = true;
                                    oldBlockId = blockId;
                                    newDataBlocks[oldBlockId] = dataBlock;
                                }else{ 
                                    // if entry not belong to old data block, we should allocate new block and assigen new block address
                                    if (blockId != oldBlockId){
                                        //newDataBlocks[blockId].setAddress(fileAccessers[fileId].length());
                                        newBlcokId = blockId;
                                        newDataBlocks[newBlcokId] = new ManagerBlock(Consts.DATA,fileAccessers[fileId].length(),1,dataBlock.getToken(),dataBlock.getParentBlock());
                                    }
                                }
                            }

                            // assign to corresponding block
                            //for (int i=0;i<dummyHashMods.length;i++)
                            //    if (dummyHashMod==dummyHashMods[i]){
                            //        blockId = i%2;
                            //        break;
                            //    }
                            // test if corresponding block initialzied
                            int newOffSet = getDataBlockVacantOffset(newBlocks[blockId]);
                            int newEntryNum = CommUtility.readIntFromBlock(newBlocks[blockId], 52)+1;
                            byte[] newEntryBytes = (new FyDataEntry(key,value)).encodeData(metaData.getColumns().size());
                            // update entryNum
                            CommUtility.arrayCopy(CommUtility.intToByteArray(newEntryNum),0,newBlocks[blockId],52,4);
                            // set new entryLen
                            CommUtility.arrayCopy(CommUtility.intToByteArray(newEntryBytes.length>vacantSize?vacantSize+1:newEntryBytes.length),0,newBlocks[blockId],newOffSet,4);
                            // set new entry
                            CommUtility.arrayCopy(newEntryBytes,0,newBlocks[blockId],newOffSet+4,newEntryBytes.length>vacantSize?vacantSize:newEntryBytes.length);
                            // if entry length larger than vacant size of a block, we should also copy the suppBlockAddr
                            if (newEntryBytes.length>vacantSize)
                                CommUtility.arrayCopy(block,blockParas.blockSize-8,newBlocks[blockId],blockParas.blockSize-8,8);
                            //offSet+=newEntryBytes.length;
                        }
                        
                        // if no need to allocate new block, we should new a dummy ManagerBlock
                        // we assinge 0 as address for the dummy block at here, will assign another id in the following step
                        if (newBlcokId < 0)
                            newDataBlocks[oldBlockId==0?1:0] = new ManagerBlock(Consts.DATA,0,1,dataBlock.getToken(),dataBlock.getParentBlock());

                        // detect which block the new entry belong to.
                        //for (int i=0;i<2;i++){
                        //    // detect that which managerblock the new entry belong to
                        //    if ((entryHashCode%newDummyModNum-entryHashCode%oldDummyModNum)/(newDummyModNum/2) == i)
                        //        updateBlock = newDataBlocks[i];
                        //}

                        // assign existing dummy managerblocks to the new block
                        // and set a real managerblock to replace the dummy block
                        for(int i=0;i<dummyHashMods.length;i++){
                            int blockId = (dummyHashMods[i]%newDummyModNum-dummyHashMods[i]%oldDummyModNum)/(newDummyModNum/2);
                            //ManagerBlock dummyBlock = (ManagerBlock)dataBlock.getParentBlock().getChild(dummyHashMods[i]);
                            // assign a identifyable id for dummy block
                            if (newDataBlocks[blockId].getAddress() == 0)
                                newDataBlocks[blockId].setAddress((long)(-dummyHashMods[i]-1));
                            dataBlock.getParentBlock().putChild(Integer.valueOf(dummyHashMods[i]),newDataBlocks[blockId]);
                        }
                        // and also update their parent bmb block
                        // blockType(4b)token(4b)hashMod(4b)modNum(4b)parentAddr(8b)keepBytes(32b)entryNum(4b)hashMod1(4)referAddr1(8b)...hashModNum(4)referAddrNum(8b)...
                        byte[] bmbBlock = new byte[blockParas.blockSize];
                        readBlockFromFile(fileId,bmbBlock,dataBlock.getParentBlock().getAddress(),false);
                        int bmbOffSet = 56;
                        // actually, children number shoud always equal to bmb's modNum, no necessary to read here
                        //int childNum = CommUtility.readIntFromBlock(bmbBlock,56);
                        bmbOffSet+=4;
                        for (int i=0;i<dataBlock.getParentBlock().getModNum();i++){
                            int childHashMod = CommUtility.readIntFromBlock(bmbBlock,bmbOffSet);
                            bmbOffSet+=4; 
                            // check if we should update its address
                            for (int j=0;j<dummyHashMods.length;j++)
                                if (dummyHashMods[j] == childHashMod){
                                    int blockId = (dummyHashMods[j]%newDummyModNum-dummyHashMods[j]%oldDummyModNum)/(newDummyModNum/2);
                                    CommUtility.arrayCopy(CommUtility.longToByteArray(newDataBlocks[blockId].getAddress()),0,bmbBlock,bmbOffSet,8);
                                }

                            bmbOffSet+=8; // skip address
                        }
                        // write the updated bmb block to file
                        writeBlockToFile(fileId,bmbBlock,dataBlock.getParentBlock().getAddress(),false);

                        // write updated data block to disk
                        // oldBlockId should always be assinged!!
                        if (oldBlockId >= 0)
                            writeBlockToFile(fileId, newBlocks[oldBlockId], dataBlock.getAddress(), false);
                        else
                            dtrace.trace(10);

                        // write new block to disk
                        if (newBlcokId >= 0)
                            appendBlockToFile(fileId, newBlocks[newBlcokId]);

                        // put data into splitted data block.
                        return putEntryToBlock(fileId, newDataBlocks[updateId], data);
                    }
                }
            }
        }catch(Exception e){
            if (debuger.isDebugMode())
                e.printStackTrace();
            return false;
        }
        return true;
    }

    // remove entry from a datablock.
    private boolean pullEntryFromBlock(int fileId, ManagerBlock dataBlock, HashMap key){
        try{
            if (dataBlock == null || dataBlock.getAddress() < 0)
                return false;
            byte[] block = new byte[blockParas.blockSize];
            // data block format:
            // blockType(4b)token(4b)hashMod(4b)parentAddr(8b)keepBytes(32b)entryNum(4b)dataEntryLen(4b)colId1(4b)colLen1(4b)col1...suppBlockAddr(8b)
            readBlockFromFile(fileId,block,dataBlock.address,false);
            int offSet = 52;
            int entryNum = CommUtility.readIntFromBlock(block,offSet);
            offSet+=4;
            boolean removed = false;
            for (int i=0;i<entryNum;i++){
                FyDataEntry readData = new FyDataEntry();
                int entryLen = CommUtility.readIntFromBlock(block,offSet);
                offSet+=4;
                readData.decodeData(block,offSet,entryLen,metaData.getKeyColumns().size(),blockParas.encoding);
                // spotted the position of the entry to be deleted, we would remove it and move remain entries forward, and update entryNum
                // we can just simply copy following bytes until blockSize - 8 to current position
                if (readData.key.equals(key)){
                    // move remain entries forward
                    CommUtility.arrayCopy(block,offSet+entryLen,block,offSet-4,blockParas.blockSize-8-offSet-entryLen);
                    // update entryNum
                    CommUtility.arrayCopy(CommUtility.intToByteArray(entryNum-1),0,block,52,4);
                    writeBlockToFile(fileId,block,dataBlock.address,false);
                    removed = true;
                    break;
                }
                offSet+=entryLen;
            }
            return removed;
        }catch(Exception e){
            if (debuger.isDebugMode())
                e.printStackTrace();
            return false;
        }
    }
            
    // modify entry in a datablock.
    // identify the entry, generate a new entry, then delete the old one, inser the new one
    // valueChanges-- colId:(oldValue,newValue)
    private boolean modifyEntryInBlock(int fileId, ManagerBlock dataBlock, HashMap key, HashMap valueChanges){
        try{
            if (dataBlock == null || dataBlock.getAddress() < 0)
                return false;
            byte[] block = new byte[blockParas.blockSize];
            // data block format:
            // blockType(4b)token(4b)hashMod(4b)parentAddr(8b)keepBytes(32b)entryNum(4b)dataEntryLen(4b)colId1(4b)colLen1(4b)col1...suppBlockAddr(8b)
            readBlockFromFile(fileId,block,dataBlock.address,false);
            int offSet = 52;
            int entryNum = CommUtility.readIntFromBlock(block,offSet);
            offSet+=4;
            boolean modified = false;
            for (int i=0;i<entryNum;i++){
                FyDataEntry readData = new FyDataEntry();
                int entryLen = CommUtility.readIntFromBlock(block,offSet);
                offSet+=4;
                byte[] entryBytes = new byte[entryLen];
                readData.decodeData(block,offSet,entryLen,metaData.getKeyColumns().size(),blockParas.encoding);
                // spotted the position of the entry to be moidfie, 
                // we will generate a new one base on existing data and valueChanges,
                // then remove it and move remain entries forward, and update entryNum, eventually insert the new one
                if (readData.key.equals(key)){
                    // move remain entries forward
                    CommUtility.arrayCopy(block,offSet+entryLen,block,offSet-4,blockParas.blockSize-8-offSet-entryLen);
                    // update entryNum
                    CommUtility.arrayCopy(CommUtility.intToByteArray(entryNum-1),0,block,52,4);
                    // generate a new entry
                    Iterator it = valueChanges.keySet().iterator();
                    while (it.hasNext()){
                        Integer colID = (Integer)it.next();
                        FyDataLogEntry.valueChangeEntry changeEntry = (FyDataLogEntry.valueChangeEntry)valueChanges.get(colID);
                        readData.value.put(colID,changeEntry.newValue);
                    }
                    // write the block with deleted data to file
                    writeBlockToFile(fileId,block,dataBlock.address,false);
                    // insert the new entry
                    modified = putEntryToBlock(fileId,dataBlock,readData);
                    break;
                }
                offSet+=entryLen;
            }
            return modified;
        }catch(Exception e){
            if (debuger.isDebugMode())
                e.printStackTrace();
            return false;
        }
    }
            
    // get a data entry from a data block
    private FyDataEntry getEntryFromBlock(int fileId, HashMap key, byte[] block){
        int blockType = CommUtility.readIntFromBlock(block,0);
        int vacantSize = blockParas.blockSize - blockParas.dataBlockHeaderSize - 4; // 4b for dataEntryLen
        if (blockType == Consts.DATA){
            int entryNum = CommUtility.readIntFromBlock(block,52);
            int offSet = 56;
            FyDataEntry data = new FyDataEntry();
            for (int i=0;i<entryNum;i++){
                int entryLen = CommUtility.readIntFromBlock(block,offSet);
                offSet+=4;
                byte[] entryBytes = new byte[0];
                // entryLen > vacantSize indicate that it has bytes storing in supplement block
                if (entryLen > vacantSize){
                    // get current bytes first
                    entryBytes = new byte[vacantSize];
                    CommUtility.arrayCopy(block,offSet,entryBytes,0,entryBytes.length);
                    long suppBlockAddr = CommUtility.readLongFromBlock(block,blockParas.blockSize-8);
                    byte[] suppBytes = readSuppBytes(fileId, suppBlockAddr);
                    entryBytes = CommUtility.appendToArrayB(entryBytes,suppBytes);
                }else{
                    entryBytes = new byte[entryLen];
                    CommUtility.arrayCopy(block,offSet,entryBytes,0,entryBytes.length);
                }
                data.decodeData(entryBytes,0,entryBytes.length,metaData.getKeyColumns().size(),blockParas.encoding);
                offSet+=entryLen;
                if (data.key.equals(key))
                    return data;
            }
        }
        return null;
    }

    // fill a dataEntry into a data bock
    private boolean fillDataEntry(FyDataEntry data){
        // get a data block address for a new data entry
        int hashCode = data.key.hashCode();
        int fileId = hashCode%fileModNum;
        if (fileAccessers[fileId] == null && !newFileAccesser(fileId))
            return false;
        ManagerBlock dataBlock = rootManagers[fileId].getDataBlock(hashCode);
        if (dataBlock == null)
            return false;
        return putEntryToBlock(fileId, dataBlock, data);
    }

     // remove a dataEntry 
    private boolean removeDataEntry(HashMap key){
        // get a data block address for a new data entry
        int hashCode = key.hashCode();
        int fileId = hashCode%fileModNum;
        if (fileAccessers[fileId] == null && !newFileAccesser(fileId))
            return false;
        ManagerBlock dataBlock = rootManagers[fileId].getDataBlock(hashCode);
        if (dataBlock == null)
            return false;
        return pullEntryFromBlock(fileId, dataBlock, key);
    }

    // change content of a dataEntry. it will involve a delete and insert operation
    private boolean changeDataEntry(HashMap key, HashMap valueChanges){
       // get a data block address for a new data entry
       int hashCode = key.hashCode();
       int fileId = hashCode%fileModNum;
       if (fileAccessers[fileId] == null && !newFileAccesser(fileId))
           return false;
       ManagerBlock dataBlock = rootManagers[fileId].getDataBlock(hashCode);
       if (dataBlock == null)
           return false;
       return modifyEntryInBlock(fileId, dataBlock, key, valueChanges);
    }

    // read a dataEntry from data block
    // format of datablock
    // keepBytes(64b)blockType(4b)entryNum(4b)colNum(4b)colId1(4b)colLen1(4b)col1...
    public FyDataEntry next(BP bp){
        if (!phyinited){
            dtrace.trace(124);
            return null;
        }
        if (!loading){
            dtrace.trace(126);
            return null;
        }
        // reached the end. clean memData
        if (memData == null || curEntryIt == null || !curEntryIt.hasNext()){
            memData = null;
            loading = false;
            return null;
        }
        HashMap key = (HashMap)curEntryIt.next();
        HashMap value = (HashMap)memData.get(key);
        FyDataEntry data = new FyDataEntry(key,value);
        return data;
    }

    /*
    // read a dataEntry from data block
    // format of datablock
    // keepBytes(64b)blockType(4b)entryNum(4b)colNum(4b)colId1(4b)colLen1(4b)col1...
    public FyDataEntry next2(BP bp){
        if (!phyinited){
            dtrace.trace(124);
            return null;
        }
        if (!loading){
            dtrace.trace(126);
            return null;
        }
        FyDataEntry data = null;
        if (fileAccessers == null){
            loading = false;
            return data;
        }
        byte[] entryBytes = readEntryBytes();
        if (entryBytes != null && entryBytes.length > 0){
            data = new FyDataEntry();
            try{
                data.decodeData(entryBytes,0,entryBytes.length,metaData.getKeyColumns().size(),blockParas.encoding);
            }catch(Exception e){
                dtrace.trace(10);
                if (debuger.isDebugMode())
                    e.printStackTrace();
                data = null;
            }
        }
        loading = data!=null;
        return data;
    }//*/

    // implement a data change log entry  (op = MODIFY)
    private boolean updateData(FyDataLogEntry.LogContent logData){
        if (!phyinited){
            dtrace.trace(124);
            return false;
        }
        if (loading){
            dtrace.trace(125);
            return false;
        }
        return changeDataEntry(logData.key,logData.value);
    }

    // insert a data change log entry  (op = INSERT)
    private boolean insertData(FyDataLogEntry.LogContent logData){
        if (!phyinited){
            dtrace.trace(124);
            return false;
        }
        if (loading){
            dtrace.trace(125);
            return false;
        }
        return insertData(logData.key,logData.value);
    }

    // delete a data change log entry  (op = DELETE)
    private boolean deleteData(FyDataLogEntry.LogContent logData){
        if (!phyinited){
            dtrace.trace(124);
            return false;
        }
        if (loading){
            dtrace.trace(125);
            return false;
        }
        return removeDataEntry(logData.key);
    }

     // update data to physical data source
    private boolean implementLog(FyDataLogEntry.LogContent logData){ // update data to physical data source
        if (!phyinited){
            dtrace.trace(124);
            return false;
        }
        if (loading){
            dtrace.trace(125);
            return false;
        }
        switch (logData.op){
            case Consts.INSERT:
                return insertData(logData);
            case Consts.MODIFY:
                return updateData(logData);
            case Consts.DELETE:
                return deleteData(logData);
            default:
                return false;
        }
    }

    public int implementLog(TreeMap logs, boolean ignoreFails){  // batch update data to physical data source, return failed number
        if (!phyinited){
            dtrace.trace(124);
            return 0;
        }
        if (loading){
            dtrace.trace(125);
            return 0;
        }
        int fails = 0;
        Iterator it = logs.keySet().iterator();
        while (it.hasNext()){
            BP bp = (BP)it.next();
            FyDataLogEntry log = (FyDataLogEntry)logs.get(bp);
            if (log == null || log.logType != Consts.DATA)
                continue;
            debuger.printMsg(bp.encodeString()+"("+log.logData.op+"): "+log.logData.key.toString()+": "+log.logData.value.toString(),true);
            if (!implementLog(log.logData))
                fails++;
            debuger.printMsg("ok",true);
            if (!ignoreFails && fails>0)
                break;
        }
        synFiles();
        return fails;
    }

    public boolean insertData(HashMap key, HashMap value){ // insert data to data source 
        if (!phyinited){
            dtrace.trace(124);
            return false;
        }
        if (loading){
            dtrace.trace(125);
            return false;
        }
        return fillDataEntry(new FyDataEntry(key,value));
    }

    // batch insert data to data source (file), input a Array of FyDataEntry, return fails number
    public int insertData(ArrayList datas, boolean ignoreFails){ // batch insert data to data source (db), input a Array of FyDataEntry, return fails number
        int fails = 0;
        if (!phyinited){
            dtrace.trace(124);
            return 0;
        }
        if (loading){
            dtrace.trace(125);
            return 0;
        }
        for (int i=0; i<datas.size(); i++){
            FyDataEntry data = (FyDataEntry)datas.get(i);
            if (!fillDataEntry(data)){
                fails++;
                if (!ignoreFails)
                    return fails;
            }
        }
        return fails;
    }

    public boolean deleteData(HashMap key) {// delete data from data source
        if (!phyinited){
            dtrace.trace(124);
            return false;
        }
        if (loading){
            dtrace.trace(125);
            return false;
        }
        return removeDataEntry(key);
    }

    // batch delete data to data source (file), input a Array of key(HashMap), return fails number
    public int deleteData(ArrayList datas, boolean ignoreFails){ // batch delete data to data source (db), input a Array of key(HashMap), return fails number
        int fails = 0;
        if (!phyinited){
            dtrace.trace(124);
            return 0;
        }
        if (loading){
            dtrace.trace(125);
            return 0;
        }
        for (int i=0; i<datas.size(); i++){
            HashMap key = (HashMap)datas.get(i);
            if (!removeDataEntry(key)){
                fails++;
                if (!ignoreFails)
                    return fails;
            }
        }
        return 0;
    }

    public long getCount(){
        if (!phyinited){
            dtrace.trace(124);
            return 0;
        }
        if (loading){
            dtrace.trace(125);
            return 0;
        }
        return 0;
    }

    public HashMap getDataProps(){
        HashMap dataProps = new HashMap();
        dataProps.put("fileModNum",new Integer(fileModNum));
        dataProps.put("fileToken",new Integer(fileToken));
        dataProps.put("blockSize",new Integer(blockParas.blockSize));
        dataProps.put("encoding",blockParas.encoding);
        return dataProps;
    }

    // physical read a data entry. for test purpose so far
    public FyDataEntry physicalReadEntry(HashMap key){
        int hashCode = key.hashCode();
        int fileId = hashCode%fileModNum;
        if (fileAccessers[fileId] == null && !newFileAccesser(fileId))
            return null;
        ManagerBlock dataBlock = rootManagers[fileId].getDataBlock(hashCode);
        if (dataBlock == null || dataBlock.getAddress() < 0)
            return null;
        byte[] block = new byte[blockParas.blockSize];
        readBlockFromFile(fileId,block,dataBlock.getAddress(),false);
        return getEntryFromBlock(fileId, key, block);
    }
    
    // for test purpose
    public void dump(){
        for (int i=0;i<fileModNum;i++)
            rootManagers[i].dump(null,fileName+i,0);
    }

    // for test purpose
    public MemBaseData readEntriesFromBmb(int fileId, ManagerBlock mBlock){
        MemBaseData entries = new MemHashKVData(dtrace);
        if (mBlock.getBlockType() == Consts.DATA && mBlock.address >= 0){
            byte[] block = new byte[blockParas.blockSize];
            readBlockFromFile(fileId, block, mBlock.address, false);
            entries.putAll((HashMap)readEntriesFromDataBlock(fileId, block));
        }else if (mBlock.getBlockType() == Consts.BMB){
            HashSet countedChildren = new HashSet();
            Iterator it = mBlock.getChildren().keySet().iterator();
            while (it.hasNext()){
                Integer childHashMod = (Integer)it.next();
                ManagerBlock childBlock = (ManagerBlock)mBlock.getChild(childHashMod);
                if (!countedChildren.contains(childBlock.address)){
                    entries.putAll((HashMap)readEntriesFromBmb(fileId,childBlock));
                    countedChildren.add(childBlock.address);
                }
            }
        }
        return entries;
    }

    // for test purpose
    public MemBaseData fullRead(){
        MemBaseData entries = new MemHashKVData(dtrace);
        for (int i=0;i<fileModNum;i++){
            if (i<rootManagers.length && rootManagers[i] != null){
                entries.putAll((HashMap)readEntriesFromBmb(i,rootManagers[i]));
            }
        }
        return entries;
    }

    // for push data from remote server
    public void assignMetaData(FyMetaData metaData){
        phyinited = true;
        loading = false;
        this.metaData = metaData;
    }
    
    // for push data from remote server
    public void assignDataProps(HashMap dataProps){
        this.fileName = (String)dataProps.get("fileName");
        this.fileDir = (String)dataProps.get("fileDir");
        this.fileModNum = (Integer)dataProps.get("fileModNum");
        this.fileToken = (Integer)dataProps.get("fileToken");
        blockParas.encoding = (String)dataProps.get("encoding");
        blockParas.assignBlockSize((Integer)dataProps.get("blockSize"));
    }
    
    public void prepareCopy(){
        phyinited = true;
        loading = false;
    }

    // release resource
    public boolean release(){
        for (int i=0;i<fileModNum;i++){
            try{
                rootManagers[i] = null;
                fileAccessers[i].close();
            }catch(Exception e){
                dtrace.trace(10);
                if (debuger.isDebugMode())
                    e.printStackTrace();
                continue;
            }
        }
        return true;
    }
}
