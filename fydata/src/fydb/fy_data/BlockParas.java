/*
 * @(#)PhyHashData.java	0.01 11/06/14
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_data;

// general parameters for block
public class BlockParas {
    public static int blockSize = 2048; // block size. default is 2048
    public static int dataBlockHeaderSize = 64; // block header size. defined as 64 (blockType(4b)token(4b)hashMod(4b)parentAddr(8b)keepBytes(32b)entryNum(4b)suppBlockAddr(8b))
    public static String encoding = "UTF-8"; // encoding. default is utf8
    private static int modNum = (blockSize-56)/12;      // number using for mod hashNum

    public BlockParas() {
    }
    
    // since we shoul calculate the maximum modNum from blockSize, 
    // we should call this fuction to assign data for blockSize, instead assign to it directly
    public void assignBlockSize(int newBlockSize){
        blockSize = newBlockSize;
        modNum = (blockSize-56)/12;
    }

    // since aximum modNum is decided by blockSize, we should not assign data to it directly
    private void assignModNum(int newModNum){
        int maxModNum = (blockSize-60)/12;
        modNum = newModNum <= maxModNum?newModNum:maxModNum;
    }
    
    public int getModNum(){
        return modNum;
    }
}
