/**
 * @(#)CommUtility.java	0.01 11/05/23
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_comm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import java.io.UnsupportedEncodingException;

import java.lang.reflect.Array;

import java.net.InetAddress;

import java.net.UnknownHostException;

import java.nio.charset.Charset;

import java.security.MessageDigest;

import java.sql.Timestamp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

/**
 * @(#)CommUtility.java	0.01 11/05/23
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
public final class

CommUtility {
    public static char[] hexChars = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };  

    public CommUtility() {
    }
    
    // split input command line into pieces; \ is escape char, " and splitor could be escaped.
    // space quoted in "" should be ignored. \" is character '"'
    // splitString("ad cas asdfa", ' ', true)  => {ad,cas,asdfa}
    // splitString("ad "cas\" asdfa"", ' ', true) => {ad,cas\" asdfa}
    public static String[] splitString(String initialString, char splitor, boolean noDuplicatedSplitor)
    {
        String[] splittedString = new String[0];
        boolean quoteStarted = false;
        int lastPos = 0;
        for (int i=0;i<initialString.length();i++){
            if (initialString.charAt(i) == '"'){
                quoteStarted = !quoteStarted;
                initialString = initialString.substring(0, i)+(i<initialString.length()-1?initialString.substring(i+1):"");
                i--;
            }else if(initialString.charAt(i) == '\\' && i<initialString.length()-1 && 
                    ((initialString.charAt(i+1) == '"' || initialString.charAt(i+1) == splitor))){
                i++; // skip escaped " :\"
                initialString = initialString.substring(0, i-1)+initialString.substring(i);
            }else if(initialString.charAt(i) == splitor && !quoteStarted){ // splitor that not between quato are the real splitor
                //String[] oldCommand = splittedString;
                //splittedString = new String[splittedString.length + 1];
                //arrayCopy(oldCommand,0,splittedString,0,oldCommand.length);
                //splittedString[splittedString.length-1] = new String(trim(initialString.substring(lastPos, i), splitor));
                splittedString = (String[])appendToArray(splittedString,new String(trim(initialString.substring(lastPos, i), splitor)));
                if (noDuplicatedSplitor)
                    while (i<initialString.length()-1 && initialString.charAt(i+1) == ' ') // skip duplicated splitor
                        i++;
                lastPos = i;
            }
        }

        if (quoteStarted){
            System.out.println("");
            System.out.println("Warning: Double quote mismatched!");
            System.out.println("");
        }

        if (lastPos < initialString.length()&& !quoteStarted){
            //String[] oldCommand = splittedString;
            //splittedString = new String[splittedString.length + 1];
            //arrayCopy(oldCommand,0,splittedString,0,oldCommand.length);
            //splittedString[splittedString.length-1] = trim(initialString.substring(lastPos), splitor);
            splittedString = (String[])appendToArray(splittedString,new String(trim(initialString.substring(lastPos), splitor)));
        }
        //for (int i=0; i<splittedCommand.length; i++)
        //    splittedCommand[i] = splittedCommand[i].replace('|', ' ');
        return splittedString;
    }

    // split input command line into pieces; \ is escape char, " and splitor could be escaped.
    // space quoted in "" should be ignored. \" is character '"'
    // splitString({ad cas asdfa}, ' ', {'{','}'}, true)  => {ad,cas,asdfa}
    // splitString("ad "cas\" asdfa"", ' ', {'"','"'}, true) => {ad,cas\" asdfa}
    public static String[] splitString(String initialString, char splitor, char[] quoters, boolean noDuplicatedSplitor){
        if (quoters == null || quoters.length != 2)
            quoters = new char[] {'"','"'};
        String[] splittedString = new String[0];
        int quoteDeep = 0;
        int lastPos = 0;
        for (int i=0;i<initialString.length();i++){
            if (initialString.charAt(i) == quoters[0]){
                quoteDeep++;
                //if (removeQuoter && quoteDeep == 1){ // remove top level quoter
                //    initialString = initialString.substring(0, i)+(i<initialString.length()-1?initialString.substring(i+1):"");
                //    i--;
                //}
            }else if (initialString.charAt(i) == quoters[1]){
                quoteDeep--;
                if  (quoteDeep < 0){
                    System.out.println("");
                    System.out.println("Error: Quoter mismatched!");
                    System.out.println("");
                }
                //if (removeQuoter && quoteDeep == 0){ // remove top level quoter
                //    initialString = initialString.substring(0, i)+(i<initialString.length()-1?initialString.substring(i+1):"");
                //    i--;
                //}
            }else if(initialString.charAt(i) == '\\' && i<initialString.length()-1 && 
                    ((initialString.charAt(i+1) == quoters[0] || initialString.charAt(i+1) == quoters[1] || initialString.charAt(i+1) == splitor))){
                i++; // skip escaped " :\"
                initialString = initialString.substring(0, i-1)+initialString.substring(i);
            }else if(initialString.charAt(i) == splitor && quoteDeep == 0){ // splitor that not between quato are the real splitor
                //String[] oldCommand = splittedString;
                ////arrayCopy(oldCommand,0,splittedString,0,oldCommand.length);
                //String[] subCommand = splitString(trim(initialString.substring(lastPos, i), splitor),splitor,quoters,noDuplicatedSplitor);
                //splittedString = new String[splittedString.length + subCommand.length];
                //arrayCopy(oldCommand,0,splittedString,0,oldCommand.length);
                //arrayCopy(subCommand,0,splittedString,oldCommand.length,subCommand.length);
                ////splittedString[splittedString.length-1] = new String(trim(initialString.substring(lastPos, i), splitor));
                splittedString = (String[])appendToArray(splittedString,splitString(trim(initialString.substring(lastPos, i), splitor),splitor,quoters,noDuplicatedSplitor));
                if (noDuplicatedSplitor)
                    while (i<initialString.length()-1 && initialString.charAt(i+1) == ' ') // skip duplicated splitor
                        i++;
                lastPos = i;
            }
        }

        if (quoteDeep != 0){
            System.out.println("");
            System.out.println("Error: Quoter mismatched!");
            System.out.println("");
        }

        if (lastPos < initialString.length()&& quoteDeep == 0){
            //String[] oldCommand = splittedString;
            if (lastPos == 0){
                //splittedString = new String[splittedString.length + 1];
                //arrayCopy(oldCommand,0,splittedString,0,oldCommand.length);
                //splittedString[splittedString.length-1] = trim(initialString.substring(lastPos), splitor);
                splittedString = (String[])appendToArray(splittedString,new String(trim(initialString.substring(lastPos), splitor)));
            }else{
                //String[] subCommand = splitString(trim(initialString.substring(lastPos), splitor),splitor,quoters,noDuplicatedSplitor);
                //splittedString = new String[splittedString.length + subCommand.length];
                //arrayCopy(oldCommand,0,splittedString,0,oldCommand.length);
                //arrayCopy(subCommand,0,splittedString,oldCommand.length,subCommand.length);
                splittedString = (String[])appendToArray(splittedString,splitString(trim(initialString.substring(lastPos), splitor),splitor,quoters,noDuplicatedSplitor));
            }
        }
        return splittedString;
    }
    
    // format milli seconds to time 
    // 3600000 => 01:00:00.000
    public static String milliSecsToTime(long milliSecs){
        int mSecs = (int)milliSecs%1000;
        milliSecs = milliSecs/1000;
        int secs = (int)milliSecs%60;
        milliSecs = milliSecs/60;
        int mins = (int)milliSecs%60;
        milliSecs = milliSecs/60;
        int hours = (int)milliSecs%60;
        milliSecs = milliSecs/60;
        return formatInt(hours,2,'0')+":"+formatInt(mins,2,'0')+":"+formatInt(secs,2,'0')+"."+formatInt(mSecs,3,'0');
    }
    
    // choose a minimum num 
    public static int min(int...args){
        int minNum = args[0];
        for (int i=1;i<args.length;i++){
            if (args[i]<minNum)
                minNum = args[i];
        }
        return minNum;
    }

    // choose a maximum num 
    public static int max(int...args){
        int maxNum = args[0];
        for (int i=1;i<args.length;i++){
            if (args[i]>maxNum)
                maxNum = args[i];
        }
        return maxNum;
    }

    //e.g. log(100,10) = 2
    public static double log(double value, double base){
        return java.lang.Math.log(value)/Math.log(base);
    }

    // format int, supplement character.  formatInt(1, 3, '0') => "003"
    public static String formatInt(int num, int len, char c){
        int lel = num==0?0:(int)log((double)java.lang.Math.abs(num),(double)10);
        if (lel<len)
            return ""+repeatStr(String.valueOf(c),len-lel-1)+num;
        else
            return ""+num;
    }
    
    public static String trim(String s, char c){
        int start = 0;
        int end = s.length() - 1;
        while (s.charAt(start) == c ) // trim left
            start++;
        while (s.charAt(end) == c ) // trim right
            end--;
        return new String(s.substring(start,end+1));
    }

    public static String ltrim(String s, char c){
        int start = 0;
        while (s.charAt(start) == c ) // trim left
            start++;
        return new String(s.substring(start));
    }

    public static String rtrim(String s, char c){
        int end = s.length() - 1;
        while (s.charAt(end) == c ) // trim right
            end--;
        return new String(s.substring(0,end+1));
    }

    public static String lPad(String s, int n) {
        return String.format("%1$#" + n + "s", s);  
    }

    public static String rPad(String s, int n) {
         return String.format("%1$-" + n + "s", s);  
    }
    
    public static String repeatStr(String str, int repeatTimes){
        String repeatedStr = new String();
        for (int i=0;i<repeatTimes;i++)
            repeatedStr+=str;
        return repeatedStr;
    }

    // convert an int value to byte[]
    public static byte[] intToByteArray(int intValue) {
        return new byte[] {
            (byte)(intValue >>> 24),
            (byte)(intValue >>> 16),
            (byte)(intValue >>> 8),
            (byte)intValue};
    }

    // convert byte[] to int
    public static int byteArrayToInt(byte[] bytes) {
        if (bytes.length != 4)
            return 0;
        int intVal = 0; 
        int tmpInt = 0;
        for(int i=0;i<4;i++){  
            intVal<<=8;  
            tmpInt=bytes[i]&0xFF;  
            intVal|=tmpInt;  
        }  
        return intVal;
    }

    public static byte[] longToByteArray(long longVal) {
        return new byte[] {
            (byte) (longVal >> 56),
            (byte) (longVal >> 48),
            (byte) (longVal >> 40),
            (byte) (longVal >> 32),
            (byte) (longVal >> 24),
            (byte) (longVal >> 16),
            (byte) (longVal >> 8),
            (byte) (longVal >> 0)
        };
    }

    public static long byteArrayToLong(byte[] bytes) {
        if (bytes.length != 8)
            return 0;
        return ((((long) bytes[0] & 0xff) << 56)
                | (((long) bytes[1] & 0xff) << 48)
                | (((long) bytes[2] & 0xff) << 40)
                | (((long) bytes[3] & 0xff) << 32)
                | (((long) bytes[4] & 0xff) << 24)
                | (((long) bytes[5] & 0xff) << 16)
                | (((long) bytes[6] & 0xff) << 8) 
                | (((long) bytes[7] & 0xff) << 0));
    } 

    public static byte[] charToByteArray(char c){
        byte[] bytes=new byte[2];
        int tmp=(int)c;
        for (int i=bytes.length-1;i>=0;i--){
            bytes[i] = new Integer(tmp&0xff).byteValue();
            tmp = tmp >> 8;
        }
        return bytes;
    }

    public static char byteArrayToChar(byte[] bytes){
        int i=0;
        if(bytes[0]>0)
            i+=bytes[0];
        else
            i+=256+bytes[0];
        i*=256;
        if(bytes[1]>0)
            i+=bytes[1];
        else
            i+=256+bytes[1];
        char c=(char)i;
        return c;
    }

    public static byte[] doubleToByteArray(double d){
        return longToByteArray(Double.doubleToLongBits(d));
    }

    public static double byteArrayToDouble(byte[] bytes){
        return Double.longBitsToDouble(byteArrayToLong(bytes));
    }
    
    public static void arrayCopy(Object src, int srcPos, Object dest, int destPos, int length){
        System.arraycopy(src,srcPos,dest,destPos,length);
    }
    
    // read 4 bytes from block and convert to int
    public static int readIntFromBlock(byte[] block, int offset){
        byte[] blockType = new byte[4];
        arrayCopy(block,offset,blockType,0,4);
        return CommUtility.byteArrayToInt(blockType);
    }

    // read 8 bytes from block and convert to long
    public static long readLongFromBlock(byte[] block, int offset){
        byte[] blockType = new byte[8];
        arrayCopy(block,offset,blockType,0,8);
        return CommUtility.byteArrayToLong(blockType);
    }
    
    // read some bytes from block and convert to string
    public static String readStringFromBlock(byte[] block, int offset, int len, String encoding){
        try{
            return new String(block,offset,len,encoding);
        }catch(UnsupportedEncodingException e){
            return null;
        }
    }

    // read some bytes from block and convert to string
    public static String readStringFromBlock(byte[] block, int offset, int len){
        return new String(block,offset,len);
    }

    // get the absolute number
    public static int abs(int origNum){
        return origNum<0?-origNum:origNum;    
    }
    
    // generate a defferent hash number base on hashBase
    public static int multiHashNum(int origNum, int hashBase){
        return abs((origNum<<hashBase)^(origNum>>(hashBase/2)));
    }

    // compare data according to data type
    // @return int str1 < str2: -1; str1 == str2:0; str1 > str2: 1
    //             error -101~-110 -101:invalid data according to data type; -102: data type not supported
    public static int anyDataCompare(String str1, String str2, int type){
        if (str1 == null && str2 == null)
            return 0;
        else if (str1 == null)
            return -1;
        else if (str2 == null)
            return 1;
        if (type == Consts.LONG){
            try{
                long d1 = Long.parseLong(str1);
                long d2 = Long.parseLong(str2);
                if (d1 < d2)
                    return -1;
                else if (d1 == d2)
                    return 0;
                else
                    return 1;
            }catch (NumberFormatException e){
                return -101;
            }
        }else if (type == Consts.INTEGER){
            try{
                int d1 = Integer.parseInt(str1);
                int d2 = Integer.parseInt(str2);
                if (d1 < d2)
                    return -1;
                else if (d1 == d2)
                    return 0;
                else
                    return 1;
            }catch (NumberFormatException e){
                return -101;
            }
        }else if (type == Consts.DOUBLE){
            try{
                double d1 = Double.parseDouble(str1);
                double d2 = Double.parseDouble(str2);
                if (d1 < d2)
                    return -1;
                else if (d1 == d2)
                    return 0;
                else
                    return 1;
            }catch (NumberFormatException e){
                return -101;
            }
        }else if (type == Consts.DATE || type == Consts.TIMESTAMP){
            try{
                Timestamp d1 = Timestamp.valueOf(str1);
                Timestamp d2 = Timestamp.valueOf(str2);
                return d1.compareTo(d2);
            }catch (IllegalArgumentException e){
                return -101;
            }
        }else if (type == Consts.BOOLEAN){
            try{
                // convert boolean to int to compare. false => 0; true => 1
                int d1 = Boolean.parseBoolean(str1)?1:0;
                int d2 = Boolean.parseBoolean(str2)?1:0;
                if (d1 < d2)
                    return -1;
                else if (d1 == d2)
                    return 0;
                else
                    return 1;
            }catch (Exception e){
                e.printStackTrace();
                return -101;
            }
        }else if (type == Consts.STRING){
            return str1.compareTo(str2);
        }else {
            return -102;
        }
    }
    
    // compare data according to data type
    // @return int 0: false; 1: true  
    //             error -101~-110 -101:invalid data according to data type; -102: data type not supported
    public static int anyDataCompare(String str1, int comparator, String str2, int type){
        if (type == Consts.LONG){
            try{
                long d1 = Long.parseLong(str1);
                long d2 = Long.parseLong(str2);
                switch (comparator){
                case Consts.EQ:
                   return d1 == d2?1:0;
                case Consts.LT:
                   return d1 > d2?1:0;
                case Consts.ST:
                   return d1 < d2?1:0;
                case Consts.NEQ:
                   return d1 != d2?1:0;
                case Consts.LE:
                   return d1 >= d2?1:0;
                case Consts.SE:
                   return d1 <= d2?1:0;
                }
            }catch (NumberFormatException e){
                return -101;
            }
        }else if (type == Consts.DOUBLE){
            try{
                double d1 = Double.parseDouble(str1);
                double d2 = Double.parseDouble(str2);
                switch (comparator){
                case Consts.EQ:
                   return d1 == d2?1:0;
                case Consts.LT:
                   return d1 > d2?1:0;
                case Consts.ST:
                   return d1 < d2?1:0;
                case Consts.NEQ:
                   return d1 != d2?1:0;
                case Consts.LE:
                   return d1 >= d2?1:0;
                case Consts.SE:
                   return d1 <= d2?1:0;
                }
            }catch (NumberFormatException e){
                return -101;
            }
        }else if (type == Consts.DATE || type == Consts.TIMESTAMP){
            try{
                Timestamp d1 = Timestamp.valueOf(str1);
                Timestamp d2 = Timestamp.valueOf(str2);
                switch (comparator){
                case Consts.EQ:
                   return d1.equals(d2)?1:0;
                case Consts.LT:
                   return d1.compareTo(d2)>0?1:0;
                case Consts.ST:
                   return d1.compareTo(d2)<0?1:0;
                case Consts.NEQ:
                   return !d1.equals(d2)?1:0;
                case Consts.LE:
                   return d1.compareTo(d2)>=0?1:0;
                case Consts.SE:
                   return d1.compareTo(d2)<=0?1:0;
                }
            }catch (IllegalArgumentException e){
                return -101;
            }
        }else if (type == Consts.BOOLEAN){
            try{
                // convert boolean to int to compare. false => 0; true => 1
                int d1 = Boolean.parseBoolean(str1)?1:0;
                int d2 = Boolean.parseBoolean(str2)?1:0;
                switch (comparator){
                case Consts.EQ:
                   return d1 == d2?1:0;
                case Consts.LT:
                   return d1 > d2?1:0;
                case Consts.ST:
                   return d1 < d2?1:0;
                case Consts.NEQ:
                   return d1 != d2?1:0;
                case Consts.LE:
                   return d1 >= d2?1:0;
                case Consts.SE:
                   return d1 <= d2?1:0;
                }
            }catch (Exception e){
                e.printStackTrace();
                return -101;
            }
        }else if (type == Consts.STRING){
            switch (comparator){
            case Consts.EQ:
               return str1.equals(str2)?1:0;
            case Consts.LT:
               return str1.compareTo(str2)>0?1:0;
            case Consts.ST:
               return str1.compareTo(str2)<0?1:0;
            case Consts.NEQ:
               return !str1.equals(str2)?1:0;
            case Consts.LE:
               return str1.compareTo(str2)>=0?1:0;
            case Consts.SE:
               return str1.compareTo(str2)<=0?1:0;
            }
        }else {
            return -102;
        }
        return -102;
    }
    
    // get the successor of a data according to its data type
    public static String successor(String str, int type){
        String newStr = new String(str);
        switch (type){
        case Consts.LONG:
            try{
                long d = Long.parseLong(str) + 1;
                newStr = String.valueOf(d);
            }catch (NumberFormatException e){
            }
            break;
        case Consts.DOUBLE:
            try{
                double d = Double.parseDouble(str);
                d = Double.longBitsToDouble(Double.doubleToLongBits(d)+1);
                newStr = String.valueOf(d);
            }catch (NumberFormatException e){
            }
            break;
        case Consts.DATE:
        case Consts.TIMESTAMP:
            try{
                Timestamp d = new Timestamp((Timestamp.valueOf(str)).getTime()+1);
                newStr = d.toString();
            }catch (IllegalArgumentException e){
            }
            break;
        //case Consts.BOOLEAN:
        //    return newStr;
        case Consts.STRING:
            newStr += "\0";
            break;
        }
        return newStr;
    }

    // conver ArrayList to map
    public static HashMap arrayDataToMap(ArrayList keys, ArrayList values){
        HashMap mapData = new HashMap();
        if (keys == null || values == null || keys.size()!=values.size())
            return mapData;
        ArrayList newKeys = new ArrayList(keys);
        ArrayList newValues = new ArrayList(values);
        for (int i=0;i<values.size();i++){
            mapData.put(newKeys.get(i), newValues.get(i));
        }
        return mapData;
    }

    // get hash code of a file
    // hashType could  be MD5, SHA1, SHA-256, SHA-384, SHA-512
    public static String getHash(String fileName, String hashType) throws Exception {  
        InputStream fis = new FileInputStream(fileName);  
        byte[] buffer = new byte[1024];  
        MessageDigest md5 = MessageDigest.getInstance(hashType);  
        int numRead = 0;  
        while ((numRead = fis.read(buffer)) > 0) {  
            md5.update(buffer, 0, numRead);  
        }  
        fis.close();  
        return toHexString(md5.digest());  
    }  

    // conver bytes array to HEX string
    public static String toHexString(byte[] b) {  
        StringBuilder sb = new StringBuilder(b.length * 2);  
        for (int i = 0; i < b.length; i++) {  
            sb.append(hexChars[(b[i] & 0xf0) >>> 4]);  
            sb.append(hexChars[b[i] & 0x0f]);  
        }  
        return sb.toString();  
    }
    
    // merge 2 arrays to an array
    public static <T> void mergeArrays(T[] fullArray, T[] array1, T[] array2){
        if (array1 == null && array2 == null)
            fullArray = null;
        else if (array1 == null)
            arrayCopy(array2,0,fullArray,0,array2.length);
        else if (array2 == null)
            arrayCopy(array1,0,fullArray,0,array1.length);
        else{
            arrayCopy(array1,0,fullArray,0,array1.length);
            arrayCopy(array2,0,fullArray,array1.length,array2.length);
        }
    }

    // add a array to array
    public static <T> T[] appendToArray(T[] array, T[] newArray){
        try{
            final T[] result = (T[])Array.newInstance(array.getClass().getComponentType(), array.length + newArray.length);
            mergeArrays(result,array,newArray);
            //arrayCopy(array,0,result,0,array.length);
            //arrayCopy(newArray,0,result,result.length,newArray.length);
            return result;
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }

    // add a member to array
    public static <T> T[] appendToArray(T[] array, T newMember){
        try{
            final T[] result = (T[])Array.newInstance(array.getClass().getComponentType(), array.length + 1);
            arrayCopy(array,0,result,0,array.length);
            result[result.length-1] = newMember;
            return result;
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }
    
    // add a array to array
    public static byte[] appendToArrayB(byte[] array, byte[] newArray){
        byte[] oldArray = array;
        array = new byte[array.length + newArray.length];
        arrayCopy(oldArray,0,array,0,oldArray.length);
        arrayCopy(newArray,0,array,oldArray.length,newArray.length);
        return array;
    }

    // add a member to array
    public static byte[] appendToArrayB(byte[] array, byte newMember){
        byte[] oldArray = array;
        array = new byte[array.length + 1];
        arrayCopy(oldArray,0,array,0,oldArray.length);
        array[array.length-1] = newMember;
        return array;
    }
    
    // retrieve extention from file name
    public static String getExtFromFileName(String fileName){
        int extPos = fileName.lastIndexOf('.');

        if ((extPos >-1) && (extPos < (fileName.length() - 1))) {
            return fileName.substring(extPos + 1);
        }else
            return "";
    }

    // get files under a foder
    public static File[] getFiles (String dirStr, String startStr, String ext) throws IOException {
        File dir = new File(dirStr);
        File[] files = new File[0];
        if(dir == null)
            return null;
        else if(!dir.exists())
            dir.mkdirs();
        if(dir.isDirectory()){
            File[] f = dir.listFiles();
            for(int i=0;i<f.length;i++){
                if (f[i].getName().toUpperCase().startsWith(startStr.toUpperCase()) && 
                    getExtFromFileName(f[i].getName()).toUpperCase().equals(ext==null?null:ext.toUpperCase()))
                    files = (File[])appendToArray(files, new File(f[i].getCanonicalPath()));
            }
        }else {
            return null;
        }
        return files;
    }
    
    // get local address by ip string
    public static InetAddress getLocalAddrByIP(String ipAddress){
        InetAddress tmpAddr;
        try{
            tmpAddr = InetAddress.getByName(ipAddress);
            for (InetAddress localAddr:InetAddress.getAllByName(InetAddress.getLocalHost().getHostName())){
                if (tmpAddr.equals(localAddr))
                    return tmpAddr;
            }
            tmpAddr = InetAddress.getLocalHost();
        }catch(UnknownHostException e){
            try{
                tmpAddr = InetAddress.getLocalHost();
            }catch(UnknownHostException e1){
                tmpAddr = null;
            }
        }
        return tmpAddr;
    }
    
    public static HashMap subMap(HashMap allValues, HashSet subKeys){
        HashMap partValues = new HashMap();
        if (allValues == null || subKeys == null)
            return partValues;
        Iterator it = allValues.keySet().iterator();
        while (it.hasNext()){
            Object key = it.next();
            if (subKeys.contains(key))
                partValues.put(key,allValues.get(key));
        }
        return partValues;
    }

    // merge 2 data entry, the items in 2nd one will replace items in the 1st one; and new ones in 2nd one will insert into 1st one
    public static HashMap mergeValues(HashMap value1, HashMap value2){
        HashMap value = value1;
        if (value2 == null)
            return value1;
        Iterator it = value2.keySet().iterator();
        while (it.hasNext()){
            Integer colID = (Integer)it.next();
            value.remove(colID);
            String strVal = (String)value2.get(colID);
            if (strVal != null)
                value.put(colID,strVal);
        }
        return value;
    }
    
    // copy file
    public static boolean copyFile(String oldFileName, String newFileName, boolean overWrite){
        File oldF = new File(oldFileName);
        if (!oldF.exists())
            return false;
        File newF = new File(newFileName);
        if (newF.exists())
            if (overWrite)
                newF.delete();
            else
                return false;
        try{
            FileInputStream in = new FileInputStream(oldFileName);
            FileOutputStream out=  new FileOutputStream(newFileName);
            byte[] buf=new byte[8192];
            while(in.read(buf)>0)
                out.write(buf);
            in.close();
            out.close();
        }catch(IOException e){
            return false;
        }
        return true;
    }
    
    // get size of String arraylist
    public static long sizeOf(ArrayList<String> list){
        long totalSize = 0;
        if (list != null)
            for (int i=0;i<list.size();i++){
                String str = (String)list.get(i);
                totalSize += str==null?0:str.getBytes().length;
            }
        return totalSize;
    }

    // get size of Integer:String HashMap
    public static long sizeOf(HashMap<Integer, String> map){
        long totalSize = 0;
        if (map != null){
            Iterator it = map.values().iterator();
            while (it.hasNext()){
                totalSize += 4; // for Integer key
                String str = (String)it.next();
                totalSize += str==null?0:str.getBytes().length;
            }
        }
        return totalSize;
    }
}
