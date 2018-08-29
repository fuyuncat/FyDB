/*
 * @(#)Debuger.java	0.01 11/04/26
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_comm;

import java.text.SimpleDateFormat;

public class Debuger {
    private static boolean isDebugMode = true;

    public Debuger() {
    }

    public static void printMsg(String msg, boolean withTime){
        if (withTime){
            SimpleDateFormat tempDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
            String datetime = tempDate.format(new java.util.Date());
            System.out.print(datetime+": ");
        }
        System.out.println(msg);
    }
    
    public static boolean isDebugMode(){
        return isDebugMode;
    }

    public static void setDebugMode(boolean newMode){
        isDebugMode = newMode;
    }

    public static String getCodeLine(){
        StackTraceElement ste = new Throwable().getStackTrace()[1];
        return ste.getFileName() + " (" + ste.getLineNumber()+")";
    }

    public static String getMethodName(){
        StackTraceElement ste = new Throwable().getStackTrace()[1];
        return ste.getMethodName();
    }

    public static void dumpStackTrace(){
        StackTraceElement stes[] = new Throwable().getStackTrace();
        for (int i=0; i<stes.length; i++)
            System.out.println(stes[i].getFileName() + " (" + stes[i].getLineNumber()+")");
    }

    public static void fakeException(String msg){
        Exception ex = new Exception(msg);
        ex.printStackTrace();
    }
}
