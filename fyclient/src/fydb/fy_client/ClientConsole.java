/**
 * @(#)ClientConsole.java	0.01 11/05/18
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_client;

import fydb.fy_comm.CommUtility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.HashMap;
import java.util.regex.Pattern;

public class ClientConsole {
    private SessionLeader sessionLeader;
    private SessionClient session;
    private HashMap abbreviations;

    public ClientConsole() {
        sessionLeader = new SessionLeader(1);
        abbreviations = new HashMap();
        abbreviations.put("H","help");
        abbreviations.put("V","version");
        abbreviations.put("CONN","Connect");
        abbreviations.put("DISC","Disconnect");
        abbreviations.put("E","Exit");
    }

    public static void main(String[] args) {
        ClientConsole clientConsole = new ClientConsole();
        clientConsole.commandLine();
    }

    // get input command
    private String getInputCommand(){
        String strInputString = new String();
        try{
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in),1); 
            strInputString = br.readLine();
        }catch (IOException e){
            System.out.println("failed!");
            e.printStackTrace();
        }

        return strInputString;
    }

    // split input command line into pieces
    // space quoted in "" or {} should be ignored. \" is character '"'
    // ad cas asdfa => {ad,cas,asdfa}
    // ad "cas\" asdfa" => {ad,cas\" asdfa}
    private String[] splitCommand(String initialCommand)
    {
        //return CommUtility.splitString(initialCommand, ' ', true);
        return CommUtility.splitString(initialCommand, ' ', new char[] {'{','}'}, true);

        /*
        Pattern pSplit = Pattern.compile("[ ]+");
        String[] splittedCommand = new String[0];
        int quotePos = initialCommand.indexOf("\"");
        int lastPos = 0;
        boolean quoteStarted = false;
        while (quotePos >=0 && quotePos < initialCommand.length() ){
            if (quotePos > 0){
                if (initialCommand.charAt(quotePos-1) != '\\') {// escape char
                    quoteStarted = !quoteStarted;
                }else{
                    // eliminate the escap char
                    initialCommand = initialCommand.substring(0,quotePos-1)+initialCommand.substring(quotePos);
                    quotePos = initialCommand.indexOf("\"",quotePos+1);
                    continue;
                }
            }else{
                quoteStarted = true;
            }
            //if (quotePos<0)
            //    quotePos = initialCommand.length();
            if (quoteStarted){
                String[] tmpCommand = pSplit.split(initialCommand.substring(lastPos, quotePos-1));
                if (splittedCommand.length == 0)
                    splittedCommand = tmpCommand;
                else{
                    //String[] oldCommand = new String[splittedCommand.length];
                    //System.arraycopy(splittedCommand,0,oldCommand,0,splittedCommand.length);
                    String[] oldCommand = splittedCommand;
                    splittedCommand = new String[splittedCommand.length + tmpCommand.length];
                    System.arraycopy(oldCommand,0,splittedCommand,0,oldCommand.length);
                    System.arraycopy(tmpCommand,0,splittedCommand,oldCommand.length,tmpCommand.length);
                }
            }else{ // add the string between quotas "" as an element of the array
                if (splittedCommand.length == 0){
                    splittedCommand = new String[1];
                    splittedCommand[0] = initialCommand.substring(lastPos+1, quotePos-1).trim();
                }
                else{
                    //String[] oldCommand = new String[splittedCommand.length];
                    //System.arraycopy(splittedCommand,0,oldCommand,0,splittedCommand.length);
                    String[] oldCommand = splittedCommand;
                    splittedCommand = new String[splittedCommand.length + 1];
                    System.arraycopy(oldCommand,0,splittedCommand,0,oldCommand.length);
                    splittedCommand[splittedCommand.length-1] = initialCommand.substring(lastPos+1, quotePos-1).trim();
                }
            }
            lastPos = quotePos;
            quotePos = initialCommand.indexOf("\"",quotePos+1);
        }
        if (lastPos < initialCommand.length() - 1 && !quoteStarted){
            String[] tmpCommand = pSplit.split(initialCommand.substring(lastPos));
            //String[] oldCommand = new String[splittedCommand.length];
            //System.arraycopy(splittedCommand,0,oldCommand,0,splittedCommand.length);
            String[] oldCommand = splittedCommand;
            splittedCommand = new String[splittedCommand.length + tmpCommand.length];
            System.arraycopy(oldCommand,0,splittedCommand,0,oldCommand.length);
            System.arraycopy(tmpCommand,0,splittedCommand,oldCommand.length,tmpCommand.length);
        }
        //for (int i=0; i<splittedCommand.length; i++)
        //    splittedCommand[i] = splittedCommand[i].replace('|', ' ');
        return splittedCommand;//*/
    }

     // show syntax error
     private static String syntaxError(String formatString, String rightWords, String wrongWord){
         return String.format(formatString, wrongWord, rightWords);
     }
     
    // parse yes or no
    private boolean parseYesOrNo(){
      boolean commandCorrect = false;
      while (!commandCorrect)
      {
        String inputString = getInputCommand();
        if (inputString == null || 
            inputString.equalsIgnoreCase("") ||
            inputString.equalsIgnoreCase("No") ||
            inputString.equalsIgnoreCase("N"))
        {
          commandCorrect = true;
          return false;
        }

        if (inputString.equalsIgnoreCase("Yes") ||
            inputString.equalsIgnoreCase("Y"))
        {
          commandCorrect = true;
          return true;
        }
        
        System.out.println("");
        System.out.print("Please input Y(es) or N(o)! >");
      }
      
      return false;
    }
    
    // disconnect from server
    private void disConnect(){
        if (session != null){
            session.release();
            sessionLeader.dellocateSession(session.getSessionID());
            session = null;
        }
    } 

    // parse command
    private boolean parseCMD(String initialCommand)
    {
        String[] commandString = splitCommand(initialCommand.trim());
        if (commandString == null || commandString.length ==0 ||commandString[0] == null ||commandString[0].equalsIgnoreCase(""))
            return false;
        
        if (abbreviations.containsKey(commandString[0].toUpperCase())) // convert abbreviation to real command
            commandString[0] = (String)abbreviations.get(commandString[0].toUpperCase());
        
        // ^_^
        if (initialCommand.equalsIgnoreCase("I Love fuyuncat")){
            System.out.println("");
            System.out.println("Fuyuncat love you, too!");
            System.out.println("");

            return true;
        }
        
        // exit process
        if (commandString.length==1 &&
          (commandString[0].equalsIgnoreCase("Exit"))){
            System.out.println("");
            System.out.print("Are you sure you want to exit console? (Yes or No) >");
            if (parseYesOrNo()){
                goodBye();
                System.exit(0);
            }
            System.out.println("");
            
            return true;
        }
        
        if (commandString.length==4 &&
          (commandString[0].equalsIgnoreCase("connect"))){
            disConnect();
            System.out.println("Connecting to server "+commandString[1]+" ...");
            if (sessionLeader.connectToServer(commandString[1], Integer.valueOf(commandString[2]), commandString[3],false)){
            session = sessionLeader.allocateSession();
                System.out.println("");
                System.out.println("Connected to server successfully!");
                System.out.println("");
                return true;
            }else{
                System.out.println("");
                System.out.println("Connected to server failed!");
                System.out.println("");
                return false;
            }
        }

        if (commandString.length==1 &&
          (commandString[0].equalsIgnoreCase("disconnect"))){
            if (session == null){
                System.out.println("");
                System.out.println("No FyDB server connected.");
                System.out.println("");
                return false;
            }else{
                disConnect();
                System.out.println("");
                System.out.println("Disconnected from server.");
                System.out.println("");
                return true;
            }
        }

        CommandExecutor cmdExecutor = new CommandExecutor();
        if (cmdExecutor.commands.containsKey(commandString[0].toUpperCase())){
            if (cmdExecutor.parseCommand(commandString))
                return cmdExecutor.runCommand(commandString, session);
            else
                return false;
        }

        System.out.println("");
        System.out.println(initialCommand + " is not a valid command. Please type HELP to get help inform.");
        System.out.println("");
        
        return false;
    }

    // release resource, close console
    private void goodBye(){
        disConnect();
    }

    // show command line console, recieve input command
    private void commandLine(){
        System.out.print("FyDB>");
        //parseCMD("set @timing on");
        //parseCMD("set @time on");
        //parseCMD("version @server");
        //parseCMD("conn huanged 6636 fuyuncat@gmail.com");
        //parseCMD("conn 146.222.94.152 6636 fuyuncat@gmail.com");
        //parseCMD("conn huanged2 6636 fuyuncat@gmail.com");
        //parseCMD("add {OWNER:\"DEMO\";TABLE_NAME:\"WWW\"} TO DB_Oracle/1712582900/DEMO/T_TEST2");
        //parseCMD("modify {OWNER:\"DEMO\";TABLE_NAME:\"WWW1\"} in DB_Oracle/1712582900/DEMO/T_TEST2 with {owner=\"DEMO\" and table_name =\"WWW\"}");
        //parseCMD("get  @value from JDBC:ORACLE:THIN:@LOCALHOST:1521:EDGAR/DEMO/T_TEST2 with {LAST_ANALYZED = \"2005-12-23 22:00:09.0\" and table_name > \"T_\" and owner = \"RING\"}");
        //parseCMD("get  @value from JDBC:ORACLE:THIN:@LOCALHOST:1521:EDGAR/DEMO/T_TEST2 with {LAST_ANALYZED < \"2006-9-22 22:00:09.0\"}");
        //parseCMD("get  @value from JDBC:ORACLE:THIN:@LOCALHOST:1521:EDGAR/DEMO/T_TEST2 with {LAST_ANALYZED <= \"2006-9-22 22:00:09.0\"}");
        //parseCMD("get  @value from JDBC:ORACLE:THIN:@LOCALHOST:1521:EDGAR/DEMO/T_TEST2 with {LAST_ANALYZED > \"2006-9-22 22:00:09.0\"}");
        //parseCMD("get  @value from JDBC:ORACLE:THIN:@LOCALHOST:1521:EDGAR/DEMO/T_TEST2 with {LAST_ANALYZED >= \"2006-9-22 22:00:09.0\"}");
        //parseCMD("get @value from DB_Oracle/1712582900/DEMO/T_TEST2");
        //parseCMD("get @value from DB_Oracle/1712582900/DEMO/T_TEST2 with {owner=\"DEMO\" and tablespace_name =\"RING\"}");
        //parseCMD("get @value from DB_Oracle/1712582900/DEMO/T_TEST2 with {LAST_ANALYZED=\"2007-03-15 22:00:15\" and TABLE_Name >= \"M7B\" and tablespace_name = \"EDGARDEMO\"}");
        //parseCMD("join @value from DB_Oracle/1712582900/DEMO/T_TEST2 with {LAST_ANALYZED=\"2007-03-15 22:00:15\" and TABLE_Name >= \"M7B\" and tablespace_name = \"EDGARDEMO\"} T_TEST2 with {LAST_ANALYZED=\"2007-03-15 22:00:15\" and TABLE_Name >= \"M7B\" and tablespace_name = \"EDGARDEMO\"} on {OWNER=OWNER and table_name=table_name}");
        //parseCMD("join @value from DB_Oracle/1712582900/DEMO/T_TEST2 with {tablespace_name = \"EDGARDEMO\"} T_TEST2 with {LAST_ANALYZED=\"2007-03-15 22:00:15\" and TABLE_Name >= \"M7B\" and tablespace_name = \"EDGARDEMO\"} outon {OWNER=OWNER and table_name=table_name}");
        //parseCMD("join @value from DB_Oracle/1712582900/DEMO/T_TEST2 with {owner = \"DEMO\" and tablespace_name = \"EDGARDEMO\"} t_test2 with {owner = \"DEMO\" and table_name > \"T_T\"} on {OWNER=OWNER and table_name=table_name}");
        //parseCMD("remove from DB_Oracle/1712582900/DEMO/T_TEST2 with {LAST_ANALYZED=\"2007-03-15 22:00:15\" and TABLE_Name >= \"M7B\" and tablespace_name = \"EDGARDEMO\"}");
        //parseCMD("get @value from DB_Oracle/1712582900/DEMO/T_TEST2 with @key is 0:DEMO;1:T_TEST1");
        //parseCMD("get @value   from asd     with @key is ad\\ a:\"d   a\\;asad\";adae:\"da \\\"d\\:as\"\\ \"comments");
        while (true){
            parseCMD(getInputCommand());
            System.out.print("FyDB>");
        }
    }
}
