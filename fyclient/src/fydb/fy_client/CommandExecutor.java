/**
 * @(#)CommandExecutor.java	0.01 11/05/20
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */

package fydb.fy_client;

import fydb.fy_comm.CommUtility;
import fydb.fy_comm.Consts;
import fydb.fy_comm.FyMetaData;
import fydb.fy_comm.Prediction;
import fydb.fy_comm.StatementParser;
import fydb.fy_comm.FyDataEntry;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public final class CommandExecutor {
    private static boolean showTiming = false;     // if show time elapsed
    private static boolean showCurTime = false;    // if show current time;

    private final String prodName = "FyDB Console";
    private final String version = "Alpha 0.01";
    private final String copyRight = "Copyright @2011 Fuyuncat. All rights reserved."+System.getProperty("line.separator")+
                                     "FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms."+System.getProperty("line.separator")+
                                     "Email: fuyucat@gmail.com"+System.getProperty("line.separator")+
                                     "WebSite: www.HelloDBA.com"+System.getProperty("line.separator");
    /*
     * Example: get @value from <datasetName> with @key is (aaa,bbb) and colName is ()
     * 
        1:[Get] => 2
        2:[*][<colName1>,<colName2>...][<colID1>,<colID2>...] => 3
        3:[from] => 4
        4:[<datasetName>] canBeEnd => 5
        5:[with] => 6
        6:[@key][<colName>][<colID>] => 7
        7:[is][lt][gt][ltis][gtis] => 8
        8:(<data>) canBeEnd => 9
        9:[OR/AND] => 6
     * */
    public class CommandElement{
        public int currentLevel;
        public int nextLevel;
        public String[] candidateWords; 
        public boolean canBeEnd;

        public CommandElement(int currentLevel, int nextLevel, String[] candidateWords, boolean canBeEnd){
            this.currentLevel = currentLevel;
            this.nextLevel = nextLevel;
            this.candidateWords = candidateWords; // no need System.arraycopy
            this.canBeEnd = canBeEnd;
        }
        // detect if word is a candidate word
        public boolean containsWord(String word){
            for (int i=0;i<candidateWords.length;i++){
                if (candidateWords[i].equalsIgnoreCase(word) || candidateWords[i].startsWith("$")) // $word means any valid word
                    return true;
            }
            return false;
        }
        // concat all candidate words
        public String concatWords(){
            String wordList = candidateWords[0];
            for (int i=1;i<candidateWords.length;i++){
                wordList+="|"+candidateWords[i];
            }
            return wordList;
        }
    }
    
    public class Command{
        public ArrayList commandTree;
        public String description;
        public boolean isServerCommand;
        
        public Command(ArrayList commandTree, String description, boolean isServerCommand){
            this.commandTree = commandTree;
            this.description = description;
            this.isServerCommand = isServerCommand;
        }
    }

    public static HashMap commands = new HashMap();
  
    public CommandExecutor(){
        // build LIST command
        ArrayList commandTree = new ArrayList();
        String[] candidateWords = new String[]{"list"};
        CommandElement cmdElement = new CommandElement(0,1,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"@datasets", "@commands"};
        cmdElement = new CommandElement(1,-1,candidateWords,true);
        commandTree.add(cmdElement);
        Command command = new Command(commandTree, "list commands or datasets in server.", true);
        // key should be uppercase word
        commands.put(new String("LIST"), command); 

        // build GET command
        commandTree = new ArrayList();
        candidateWords = new String[]{"get"};
        cmdElement = new CommandElement(0,1,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"@value","@metadata"};
        cmdElement = new CommandElement(1,2,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"from"};
        cmdElement = new CommandElement(2,3,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$DatasetName"};
        cmdElement = new CommandElement(3,4,candidateWords,true);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"with"};
        cmdElement = new CommandElement(4,5,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$Criteria ({colName1=data1 and colName2>data2...})"};
        cmdElement = new CommandElement(5,-1,candidateWords,true);
        //candidateWords = new String[]{"@key"};
        //cmdElement = new CommandElement(5,6,candidateWords,false);
        //commandTree.add(cmdElement);
        //candidateWords = new String[]{"is"};
        //cmdElement = new CommandElement(6,7,candidateWords,false);
        //commandTree.add(cmdElement);
        //candidateWords = new String[]{"$KeyValue (colName1:data1;colName2:data2...)"};
        //cmdElement = new CommandElement(7,-1,candidateWords,true);
        commandTree.add(cmdElement);
        command = new Command(commandTree, "get data value from dataset, with optional query criteria", true);
        // key should be uppercase word
        commands.put(new String("GET"), command); 

        // build JOIN command
        commandTree = new ArrayList();
        candidateWords = new String[]{"join"};
        cmdElement = new CommandElement(0,1,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"@value"};
        cmdElement = new CommandElement(1,2,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"from"};
        cmdElement = new CommandElement(2,3,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$DatasetName1"};
        cmdElement = new CommandElement(3,4,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"with"};
        cmdElement = new CommandElement(4,5,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$searchCriteria1 ({colName1=data1 and colName2>data2...})"};
        cmdElement = new CommandElement(5,6,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$DatasetName2"};
        cmdElement = new CommandElement(6,7,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"with"};
        cmdElement = new CommandElement(7,8,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$searchCriteria2 ({colName1=data1 and colName2>data2...})"};
        cmdElement = new CommandElement(8,9,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"ON","LEFTON","RIGHTON","OUTON"};
        cmdElement = new CommandElement(9,10,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$joinCrriteria ({colName1_1=colName2_1 and colName1_2>colName2_2...})"};
        cmdElement = new CommandElement(10,-1,candidateWords,true);
        commandTree.add(cmdElement);
        command = new Command(commandTree, "join data value from 2 datasets, query criteria", true);
        // key should be uppercase word
        commands.put(new String("JOIN"), command); 

        // build REMOVE command
        commandTree = new ArrayList();
        candidateWords = new String[]{"remove"};
        cmdElement = new CommandElement(0,1,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"from"};
        cmdElement = new CommandElement(1,2,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$DatasetName"};
        cmdElement = new CommandElement(2,3,candidateWords,true);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"with"};
        cmdElement = new CommandElement(3,4,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$Criteria ({colName1=data1 and colName2>data2...})"};
        cmdElement = new CommandElement(4,-1,candidateWords,true);
        commandTree.add(cmdElement);
        command = new Command(commandTree, "remove data from dataset, with optional query criteria", true);
        // key should be uppercase word
        commands.put(new String("REMOVE"), command); 

        // build ADD command
        commandTree = new ArrayList();
        candidateWords = new String[]{"add"};
        cmdElement = new CommandElement(0,1,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$newDatas ({colName1:data1;colName2:data2...})"};
        cmdElement = new CommandElement(1,2,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"to"};
        cmdElement = new CommandElement(2,3,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$DatasetName"};
        cmdElement = new CommandElement(3,-1,candidateWords,true);
        commandTree.add(cmdElement);
        command = new Command(commandTree, "add data into dataset", true);
        // key should be uppercase word
        commands.put(new String("ADD"), command); 

        // build MODIFY command
        commandTree = new ArrayList();
        candidateWords = new String[]{"modify"};
        cmdElement = new CommandElement(0,1,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$newDatas ({colName1:data1;colName2:data2...})"};
        cmdElement = new CommandElement(1,2,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"in"};
        cmdElement = new CommandElement(2,3,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$DatasetName"};
        cmdElement = new CommandElement(3,4,candidateWords,true);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"with"};
        cmdElement = new CommandElement(4,5,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$Criteria ({colName1=data1 and colName2>data2...})"};
        cmdElement = new CommandElement(5,-1,candidateWords,true);
        commandTree.add(cmdElement);
        command = new Command(commandTree, "modiy data of dataset, with optional query criteria", true);
        // key should be uppercase word
        commands.put(new String("MODIFY"), command); 

        // build Coonect command. It's a fake command here. just used to show help information syntax check.
        commandTree = new ArrayList();
        candidateWords = new String[]{"connect"};
        cmdElement = new CommandElement(0,1,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$ServerNameOrIP"};
        cmdElement = new CommandElement(1,2,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$port"};
        cmdElement = new CommandElement(2,3,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$password"};
        cmdElement = new CommandElement(3,-1,candidateWords,false);
        commandTree.add(cmdElement);
        command = new Command(commandTree, "connect to FyDB serve", true);
        // key should be uppercase word
        commands.put(new String("CONNECT"), command);

        // build VERSION command. 
        commandTree = new ArrayList();
        candidateWords = new String[]{"version"};
        cmdElement = new CommandElement(0,1,candidateWords,true);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"@server","@console"};
        cmdElement = new CommandElement(1,-1,candidateWords,true);
        commandTree.add(cmdElement);
        command = new Command(commandTree, "show version of console or FyDB server", true);
        // key should be uppercase word
        commands.put(new String("VERSION"), command); 

        // build SHOW command. 
        commandTree = new ArrayList();
        candidateWords = new String[]{"show"};
        cmdElement = new CommandElement(0,1,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"@error"};
        cmdElement = new CommandElement(1,-1,candidateWords,true);
        commandTree.add(cmdElement);
        command = new Command(commandTree, "show last error info", true);
        // key should be uppercase word
        commands.put(new String("SHOW"), command); 

        // build SET command. 
        commandTree = new ArrayList();
        candidateWords = new String[]{"set"};
        cmdElement = new CommandElement(0,1,candidateWords,true);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"@timing","@time"};
        cmdElement = new CommandElement(1,2,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"on","off"};
        cmdElement = new CommandElement(2,-1,candidateWords,true);
        commandTree.add(cmdElement);
        command = new Command(commandTree, "set properites of console", true);
        // key should be uppercase word
        commands.put(new String("SET"), command); 

        // build Exit command. It's a fake command here. just used to show help information.
        commandTree = new ArrayList();
        candidateWords = new String[]{"exit"};
        cmdElement = new CommandElement(0,-1,candidateWords,true);
        commandTree.add(cmdElement);
        command = new Command(commandTree, "Exit console", true);
        // key should be uppercase word
        commands.put(new String("EXIT"), command); 

        // build disconect command. It's a fake command here. just used to show help information syntax check.
        commandTree = new ArrayList();
        candidateWords = new String[]{"disconnect"};
        cmdElement = new CommandElement(0,-1,candidateWords,true);
        commandTree.add(cmdElement);
        command = new Command(commandTree, "Disconnect from server.", true);
        // key should be uppercase word
        commands.put(new String("DISCONNECT"), command); 

        // build Help command. To get command list, ALWAYS put HELP into commands set at LAST!!
        commandTree = new ArrayList();
        candidateWords = new String[]{"help"};
        cmdElement = new CommandElement(0,1,candidateWords,true);
        commandTree.add(cmdElement);
        candidateWords = new String[commands.size()];
        candidateWords = (String[])(commands.keySet()).toArray(candidateWords);
        cmdElement = new CommandElement(1,-1,candidateWords,true);
        commandTree.add(cmdElement);
        command = new Command(commandTree, "show help information", false);
        // key should be uppercase word
        commands.put(new String("HELP"), command); 
        
        // those hiden command should be built after HELP command
        // build PROMOTE command
        commandTree = new ArrayList();
        candidateWords = new String[]{"promote"};
        cmdElement = new CommandElement(0,1,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$password"};
        cmdElement = new CommandElement(1,-1,candidateWords,true);
        commandTree.add(cmdElement);
        command = new Command(commandTree, "Promote as superman on server.", true);
        // key should be uppercase word
        commands.put(new String("PROMOTE"), command); 

        // build STARTUP command
        commandTree = new ArrayList();
        candidateWords = new String[]{"startup"};
        cmdElement = new CommandElement(0,-1,candidateWords,true);
        commandTree.add(cmdElement);
        command = new Command(commandTree, "startup db server.", true);
        // key should be uppercase word
        commands.put(new String("STARTUP"), command); 

        // build SHUTDOWN command
        commandTree = new ArrayList();
        candidateWords = new String[]{"shutdown"};
        cmdElement = new CommandElement(0,-1,candidateWords,true);
        commandTree.add(cmdElement);
        command = new Command(commandTree, "shutdown db server.", true);
        // key should be uppercase word
        commands.put(new String("SHUTDOWN"), command); 

        // build size command
        commandTree = new ArrayList();
        candidateWords = new String[]{"size"};
        cmdElement = new CommandElement(0,1,candidateWords,true);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$Dataset"};
        cmdElement = new CommandElement(1,-1,candidateWords,true);
        commandTree.add(cmdElement);
        command = new Command(commandTree, "get db/dataset size", true);
        // key should be uppercase word
        commands.put(new String("SIZE"), command); 

        // build RELEASE command
        commandTree = new ArrayList();
        candidateWords = new String[]{"release"};
        cmdElement = new CommandElement(0,1,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$Dataset"};
        cmdElement = new CommandElement(1,-1,candidateWords,true);
        commandTree.add(cmdElement);
        command = new Command(commandTree, "release dataset", true);
        // key should be uppercase word
        commands.put(new String("RELEASE"), command); 

        // build RELOAD command
        commandTree = new ArrayList();
        candidateWords = new String[]{"reloade"};
        cmdElement = new CommandElement(0,1,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$Dataset"};
        cmdElement = new CommandElement(1,-1,candidateWords,true);
        commandTree.add(cmdElement);
        command = new Command(commandTree, "reload dataset", true);
        // key should be uppercase word
        commands.put(new String("RELOAD"), command); 

        // build COPY command
        commandTree = new ArrayList();
        candidateWords = new String[]{"copy"};
        cmdElement = new CommandElement(0,1,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$Dataset"};
        cmdElement = new CommandElement(1,2,candidateWords,false);
        commandTree.add(cmdElement);
        candidateWords = new String[]{"$fileName"};
        cmdElement = new CommandElement(2,-1,candidateWords,true);
        commandTree.add(cmdElement);
        command = new Command(commandTree, "copy dataset to local file", true);
        // key should be uppercase word
        commands.put(new String("COPY"), command); 
    }

    // show command information
    private boolean showCommand(String cmdKey){
        Command command = (Command)commands.get(cmdKey.toUpperCase());
        if (command == null){
            System.out.println("");
            System.out.println("Error: Command "+cmdKey+" is not supported!");
            System.out.println("");
            return false;
        }else{
            CommandElement curElement = (CommandElement)command.commandTree.get(0);
            System.out.println("");
            System.out.println(cmdKey+" : "+command.description);
            System.out.println(CommUtility.lPad(" usage: ", cmdKey.length()));
            System.out.print(cmdKey+" ");
            String optionQuote =""; // [] is used for optional clause. 
            while (curElement.nextLevel >= 0){
                if (curElement.canBeEnd && curElement.nextLevel>=0) {// following clause is optional
                    System.out.print("[");
                    optionQuote += "]";
                }
                if (curElement.nextLevel < curElement.currentLevel)
                    break;
                curElement = (CommandElement)command.commandTree.get(curElement.nextLevel);
                if (curElement.candidateWords.length>1)
                    System.out.print(" {"+curElement.concatWords()+"} ");
                else
                    System.out.print(" "+curElement.candidateWords[0]+" ");
            }
            System.out.println(optionQuote);
            System.out.println("");
            return true;
        }
        //return false;
    }

    // check session status
    private boolean checkSession(SessionClient session){
        if (session == null || session.isClosed()){
            System.out.println("");
            System.out.println("Connect to fydb server first!");
            System.out.println("");
            return false;
        }
        return true;
    }
    
    // run help command
    private boolean runHelp(String[] commandString){
        if (commandString.length == 1){ // general help information
            System.out.println("");
            System.out.println("FyDB Console Help Information:");
            System.out.println(" H(elp) [Command]      : get help information of command;");
            System.out.println(" List @Commands        : Show all commands;");
            System.out.println(" E(xit)                : exit console;");
            return true;
        }else if(commandString.length == 2){
            return showCommand(commandString[1]);
        }
        return false;
    }

    // FAKE function of run connect. it actually run in ClientConsole to get SessionLeader
    private boolean runConnect(){
        return true;
    }
    
    // run the get command in server
    private boolean runDisconnect(){
        return true;
    }
    
    // run version command
    private boolean runVersion(String[] commandString, SessionClient session){
        if (commandString.length == 1 || (commandString.length == 2 && commandString[1].equalsIgnoreCase("@console"))){ // general help information
            System.out.println("");
            System.out.println(prodName+ " " + version);
            System.out.println(copyRight);
            System.out.println("");
            return true;
        }else if(commandString.length == 2 && commandString[1].equalsIgnoreCase("@server")){
            if (!checkSession(session))
                return false;
            String serverVersion = session.getVersion();
            System.out.println("");
            System.out.println("FyDB " + serverVersion);
            System.out.println(copyRight);
            System.out.println("");
            return true;
        }
        return false;
    }

    // run promote command
    private boolean runPromote(String[] commandString, SessionClient session){
        if(commandString.length == 2){
            if (!checkSession(session))
                return false;

            boolean isSuperMan = session.promote(commandString[1]);
            System.out.println("");
            System.out.println("Promote " + (isSuperMan?"success. You are super man!":"failed."));
            System.out.println("");
            return true;
        }
        return false;
    }

    // run startup command
    private boolean runStartup(SessionClient session){
        if (!checkSession(session))
            return false;
        boolean result = session.startup();
        System.out.println("");
        System.out.println("DB server startup " + (result?"success.":"failed."));
        System.out.println("");
        return true;
    }

    // run shutdown command
    private boolean runShutdown(SessionClient session){
        if (!checkSession(session))
            return false;
        boolean result = session.shutdown();
        System.out.println("");
        System.out.println("DB server shutdown " + (result?"success.":"failed."));
        System.out.println("");
        return true;
    }

    // run size command
    private boolean runSize(String[] commandString, SessionClient session){
        if (!checkSession(session))
            return false;
        long size = 0;
        if(commandString.length == 2){
            size = session.getDatasetSize(commandString[1]);
        }else
            size = session.getDBSize();
        if (size < 0){
            System.out.println("");
            System.out.println("Get size failed. ");
            System.out.println("");
            return false;
        }
        System.out.println("");
        System.out.println("Size is " + size);
        System.out.println("");
        return true;
    }

    // run release command
    private boolean runRelease(String[] commandString, SessionClient session){
        if (!checkSession(session))
            return false;
        if(commandString.length == 2){
            boolean result = session.releaseDataset(commandString[1]);
            System.out.println("");
            System.out.println("Release " + (result?"success.":"failed."));
            System.out.println("");
            return true;
        }
        return false;
    }

    // run reload command
    private boolean runReload(String[] commandString, SessionClient session){
        if (!checkSession(session))
            return false;
        if(commandString.length == 2){
            boolean result = session.reloadDataset(commandString[1]);
            System.out.println("");
            System.out.println("Reload " + (result?"success.":"failed."));
            System.out.println("");
            return true;
        }
        return false;
    }

    // run copy command
    private boolean runCopy(String[] commandString, SessionClient session){
        if (!checkSession(session))
            return false;
        if(commandString.length == 3){
            boolean result = session.copyDatasetToFile(commandString[1], commandString[2]);
            System.out.println("");
            System.out.println("copy dataset " + (result?"success.":"failed."));
            System.out.println("");
            return true;
        }
        return false;
    }

    private HashMap parseColumns(String rawString){
        if (rawString == null)
            return null;
        String colArray[] = CommUtility.splitString(rawString,';',false);
        HashMap data = new HashMap();
        for (int i=0;i<colArray.length;i++){
            String datas[] = CommUtility.splitString(colArray[i],':',false);
            Integer colID;
            try {
                colID = new Integer(datas[0]);
            }catch(NumberFormatException e){
                System.out.println("");
                System.out.println("Error: Invalid key column ID, it should be Integer");
                System.out.println("");
                return null;
            }
            data.put(colID,datas.length>1?datas[1]:null);
        }
        return data;
    }

    private boolean parseColumns(String rawString, ArrayList colNames, ArrayList values){
        if (rawString == null || colNames == null || values == null)
            return false;
        colNames.clear();
        values.clear();
        String colArray[] = CommUtility.splitString(rawString,';',false);
        for (int i=0;i<colArray.length;i++){
            String datas[] = CommUtility.splitString(colArray[i],':',false);
            colNames.add(datas[0]);
            values.add(datas.length>1?datas[1]:null);
        }
        return true;
    }

    //format and show metaData
    private void showMetaData(FyMetaData metaData){
        if (metaData == null || metaData.getColumns() == null)
            return;
        HashMap columns = metaData.getColumns();
        Iterator it = columns.keySet().iterator();
        System.out.println("");

        for (int i=0;i<metaData.getColumns().size();i++){
            HashMap colProperties = (HashMap)columns.get(Integer.valueOf(i));
            if (colProperties == null)
                continue;
            String colName = (String)colProperties.get("N");
            int colType = (Integer)colProperties.get("T");
            boolean isKey = (Integer)colProperties.get("K") >= 0?true:false;
            boolean nullable = (Integer)colProperties.get("NL") == 1?true:false;
            System.out.println(i+":\t"+colName+"\t("+(isKey?"KEY\t":"")+(nullable?"":"NOT NULL\t")+Consts.decodeDataType(colType)+")");
        }
        System.out.println("");
    }

    //format and show result
    private void showDataSet(FyMetaData metaData, ArrayList datas){
        String formatStr = "";
        String line = "";
        if (metaData == null || metaData.getColumns() == null)
            return;
        HashMap columns = metaData.getColumns();
        Iterator it = columns.keySet().iterator();
        while (it.hasNext()){
            Integer colID = (Integer)it.next();
            String colName = (String)((HashMap)columns.get(colID)).get("N");
            formatStr += colName+"\t";
            line += CommUtility.repeatStr("-", colName.length())+"\t";
        }
        System.out.println("");
        System.out.println(formatStr);
        System.out.println(line);

        if (datas == null){
            System.out.println("Search datas failed.");
            System.out.println("");
            return;
        }
        for (int i=0;i<datas.size();i++){
            formatStr = "";
            HashMap data = (HashMap)datas.get(i);
            if (data == null)
                continue;
            it = columns.keySet().iterator();
            while (it.hasNext()){
                Integer colID = (Integer)it.next();
                String colData = (String)data.get(colID);
                formatStr += colData+"\t";
            }
            System.out.println(formatStr);
        }
        System.out.println("");
        System.out.println(datas.size()+" datas found.");
        System.out.println("");
    }

    //format and show join result
    private void showDataSet(FyMetaData metaData1, FyMetaData metaData2, ArrayList datas){
        String formatStr = "";
        String line = "";
        if (metaData1 == null || metaData1.getColumns() == null || metaData2 == null || metaData2.getColumns() == null)
            return;
        HashMap columns1 = metaData1.getColumns();
        Iterator it1 = columns1.keySet().iterator();
        while (it1.hasNext()){
            Integer colID = (Integer)it1.next();
            String colName = (String)((HashMap)columns1.get(colID)).get("N");
            formatStr += colName+"\t";
            line += CommUtility.repeatStr("-", colName.length())+"\t";
        }
        HashMap columns2 = metaData2.getColumns();
        Iterator it2 = columns2.keySet().iterator();
        while (it2.hasNext()){
            Integer colID = (Integer)it2.next();
            String colName = (String)((HashMap)columns2.get(colID)).get("N");
            formatStr += colName+"\t";
            line += CommUtility.repeatStr("-", colName.length())+"\t";
        }
        System.out.println("");
        System.out.println(formatStr);
        System.out.println(line);

        if (datas == null){
            System.out.println("Search datas failed.");
            System.out.println("");
            return;
        }
        for (int i=0;i<datas.size();i++){
            formatStr = "";
            HashMap[] joinResult = (HashMap[])datas.get(i);
            HashMap data1 = joinResult[0];
            if (data1 == null)
                formatStr += CommUtility.repeatStr("\t",columns1.size());
            else{
                it1 = columns1.keySet().iterator();
                while (it1.hasNext()){
                    Integer colID = (Integer)it1.next();
                    String colData = (String)data1.get(colID);
                    formatStr += colData+"\t";
                }
            }
            HashMap data2 = joinResult[1];
            if (data2 == null)
                formatStr += CommUtility.repeatStr("\t",columns2.size());
            else{
                it2 = columns2.keySet().iterator();
                while (it2.hasNext()){
                    Integer colID = (Integer)it2.next();
                    String colData = (String)data2.get(colID);
                    formatStr += colData+"\t";
                }
            }
            System.out.println(formatStr);
        }
        System.out.println("");
        System.out.println(datas.size()+" datas found.");
        System.out.println("");
    }

    // run the get command in server
    private boolean runGet(String[] commandString, SessionClient session){
        if (!checkSession(session))
            return false;
        if (commandString.length == 4 && commandString[1].equalsIgnoreCase("@metaData")){
            FyMetaData metaData = session.getMetaData(commandString[3]);
            showMetaData(metaData);
        }else if (commandString.length == 4 && commandString[1].equalsIgnoreCase("@value")){ // full data get
            FyMetaData metaData = session.getMetaData(commandString[3]);
            ArrayList result = session.searchData(commandString[3],new Prediction(),false);
            showDataSet(metaData, result);
        }else if (commandString.length == 6 && commandString[1].equalsIgnoreCase("@value")){ // search by criterias
            FyMetaData metaData = session.getMetaData(commandString[3]);
            Prediction prediction = StatementParser.buildPrediction(CommUtility.rtrim(CommUtility.ltrim(commandString[5],'{'),'}'));
            ArrayList result = session.searchData(commandString[3],prediction,false);
            showDataSet(metaData, result);
        }
        return true;
    }
    
    // run the remove command in server
    private boolean runRemove(String[] commandString, SessionClient session){
        int removedNum = 0;
        if (!checkSession(session))
            return false;
        if (commandString.length == 3){ // full data remove
            removedNum = session.removeData(commandString[2], new Prediction(),false);
        }else if (commandString.length == 5){ // remove by criterias
            Prediction prediction = StatementParser.buildPrediction(CommUtility.rtrim(CommUtility.ltrim(commandString[4],'{'),'}'));
            removedNum = session.removeData(commandString[2],prediction,false);
        }
        if (removedNum<0){
            System.out.println("");
            System.out.println("Remove data failed!");
            System.out.println("");
        }else{
            System.out.println("");
            System.out.println(removedNum+" data removed.");
            System.out.println("");
        }
        return true;
    }
    
    // run the add command in server
    private boolean runAdd(String[] commandString, SessionClient session){
        boolean dataInstered = false;
        if (!checkSession(session))
            return false;
        if (commandString.length == 4){ // 
            ArrayList colNames = new ArrayList();
            ArrayList values = new ArrayList();
            if (parseColumns(CommUtility.rtrim(CommUtility.ltrim(commandString[1],'{'),'}'), colNames, values))
                dataInstered = session.addData(commandString[3],colNames,values);
        }
        if (!dataInstered){
            System.out.println("");
            System.out.println("Insert data failed!");
            System.out.println("");
        }else{
            System.out.println("");
            System.out.println("data inserted.");
            System.out.println("");
        }
        return true;
    }
    
    // run the modify command in server
    private boolean runModify(String[] commandString, SessionClient session){
        int modNum = 0;
        if (!checkSession(session))
            return false;
        if (commandString.length == 4){ // full data modify
            ArrayList colNames = new ArrayList();
            ArrayList values = new ArrayList();
            if (parseColumns(CommUtility.rtrim(CommUtility.ltrim(commandString[1],'{'),'}'), colNames, values))
                modNum = session.modifyData(commandString[3],colNames,values, new Prediction(),false);
        }else if (commandString.length == 6){ // data modify with criteria
            Prediction prediction = StatementParser.buildPrediction(CommUtility.rtrim(CommUtility.ltrim(commandString[5],'{'),'}'));
            ArrayList colNames = new ArrayList();
            ArrayList values = new ArrayList();
            if (parseColumns(CommUtility.rtrim(CommUtility.ltrim(commandString[1],'{'),'}'), colNames, values))
                modNum = session.modifyData(commandString[3],colNames,values,prediction,false);
        }
        if (modNum<0){
            System.out.println("");
            System.out.println("Modify data failed!");
            System.out.println("");
        }else{
            System.out.println("");
            System.out.println(modNum + " data modified.");
            System.out.println("");
        }
        return true;
    }
    
    // run the show command in server
    private boolean runShow(String[] commandString, SessionClient session){
        String errMsg = null;
        if (!checkSession(session))
            return false;
        if (commandString.length == 2 && "@error".equalsIgnoreCase(commandString[1])){ // full data modify
            errMsg = session.getLastError();
        }
        if (errMsg == null || "".equals(errMsg)){
            System.out.println("");
            System.out.println("No error");
            System.out.println("");
        }else{
            System.out.println("");
            System.out.println(errMsg);
            System.out.println("");
        }
        return true;
    }
    
    // run the join command in server
    private boolean runJoin(String[] commandString, SessionClient session){
        if (!checkSession(session))
            return false;
        if (commandString.length == 11){ // full data modify
            FyMetaData metaData1 = session.getMetaData(commandString[3]);
            FyMetaData metaData2 = session.getMetaData(commandString[6]);
            Prediction prediction1 = StatementParser.buildPrediction(CommUtility.rtrim(CommUtility.ltrim(commandString[5],'{'),'}'));
            Prediction prediction2 = StatementParser.buildPrediction(CommUtility.rtrim(CommUtility.ltrim(commandString[8],'{'),'}'));
            Prediction joinPred = StatementParser.buildPrediction(CommUtility.rtrim(CommUtility.ltrim(commandString[10],'{'),'}'));
            int joinMode = Consts.UNKNOWN;
            if ("ON".equalsIgnoreCase(commandString[9]))
                joinMode = Consts.INNERJOIN;
            else if ("LEFTON".equalsIgnoreCase(commandString[9]))
                joinMode = Consts.LEFTJOIN;
            else if ("RIGHTON".equalsIgnoreCase(commandString[9]))
                joinMode = Consts.RIGHTJOIN;
            else if ("OUTON".equalsIgnoreCase(commandString[9]))
                joinMode = Consts.OUTERJOIN;
            else
                return false;
            ArrayList result = session.searchJoinData(commandString[3],commandString[6],null,null,prediction1,prediction2,joinPred,joinMode,false);
            showDataSet(metaData1, metaData2, result);
        }
        return true;
    }
    
    // run the list command in server
    private boolean runList(String[] commandString, SessionClient session){
        if (commandString.length == 2 ){
            if (commandString[1].equalsIgnoreCase("@datasets")){
                if (!checkSession(session))
                    return false;
                ArrayList dsNames = session.getDataSetsList();
                if (dsNames != null){
                    System.out.println("");
                    System.out.println("dataset name");
                    System.out.println("--------------------------------------");
                    for (int i=0; i<dsNames.size();i++)
                        System.out.println((String)dsNames.get(i));
                    System.out.println("");
                    System.out.println("Totally "+dsNames.size()+" datasets loaded in server.");
                }
                else{
                    System.out.println("");
                    System.out.println("No dataset loaded in server!");
                    System.out.println("");
                }
                return true;
            }else if(commandString[1].equalsIgnoreCase("@commands")){
                System.out.println("");
                for (Object command: commands.keySet().toArray())
                    System.out.println(command.toString());
                System.out.println("");
            }
        }
        return false;
    }
    
    // run the set command
    private boolean runSet(String[] commandString){
        if (commandString.length == 3 && commandString[1].equalsIgnoreCase("@timing")){
            if ("on".equalsIgnoreCase(commandString[2]))
                showTiming = true;
            else if ("off".equalsIgnoreCase(commandString[2]))
                showTiming = false;
            else
                return false;
        }else if (commandString.length == 3 && commandString[1].equalsIgnoreCase("@time")){
            if ("on".equalsIgnoreCase(commandString[2]))
                showCurTime = true;
            else if ("off".equalsIgnoreCase(commandString[2]))
                showCurTime = false;
            else
                return false;
        }
        return true;
    }
    
    // run server command
    public boolean runCommand(String[] commandString, SessionClient session){
        if (showCurTime){
            SimpleDateFormat tempDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
            String datetime = tempDate.format(new java.util.Date());
            System.out.println(datetime);
        }
        long curMilliSecs = System.currentTimeMillis();
        boolean result = false;
        if (commandString[0].equalsIgnoreCase("get"))
            result = runGet(commandString, session);
        else if (commandString[0].equalsIgnoreCase("join"))
            result = runJoin(commandString, session);
        else if (commandString[0].equalsIgnoreCase("remove"))
            result = runRemove(commandString, session);
        else if (commandString[0].equalsIgnoreCase("add"))
            result = runAdd(commandString, session);
        else if (commandString[0].equalsIgnoreCase("modify"))
            result = runModify(commandString, session);
        else if (commandString[0].equalsIgnoreCase("show"))
            result = runShow(commandString, session);
        else if (commandString[0].equalsIgnoreCase("set"))
            result = runSet(commandString);
        else if (commandString[0].equalsIgnoreCase("list"))
            result = runList(commandString, session);
        else if (commandString[0].equalsIgnoreCase("help"))
            result = runHelp(commandString);
        else if (commandString[0].equalsIgnoreCase("connect"))
            result = runConnect();
        else if (commandString[0].equalsIgnoreCase("disconnect"))
            result = runDisconnect();
        else if (commandString[0].equalsIgnoreCase("version"))
            result = runVersion(commandString, session);
        else if (commandString[0].equalsIgnoreCase("promote"))
            result = runPromote(commandString, session);
        else if (commandString[0].equalsIgnoreCase("startup"))
            result = runStartup(session);
        else if (commandString[0].equalsIgnoreCase("shutdown"))
            result = runShutdown(session);
        else if (commandString[0].equalsIgnoreCase("size"))
            result = runSize(commandString, session);
        else if (commandString[0].equalsIgnoreCase("release"))
            result = runRelease(commandString, session);
        else if (commandString[0].equalsIgnoreCase("reload"))
            result = runReload(commandString, session);
        else if (commandString[0].equalsIgnoreCase("copy"))
            result = runCopy(commandString, session);
        if (showTiming){
            System.out.println("Time eplapsed: "+CommUtility.milliSecsToTime(System.currentTimeMillis() - curMilliSecs));
        }
        return result;
    }

    // parse command
    public boolean parseCommand(String[] commandString){
        Command command = (Command)commands.get(commandString[0].toUpperCase());
        if (command == null){
            System.out.println("");
            System.out.println("Error: "+commandString[0] + " is not a valid command. Please type HELP to get help inform.");
            System.out.println("");
            return false;
        }
        CommandElement curElement = (CommandElement)command.commandTree.get(0);
        if (curElement.nextLevel >= 0){
            CommandElement nextElement = (CommandElement)command.commandTree.get(curElement.nextLevel);
            for (int i=1;i<commandString.length;i++){
                //check following word
                if (!nextElement.containsWord(commandString[i])){
                    System.out.println("");
                    System.out.println(String.format("Error: Expecting words {%s} following %s", nextElement.concatWords(), commandString[i-1]));
                    System.out.println(String.format("       But got %s", commandString[i]));
                    System.out.println("");
                    return false;
                }
                curElement = nextElement;
                if (curElement.nextLevel<0){ // this is the definate end word
                    if (i<commandString.length-1){
                        System.out.println("");
                        System.out.println(String.format("Error: Command should be end, But got %s", commandString[i+1]));
                        System.out.println("");
                        return false;
                    }
                }else{
                    nextElement = (CommandElement)command.commandTree.get(curElement.nextLevel);
                }
            }
            if (!curElement.canBeEnd){
                System.out.println("");
                System.out.println(String.format("Error: Expecting words (%s) following %s", nextElement.concatWords(), commandString[commandString.length-1]));
                System.out.println(String.format("       But got nothing"));
                System.out.println("");
                return false;
            }
        }
        return true;
    }
}
