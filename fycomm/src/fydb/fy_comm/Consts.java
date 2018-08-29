/**
 * @(#)Consts.java	0.01 11/05/25
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_comm;

public final class Consts {
    //public enum phyTypes{
    //    DB_Oracle, DB_Mysql, HashMap, SortedMap, TreeMap, File_Json, File_XML
    //}
    public final static int UNKNOWN = 0;
    // 1 ~ 10 are defined for RDBMS
    public final static int DB_ORACLE = 1;
    public final static int DB_MYSQL = 2;
    public final static int DB_MSSQL = 3;
    public final static int DB_DB2 = 4;
    public final static int DB_SYBASE = 5;
    // 11 ~ 20 are defined for K-V
    public final static int HASHMAP = 11;
    public final static int SORTEDMAP = 12;
    public final static int TREEMAP = 13;
    // supported physical data types : all
    // supported memory data types : HASHMAP; SORTEDMAP; TREEMAP
    //public enum memTypes{
    //    HashMap, SortedMap, TreeMap, Unknown
    //}

    // statuses of asynchronized calling
    //public enum AsynStatus {
    //    clear, preinitializing, initializing, initialized, running, completed, failed
    //}
    public final static int CLEAR=1;
    public final static int PREINITIALIZING=2;
    public final static int INITIALIZING=3;
    public final static int INITIALIZED=4;
    public final static int RUNNING=5;
    public final static int COMPLETED=6;
    public final static int FAILED=7;
    // asynchronized calling command
    //public enum CallCommand {
    //    batchDeleteData, batchInsertData, batchModifyMemDataByKey, searchData
    //}
    public final static int BATCHDELETEDATA=1;
    public final static int BATCHINSERTDATA=2;
    public final static int BATCHMODIFYMEMDATABYKEY=3;
    public final static int SEARCHDATA=4;
    public final static int ISBEEPERHOST=5;
    public final static int HANDOVERBEEPER=6;
    public final static int SYNCHRONIZESERVERS=7;
    public final static int GETBP=8;
    public final static int GETCURBP=9;
    public final static int ISMASTER=10;
    public final static int HANDOVERMASTER=11;
    public final static int SYNCHRONIZELOGS=12;
    public final static int ADDDATA=13;
    public final static int REMOVEDATA=14;
    public final static int GETDATASETLIST=15;
    public final static int DELETEDATABYKEY=16;
    public final static int INSERTDATAM=17;
    public final static int INSERTDATAS=18;
    public final static int INSERTDATAI=19;
    public final static int MODIFYDATABYKEM=20;
    public final static int MODIFYDATABYKES=21;
    public final static int MODIFYDATABYKEI=22;
    public final static int SINGLEREAD=23;
    public final static int GETMETADATA=24;
    public final static int GETDATAPROPS=25;
    public final static int BEGINFETCH=26;
    public final static int BATCHFETCH=27;
    public final static int MODIFYDATA=28;
    public final static int SEARCHJOINDATA=29;
    public final static int GETLASTERROR=30;
    public final static int GETVERSION=31;
    public final static int PROMOTE=32;
    public final static int SHUTDOWN=33;
    public final static int RELEASEDATASET=34;
    public final static int RELOADDATASET=35;
    public final static int STARTUP=36;
    public final static int GETDATASETSIZE=37;
    public final static int GETDBSIZE=38;
    public final static int COPYDATASETTOFILE=39;
    public final static int BYEBYE=88;
    
    // prediction node type
    public final static int BRANCH=1;
    public final static int LEAF=2;

    // prediction junction
    public final static int AND=1;
    public final static int OR=2;

    // prediction comparator. 1: ==; 2: >; 3: <; 4: !=; 5: >=; 6: <=
    public final static int EQ=1;
    public final static int LT=2;
    public final static int ST=3;
    public final static int NEQ=4;
    public final static int LE=5;
    public final static int SE=6;

    // state
    public final static int VALID=1;
    public final static int INVALID=2;

    // object&block type
    public final static int DATA=1;
    public final static int INDEX=2;
    public final static int META=3;
    public final static int BMB=4;
    public final static int SUPDATA=5;

    // data operate type
    public final static int INSERT=1;
    public final static int MODIFY=2;
    public final static int DELETE=3;

    // data access type. the number also indicated its priority
    public final static int INDEXGET=1;
    public final static int PARTINDEXSCAN=5;
    public final static int FULLINDEXSCAN=11;
    public final static int FULLDATASCAN=21;

    // data types
    public final static int INTEGER=11;
    public final static int LONG=12;
    public final static int DOUBLE=13;
    public final static int STRING=21;
    public final static int DATE=31;
    public final static int TIMESTAMP=32;
    public final static int BOOLEAN=41;
    public final static int NULL=51;

    // network channel types
    public final static int SERVER=1;
    public final static int CLIENT=2;

    // working modes
    public final static int BUFFER=1;
    public final static int DISK=2;

    // locations
    public final static int LOCAL=1;
    public final static int REMOTE=2;

    // join mode
    public final static int INNERJOIN=1;
    public final static int LEFTJOIN=2;
    public final static int RIGHTJOIN=3;
    public final static int OUTERJOIN=4;

    // sides
    public final static int LEFT=1;
    public final static int RIGHT=2;

    // join method
    public final static int LOOPJOIN=1;
    public final static int HASHJOIN=2;
    public final static int SORTJOIN=3;

    public Consts() {
    }

    public static int valueOf(String str){
        if ("UNKNOWN".equalsIgnoreCase(str))
            return 0;
        else if ("DB_ORACLE".equalsIgnoreCase(str) || "CLEAR".equalsIgnoreCase(str) || 
                 "BATCHDELETEDATA".equalsIgnoreCase(str) || "BRANCH".equalsIgnoreCase(str) ||
                 "AND".equalsIgnoreCase(str) || "EQ".equalsIgnoreCase(str) ||
                 "VALID".equalsIgnoreCase(str) || "DATA".equalsIgnoreCase(str) ||
                 "INSERT".equalsIgnoreCase(str) || "INDEXGET".equalsIgnoreCase(str) || 
                 "SERVER".equalsIgnoreCase(str) || "BUFFER".equalsIgnoreCase(str) ||
                 "LOCAL".equalsIgnoreCase(str) || "INNERJOIN".equalsIgnoreCase(str) ||
                 "LEFT".equalsIgnoreCase(str) || "LOOPJOIN".equalsIgnoreCase(str))
            return 1;
        else if ("DB_MYSQL".equalsIgnoreCase(str) || "PREINITIALIZING".equalsIgnoreCase(str) || 
                 "BATCHINSERTDATA".equalsIgnoreCase(str) || "LEAF".equalsIgnoreCase(str) || 
                 "OR".equalsIgnoreCase(str) || "LT".equalsIgnoreCase(str) ||
                 "INVALID".equalsIgnoreCase(str) || "INDEX".equalsIgnoreCase(str) ||
                 "MODIFY".equalsIgnoreCase(str) || "CLIENT".equalsIgnoreCase(str) ||
                 "DISK".equalsIgnoreCase(str) || "REMOTE".equalsIgnoreCase(str) ||
                 "LEFTJOIN".equalsIgnoreCase(str) || "RIGHT".equalsIgnoreCase(str) ||
                 "HASHJOIN".equalsIgnoreCase(str))
            return 2;
        else if ("DB_MSSQL".equalsIgnoreCase(str) || "INITIALIZING".equalsIgnoreCase(str) || 
                 "BATCHMODIFYMEMDATABYKEY".equalsIgnoreCase(str) || "ST".equalsIgnoreCase(str) ||
                 "DELETE".equalsIgnoreCase(str) || "META".equalsIgnoreCase(str) ||
                 "RIGHTJOIN".equalsIgnoreCase(str) || "SORTJOIN".equalsIgnoreCase(str))
            return 3;
        else if ("DB_DB2".equalsIgnoreCase(str) || "INITIALIZED".equalsIgnoreCase(str) || 
                 "SEARCHDATA".equalsIgnoreCase(str) || "NEQ".equalsIgnoreCase(str) || 
                 "BMB".equalsIgnoreCase(str) || "OUTERJOIN".equalsIgnoreCase(str))
            return 4;
        else if ("DB_SYBASE".equalsIgnoreCase(str) || "RUNNING".equalsIgnoreCase(str) ||
                 "LE".equalsIgnoreCase(str) || "PARTINDEXSCAN".equalsIgnoreCase(str) || 
                 "SUPDATA".equalsIgnoreCase(str) || "ISBEEPERHOST".equalsIgnoreCase(str))
            return 5;
        else if ("COMPLETED".equalsIgnoreCase(str) || "SE".equalsIgnoreCase(str) ||
                 "HANDOVERBEEPER".equalsIgnoreCase(str))
            return 6;
        else if ("FAILED".equalsIgnoreCase(str) || "SYNCHRONIZESERVERS".equalsIgnoreCase(str))
            return 7;
        else if ("GETBP".equalsIgnoreCase(str))
            return 8;
        else if ("GETCURBP".equalsIgnoreCase(str))
            return 9;
        else if ("ISMASTER".equalsIgnoreCase(str))
            return 10;
        else if ("HASHMAP".equalsIgnoreCase(str) || "FULLINDEXSCAN".equalsIgnoreCase(str) || 
                 "INTEGER".equalsIgnoreCase(str) || "HANDOVERMASTER".equalsIgnoreCase(str))
            return 11;
        else if ("SORTEDMAP".equalsIgnoreCase(str) || "LONG".equalsIgnoreCase(str) ||
                 "SYNCHRONIZELOGS".equalsIgnoreCase(str))
            return 12;
        else if ("TREEMAP".equalsIgnoreCase(str) || "DOUBLE".equalsIgnoreCase(str) ||
                 "ADDDATA".equalsIgnoreCase(str))
            return 13;
        else if ("REMOVEDATA".equalsIgnoreCase(str))
            return 14;
        else if ("GETDATASETLIST".equalsIgnoreCase(str))
            return 15;
        else if ("DELETEDATABYKEY".equalsIgnoreCase(str))
            return 16;
        else if ("INSERTDATAM".equalsIgnoreCase(str))
            return 17;
        else if ("INSERTDATAS".equalsIgnoreCase(str))
            return 18;
        else if ("INSERTDATAI".equalsIgnoreCase(str))
            return 19;
        else if ("MODIFYDATABYKEM".equalsIgnoreCase(str))
            return 20;
        else if ("FULLDATASCAN".equalsIgnoreCase(str) || "STRING".equalsIgnoreCase(str) ||
                 "MODIFYDATABYKES".equalsIgnoreCase(str))
            return 21;
        else if ("MODIFYDATABYKEI".equalsIgnoreCase(str))
            return 22;
        else if ("SINGLEREAD".equalsIgnoreCase(str))
            return 23;
        else if ("GETMETADATA".equalsIgnoreCase(str))
            return 24;
        else if ("GETDATAPROPS".equalsIgnoreCase(str))
            return 25;
        else if ("BEGINFETCH".equalsIgnoreCase(str))
            return 26;
        else if ("BATCHFETCH".equalsIgnoreCase(str))
            return 27;
        else if ("MODIFYDATA".equalsIgnoreCase(str))
            return 28;
        else if ("SEARCHJOINDATA".equalsIgnoreCase(str))
            return 29;
        else if ("GETLASTERROR".equalsIgnoreCase(str))
            return 30;
        else if ("DATE".equalsIgnoreCase(str) || "GETVERSION".equalsIgnoreCase(str))
            return 31;
        else if ("TIMESTAMP".equalsIgnoreCase(str) || "PROMOTE".equalsIgnoreCase(str))
            return 32;
        else if ("SHUTDOWN".equalsIgnoreCase(str))
            return 33;
        else if ("RELEASEDATASET".equalsIgnoreCase(str))
            return 34;
        else if ("RELOADDATASET".equalsIgnoreCase(str))
            return 35;
        else if ("STARTUP".equalsIgnoreCase(str))
            return 36;
        else if ("GETDATASETSIZE".equalsIgnoreCase(str))
            return 37;
        else if ("GETDBSIZE".equalsIgnoreCase(str))
            return 38;
        else if ("COPYDATASETTOFILE".equalsIgnoreCase(str))
            return 39;
        else if ("BOOLEAN".equalsIgnoreCase(str))
            return 41;
        else if ("NULL".equalsIgnoreCase(str))
            return 51;
        else if ("BYEBYE".equalsIgnoreCase(str))
            return 88;
        else
            return -1;
    }

    public static String decodeDataType(int dataType){
        switch (dataType){
        case INTEGER:
            return "INTEGER";
        case LONG:
            return "LONG";
        case DOUBLE:
            return "DOUBLE";
        case STRING:
            return "STRING";
        case DATE:
            return "DATE";
        case TIMESTAMP:
            return "TIMESTAMP";
        case BOOLEAN:
            return "BOOLEAN";
        case NULL:
            return "NULL";
        default:
            return "UNKNOWN";
        }
    }

    public static String decodeJunction(int junction){
        switch (junction){
        case AND:
            return "AND";
        case OR:
            return "OR";
        default:
            return "UNKNOWN";
        }
    }

    public static String decodeComparator(int comparator){
        switch (comparator){
        case EQ:
            return "=";
        case LT:
            return ">";
        case ST:
            return "<";
        case NEQ:
            return "!=";
        case LE:
            return ">=";
        case SE:
            return "<=";
        default:
            return "UNKNOWN";
        }
    }
}
