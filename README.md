# FyDB
FyDB is a NOSQL database system implemented by Java

Web Site: http://www.HelloDBA.com

FyDB is a NOSQL database system. It supports distributed deployment, and can integrate heterogeneous data sources. Data of FyDB is stroed in memory as key-value structure, and it is also persistent. According to NOSQL theories, FyDB discarded support on Data Integration to gain better performance and high availability, which means it will not check integration of Parent-Children data. While Data Integration is one of the most important features of traditional relationship database.



1.   Featurs:
Data Storage/Access:
·         In-memory data access;

·         Data persistent supported;

·         Local In-Memory Index supported:

       Hash index

       B-Tree Index;

·         KV map Get()/Put()/Remove() Methods supported;

·         Batch data search/modify/add/remove supported;

·         2 data sets join search supported;

·         Simple data access script supported;

·         Simple optimizer chooses the best access method;

·         Data consistent read supported;

·         Supported data types:

       String (char, varchar, varchar2 ... );

       Date (Date, Time, Datetime, Timestamp ...);

       Integer (Number, Integer, Int ...);

       Long (Long Int ...)

       Double (Double, Float ...);

Distributed/Heterogeneous:
·         High Availability: Multiple server nodes;

·         Heterogeneous:

       Traditional RDBMS (Oracle, MySQL, MS SQL, Sybase, DB2) as external data source;

       Local file as external data source;

·         OS independent;

 

2.   Installation
    Java 5 or higher is required.

Server:
1.     Download the last zip package from www.HelloDBA.com;

2.     Create a folder in your server, e.g. c:\fydb

3.     Extract the required files in the folder.

            fy_server.jar

            fy_comm.jar

            parameters.ini

            DataSets.xml (need to be configured)

4.     Optional: if you want to deploy FyDB as server, you should generate the key/trust files using the keytool of java. You find 2 demo files from the package;

5.     Optional: if you want to deploy FyDB as distributed server, you should generate the key/trust files using the keytool of java. You find 2 demo files from the package;

6.     Optional: if you want to adopt traditional RDBMS as external data source, you should download according JDBC drivers, e.g.

            ojdbc14.jar (Oracle)

            jconn3.jar (Sybase)

            mysql-connector-java-3.1.14-bin.jar (MySQL)

            sqljdbc.jar (MS SQL)

 

Client:
1.     Download the last zip package from www.HelloDBA.com;

2.     Create a folder in your server, e.g. c:\fydb

3.     Extract the required files in the folder.

            fy_client.jar

            fy_comm.jar

            parameters.ini

            SSL key/trust files

 

Notice: You should align Java version in all servers and clients.

3.   Configuration
parameters.ini:
Server Parameters:
·         logFileSize: Maximum size of log file, unit is byte. Once reached limitation, FyDB will create another log file named in sequence.

·         logNumber: Maximum log file number, Once reached limitation, FyDB will remove old one and generate a new log file.

·         maxSessionNumber: Maximum client session number in local server.

·         baseDir: Basic directory for storing files, e.g. log, trace and data files;

·         traceDir: Directory for storing trace file, if not set, FyDB will create one base on the baseDir.

·         logDir: Directory for storing log file, if not set, FyDB will create one base on the baseDir.

·         dataDir: Directory for storing local data file, if not set, FyDB will create one base on the baseDir.

 

·         dbKeyFile: Name of ssl Key file for client connection.

·         dbTrustFile: Name of ssl trust file for client connection.

·         dbPassword: Password for client ssl connection.

·         dbHostAddr: IP Address of local server. You should specify it if you have multiple IP addresses.

·         dbGatePort: Service port of local server.

 

·         imKeyFile: Name of ssl Key file for distributed servers connection.

·         imTrustFile: Name of ssl trust file for distributed servers connection.

·         imPassword: Password for distributed servers connection.

·         imHostAddr: IP Address of local server. You should specify it if you have multiple IP addresses.

·         imGatePort: Service port of local server.

 

·         buddyServers: IP Address of buddy servers in distributed deployment.

Client Parameters:
·         dbKeyFile: Name of ssl Key file for client connection.

·         dbTrustFile: Name of ssl trust file for client connection.

 

Datasets.xml
    Notice: element names are case sensitive!

 

·         dataSet: External data source configuration. You can configure multiple datasets.

o        name: obsoleted!

o        memType: Storage type in memory.

§         HashMap --- Better performance for single key get

§         TreeMap --- Better performance for key range search

o        phyType: External data source. DB_Oracle, DB_Mysql, DB_Mssql, DB_Sybase, DB_DB2, HashMap

§         DB_* means to adopt the traditional RDBMS as external data source, you should also download corresponding JDBC drirver and put in the folder containing FyDB, or in CLASSPATH;

§         HashMap means to adopt local or remote file as external data source.

·         tableName:

o        For DB_* physical types, it's the table name in the RDBMS;

o        For HashMap, it's the file name. Notice, FyDB will automaticly add a sequence number following the file name, you should NOT input the sequence number at here. For example, the file name is T_TEST, it's actual file name in OS is T_TEST0.dat, T_TEST1.dat ... And you should just input T_TEST at here.

o        Notice: in a distributed environment, FyDB will check if there is a dataSet with same name (case insensitive, for DB_* source, it will also consider connString) loaded in other servers. Once find a loaded dataSet with same name, it will ship the data from the remote server instead of from the external data source specified.

·         keyColumns: Key columns of the data source, which should be UNIQUE and NOT NULL. You can specify one or more columns of the key.

Notice: if you want to ship the data from other servers, you can ignore this element.

·         where: It's an optional element for RDBMS data source. You can special it if you just want to get part of data in the table. It's WHERE clause of standard SQL.

·         connString: It's an element for RDBMS data source. It's the JDBC connection string.

Notice: for the same data source deployed in multiple servers, you should specify the exactly same connString in each server, otherwise, it will confuse FyDB servers. For example, you should NOT specify IP in the connString for one server, and specify HOST Name for other server.

·         dbUser: It's an element for RDBMS data source. It's the user name connecting to RDBMS.

·         dbPassword: It's an element for RDBMS data source. It's the password of user name connecting to RDBMS.

·         storeLocal: It's an optional element for file data source when shipping data from other servers. It will tell FyDB to store a mirror data file in local or not.

Notice: if there already is a file with same name stored in local, FyDB will rename it as OldFileName.bak, then store the new one.

·         indexes: Local in-memory indexes.

o        Notice: All of indexes in FyDB will not be stored in physical data source, they will be built in local server memory when loading the dataset.

o        indexType: Structure type of the index.

§         HashMap --- Better performance for single key get

§         TreeMap --- Better performance for key range search

o        columns: Columns to be indexed. You can specify one or more columns of the index.

4.   Run
    Once completed installation and configure, you can run FyDB server/client called by java JVM.

    e.g.

C:\Fydb> java -jar fy_server.jar

launching FyDB server ...

loading system meta data ...

building communicator ...

Initializing communicator ...

Connecting to buddies ...

huang2/192.168.1.2

connecting to buddy huang2

huang2

2011-07-08 10:30:13 347(warning): Connect to buddy failed

Initializing session leader ...

loading datasets ...

jdbc:mysql://huang2:3306/wapdb

java.lang.Exception: 2011-07-08 10:30:16 144(Sys error): connectting to physical data source failed

        at fydb.fy_comm.Tracer.trace(Unknown Source)

        at fydb.fy_main.Manager.loadDataSet(Unknown Source)

        at fydb.fy_main.Manager.for(Unknown Source)

        at fydb.fy_main.Manager.launch(Unknown Source)

        at fydb.fy_main.Manager.main(Unknown Source)

2011-07-08 10:30:18 691: T_TEST2 loaded.

C:\my java\Fydb\classes\log\0\T_USER.sys

2011-07-08 10:30:18 707(warning): Log control file not exist, will create a new one

C:\my java\Fydb\classes\log\0\T_USER_1.log

2011-07-08 10:30:18 707(warning): Log file not exist, will create a new one

2011-07-08 10:30:18 738: T_USER loaded.

FyDB server is running ...

 

Notice: If external data source is not available, FyDB will raise exception when load dataset, but will not affect other dataset loading.

5.   Deployment:
Deploy as middle ware of Java programs;
 

Run as stand alone service;
 

6.   Architecture
Local files as data source
 

RDBMS as data source
 

Heterogeneous Data Source Integration
 

Distributed Architecture


7.   Client Console Commands
Local Commands
Help

help : show help information

 usage:

help [ Command]

 

Example:

FyDB> help

 

FyDB Console Help Information:

 H(elp) [Command]      : get help information of command;

 List @Commands        : Show all commands;

 E(xit)                : exit console;

 

Version

version : show version of console or FyDB server

 usage:

version [ {@server|@console} ]

 

Example:

FyDB>version

 

FyDB ConsoleAlpha 0.01

Copyright @2011 Fuyuncat. All rights reserved.

FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.

Email: fuyucat@gmail.com

WebSite: www.HelloDBA.com

 

List

list : list commands or datasets in server.

 usage:

list  {@datasets|@commands}

 

Example:

FyDB>list @commands

 

SHOW

GET

…

 

Set

set : set properites of console

 usage:

set [ {@timing|@time}  {on|off} ]

 

Example:

FyDB>set @time on

FyDB>help

2011-07-08 10:47:39 270

 

FyDB Console Help Information:

 H(elp) [Command]      : get help information of command;

 List @Commands        : Show all commands;

 E(xit)                : exit console;

 

Exit

exit : Exit console

 usage:

exit

 

Server & Data Access commands
Connecct

connect : connect to FyDB serve

 usage:

connect  $ServerNameOrIP  $port  $password

 

Example:

FyDB>conn 146.222.94.152 6636 fuyuncat@gmail.com

Connecting to server 146.222.94.152 ...

 

Connected to server successfully!

 

Disconnect

disconnect : Disconnect from server.

   usage:

disconnect

 

Version

version : show version of console or FyDB server

 usage:

version [ {@server|@console} ]

 

List

list : list commands or datasets in server.

 usage:

list  {@datasets|@commands}

 

Example:

FyDB>list @datasets

 

dataset name

--------------------------------------

T_TEST2

JDBC:ORACLE:THIN:@LOCALHOST:1521:ORA10G/DEMO/T_TEST2

T_USER

 

Totally 3 datasets loaded in server.

 

Get

get : get data value from dataset, with optional query criteria

 usage:

get  {@value|@metadata}  from  $DatasetName [ with  $Criteria ({colName1=data1 and colName2>data2...}) ]

 

Notice: quoters for String value is " instead of '

 

Example:

FyDB>get @value from t_test2 with {owner="DEMO" and table_name >= "T_T"}

 

FREELISTS       TABLE_LOCK      SKIP_CORRUPT    FREELIST_GROUPS SAMPLE_SIZE     DROPPED LAST_ANALYZED   IOT_TYPE        USER_STATS      TABLE_NAME

CACHE   PCT_INCREASE    COMPRESSION     BACKED_UP       CLUSTER_NAME    MIN_EXTENTS     DEGREE  MONITORING      LOGGING TABLESPACE_NAME MAX_EXTENTS

INSTANCES       BUFFER_POOL     NESTED  INI_TRANS       CHAIN_CNT       GLOBAL_STATS    TEMPORARY   PCT_USED    AVG_SPACE       MAX_TRANS       EMPTY_

BLOCKS  PARTITIONED     PCT_FREE        AVG_ROW_LEN     DURATION        IOT_NAME        NUM_ROWS    SECONDARY   NUM_FREELIST_BLOCKS     NEXT_EXTENT

ROW_MOVEMENT    CLUSTER_OWNER   BLOCKS  DEPENDENCIES    AVG_SPACE_FREELIST_BLOCKS       INITIAL_EXTENT  STATUS  OWNER

---------       ----------      ------------    --------------- -----------     ------- -------------   --------        ----------      ----------

-----   ------------    -----------     ---------       ------------    -----------     ------  ----------      ------- --------------- -----------

---------       -----------     ------  ---------       ---------       ------------    ---------   --------    ---------       ---------       ------

------  -----------     --------        -----------     --------        --------        --------    ---------   -------------------     -----------

------------    -------------   ------  ------------    -------------------------       --------------  ------  -----

null    ENABLED DISABLED        null    23      NO      2006-07-05 13:11:25.0   null    NO      T_USERS     N   null    DISABLED        N       null

1                1      YES     YES     EDGARDEMO       2147483645               1      DEFAULT NO  1   0       NO      N       null    5840    255

4       NO      1000    94      null    null    23      N       0       null    DISABLED        null    4       DISABLED        0       65536   VALID

DEMO

null    ENABLED DISABLED        null    5       NO      2006-04-03 11:56:56.0   null    NO      T_USERINFO          N   null    DISABLED        N

null    1                1      YES     YES     EDGARDEMO       2147483645               1      DEFAULT NO      1       0       YES     N       null

0       255     0       NO      1000    18      null    null    5       N       0       null    DISABLED        null    5       DISABLED        0

65536   VALID   DEMO

… …

 

18 datas found.

 

Add

add : add data into dataset

 usage:

add  $newDatas ({colName1:data1;colName2:data2...})  to  $DatasetName

 

Remove

remove : remove data from dataset, with optional query criteria

 usage:

remove  from  $DatasetName [ with  $Criteria ({colName1=data1 and colName2>data2...}) ]


Modify

modify : modiy data of dataset, with optional query criteria

 usage:

modify  $newDatas ({colName1:data1;colName2:data2...})  in  $DatasetName [ with  $Criteria ({colName1=data1 and colName2>data2...}) ]

 

Join

join : join data value from 2 datasets, query criteria

 usage:

join  @value  from  $DatasetName1  with  $searchCriteria1 ({colName1=data1 and colName2>data2...})  $DatasetName2  with  $searchCriteria2 ({colName1=d

ata1 and colName2>data2...})  {ON|LEFTON|RIGHTON|OUTON}  $joinCrriteria ({colName1_1=colName2_1 and colName1_2>colName2_2...})

 

Example:

FyDB>Join @value from JDBC:ORACLE:THIN:@LOCALHOST:1521:ORA10G/DEMO/T_TEST2 with {OWNER = "DEMO"} T_TEST2 with {last_analyzed >= "2006-9-22 22:00:09"} ON {OWNER=OWNER and table_name = table_name}

 

FREELISTS       TABLE_LOCK      SKIP_CORRUPT    FREELIST_GROUPS SAMPLE_SIZE     DROPPED LAST_ANALYZED   IOT_TYPE        USER_STATS      TABLE_NAME

CACHE   PCT_INCREASE    COMPRESSION     BACKED_UP       CLUSTER_NAME    MIN_EXTENTS     DEGREE  MONITORING      LOGGING TABLESPACE_NAME MAX_EXTENTS

INSTANCES       BUFFER_POOL     NESTED  INI_TRANS       CHAIN_CNT       GLOBAL_STATS    TEMPORARY   PCT_USED    AVG_SPACE       MAX_TRANS       EMPTY_

BLOCKS  PARTITIONED     PCT_FREE        AVG_ROW_LEN     DURATION        IOT_NAME        NUM_ROWS    SECONDARY   NUM_FREELIST_BLOCKS     NEXT_EXTENT

ROW_MOVEMENT    CLUSTER_OWNER   BLOCKS  DEPENDENCIES    AVG_SPACE_FREELIST_BLOCKS       INITIAL_EXTENT  STATUS  OWNER   FREELISTS       TABLE_LOCK

SKIP_CORRUPT    FREELIST_GROUPS SAMPLE_SIZE     DROPPED LAST_ANALYZED   IOT_TYPE        USER_STATS  TABLE_NAME  CACHE   PCT_INCREASE    COMPRESSION

BACKED_UP       CLUSTER_NAME    MIN_EXTENTS     DEGREE  MONITORING      LOGGING TABLESPACE_NAME MAX_EXTENTS     INSTANCES       BUFFER_POOL     NESTED

        INI_TRANS       CHAIN_CNT       GLOBAL_STATS    TEMPORARY       PCT_USED        AVG_SPACE   MAX_TRANS   EMPTY_BLOCKS    PARTITIONED     PCT_FR

EE      AVG_ROW_LEN     DURATION        IOT_NAME        NUM_ROWS        SECONDARY       NUM_FREELIST_BLOCKS     NEXT_EXTENT     ROW_MOVEMENT    CLUSTE

R_OWNER BLOCKS  DEPENDENCIES    AVG_SPACE_FREELIST_BLOCKS       INITIAL_EXTENT  STATUS  OWNER

---------       ----------      ------------    --------------- -----------     ------- -------------   --------        ----------      ----------

-----   ------------    -----------     ---------       ------------    -----------     ------  ----------      ------- --------------- -----------

---------       -----------     ------  ---------       ---------       ------------    ---------   --------    ---------       ---------       ------

------  -----------     --------        -----------     --------        --------        --------    ---------   -------------------     -----------

------------    -------------   ------  ------------    -------------------------       --------------  ------  -----   ---------       ----------

------------    --------------- -----------     ------- -------------   --------        ----------  ----------  -----   ------------    -----------

---------       ------------    -----------     ------  ----------      ------- --------------- -----------     ---------       -----------     ------

        ---------       ---------       ------------    ---------       --------        ---------   ---------   ------------    -----------     ------

--      -----------     --------        --------        --------        ---------       -------------------     -----------     ------------    ------

------- ------  ------------    -------------------------       --------------  ------  -----

null    ENABLED DISABLED        null    1       NO      2006-09-22 22:00:09.0   null    NO      QMS_USER_OPTIONS_CMP        N   null    ENABLED N

null    1                1      YES     NO      RING    2147483645               1      DEFAULT NO  1   0       YES     N       null    0       255

0       NO      0       155     null    null    1       N       0       null    DISABLED        null    4       DISABLED        0       65536   VALID

DEMO    null    ENABLED DISABLED        null    1       NO      2006-09-22 22:00:09.0   null    NO  QMS_USER_OPTIONS_CMP            N   null    ENABLE

D       N       null    1                1      YES     NO      RING    2147483645               1  DEFAULT     NO      1       0       YES     N

null    0       255     0       NO      1000    155     null    null    1       N       0       null    DISABLED        null    4       DISABLED

0       65536   VALID   DEMO

… …

 

36 datas found.

 

Show

show : show last error info

 usage:

show [ @error ]

 

Example:
FyDB>get @metadata from t_t

FyDB>show @error

 

specified dataset name is not found
