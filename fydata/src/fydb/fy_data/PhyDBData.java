/**
 * @(#)PhyDBData.java	0.01 11/04/19
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_data;

import fydb.fy_comm.Consts;
import fydb.fy_comm.Debuger;
import fydb.fy_comm.FyDataEntry;
import fydb.fy_comm.Tracer;
import fydb.fy_comm.FyMetaData;

//import fydb.fy_main.BP;

import java.math.BigDecimal;

import java.sql.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class PhyDBData implements PhyBaseData{
    private Tracer dtrace; // = new Tracer();
    private Debuger debuger;// = new Debuger();
    private FyMetaData metaData; 

    protected Connection dbconn;
    protected boolean phyinited;
    protected String tableName;
    protected ArrayList keyColumns;
    protected long size;
    protected ResultSet rset;

    private String insertStatement; // a insert statement generated in initialization phase for insert operation
    private String deleteStatement; // a delete statement generated in initialization phase for insert operation
    private String where = "";
    
    public PhyDBData(Tracer dtrace, Debuger debuger) {
        this.dtrace = dtrace;
        this.debuger = debuger;
        phyinited = false;
    }
    
    private String readToString(ResultSet rest, int colIndex, int colType){
        String value = new String();
        try{
            switch (colType){
            case Types.BIT:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:{
                    Integer oriVal = Integer.valueOf(rest.getInt(colIndex));
                    value = oriVal==null?null:value.valueOf(oriVal);
                }
                break;
            case Types.BIGINT:{
                    Long oriVal = Long.valueOf(rest.getLong(colIndex));
                    value = oriVal==null?null:value.valueOf(oriVal);
                }
                break;
            case Types.FLOAT:
            case Types.REAL:{
                    Double oriVal = Double.valueOf(rest.getDouble(colIndex));
                    value = oriVal==null?null:value.valueOf(oriVal);
                }
                break;
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:{
                    //BigDecimal oriVal = BigDecimal.valueOf(rest.getBigDecimal(colIndex));
                    BigDecimal oriVal = rest.getBigDecimal(colIndex);
                    value = oriVal==null?null:value.valueOf(oriVal);
                }
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
                value = rest.getString(colIndex);
                break;
            case Types.DATE:
                /*Date d = rest.getDate(colIndex);
                if (d != null)
                    value = d.toString();
                else
                    value = "";
                break;*/
            case Types.TIME:
                /*Time t = rest.getTime(colIndex);
                if (t != null)
                    value = t.toString();
                else
                    value = "";
                break;*/
            case Types.TIMESTAMP:{
                    Timestamp oriVal = rest.getTimestamp(colIndex);
                    value = oriVal==null?null:oriVal.toString();
                }
                break;
            case Types.LONGVARBINARY:
            case Types.BINARY:
            case Types.VARBINARY:{
                    byte[] oriVal = rest.getBytes(colIndex);
                    value = oriVal==null?null:value.valueOf(oriVal);
                }
                break;
            case Types.BOOLEAN:{
                    Boolean oriVal = rest.getBoolean(colIndex);
                    value = oriVal==null?null:value.valueOf(oriVal);
                }
                break;
            case Types.NULL:
                value = null;
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.ARRAY:
            case Types.BLOB:
            case Types.REF:
            case Types.DATALINK:
            default:
                dtrace.trace(504);
                return null;
            }
        }catch (SQLException e){
            //e.printStackTrace();
            dtrace.trace(502);
            //dtrace.trace(e.getMessage());
            return null;
        }
        return value;
    }
    
    private int colTypeMap(int colType){
        int mappedType = Consts.UNKNOWN;
        switch (colType)
        {
        case Types.BIT:
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
            //typeName = "Integer";
            mappedType = Consts.INTEGER;
            break;
        case Types.BIGINT:
            //mappedType = "long";
            mappedType = Consts.LONG;
            break;
        case Types.FLOAT:
        case Types.REAL:
        case Types.DOUBLE:
        case Types.NUMERIC:
        case Types.DECIMAL:
            //mappedType = "double";
            mappedType = Consts.DOUBLE;
            break;
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.CLOB:
            //mappedType = "String";
            mappedType = Consts.STRING;
            break;
        case Types.DATE:
        case Types.TIME:
            //mappedType = "Date";
            mappedType = Consts.DATE;
            break;
        case Types.TIMESTAMP:
            //mappedType = "Timestamp";
            mappedType = Consts.TIMESTAMP;
            break;
        case Types.LONGVARBINARY:
        case Types.BINARY:
        case Types.VARBINARY:
            //mappedType = "String";
            mappedType = Consts.STRING;
            break;
        case Types.BOOLEAN:
            //mappedType = "boolean";
            mappedType = Consts.BOOLEAN;
            break;
        case Types.NULL:
            //mappedType = "";
            mappedType = Consts.NULL;
        case Types.OTHER:
        case Types.JAVA_OBJECT:
        case Types.DISTINCT:
        case Types.STRUCT:
        case Types.ARRAY:
        case Types.BLOB:
        case Types.REF:
        case Types.DATALINK:
        default:
            dtrace.trace(504);
            mappedType = Consts.UNKNOWN;
        }

        return mappedType;
    }
    
    private boolean dataValidate(HashMap data){
        Iterator it = data.keySet().iterator();
        while (it.hasNext()) {
            Integer colID = (Integer)it.next();
            String strVal = (String)data.get(colID);
            if (strVal == null) // null is suitable for all data type
                continue;
            HashMap colProperties = (HashMap)metaData.getColumns().get(colID);
            int type = ((Integer)colProperties.get("T")).intValue(); 
            if (type == Consts.LONG){
                try{
                    Long.parseLong(strVal);
                }catch (NumberFormatException e){
                    dtrace.trace((String)colProperties.get("N")+":"+strVal);
                    dtrace.trace(223);
                    return false;
                }
            }else if (type == Consts.INTEGER){
                try{
                    Integer.parseInt(strVal);
                }catch (NumberFormatException e){
                    dtrace.trace((String)colProperties.get("N")+":"+strVal);
                    dtrace.trace(251);
                    return false;
                }
            }else if (type == Consts.DOUBLE){
                try{
                    Double.parseDouble(strVal);
                }catch (NumberFormatException e){
                    dtrace.trace((String)colProperties.get("N")+":"+strVal);
                    dtrace.trace(224);
                    return false;
                }
            }else if (type == Consts.DATE || type == Consts.TIMESTAMP){
                try{
                    Timestamp.valueOf(strVal);
                }catch (IllegalArgumentException e){
                    dtrace.trace((String)colProperties.get("N")+":"+strVal);
                    dtrace.trace(225);
                    return false;
                }
            }else if (type == Consts.BOOLEAN){
                try{
                    Boolean.parseBoolean(strVal);
                }catch (Exception e){
                    dtrace.trace((String)colProperties.get("N")+":"+strVal);
                    dtrace.trace(226);
                    if (debuger.isDebugMode())
                        e.printStackTrace();
                    return false;
                }
            }else if (type == Consts.STRING){
                continue;
            }else {
                dtrace.trace((String)colProperties.get("N")+"("+type+"):"+strVal);
                dtrace.trace(227);
                return false;
            }
        }
        return true;
    }

    private boolean dataValidate(FyDataEntry row){
        if (metaData.getColumns()==null) { // require column properties
            dtrace.trace(222);
            return false;
        }

        // validate data
        if (row.key != null && dataValidate(row.key) && row.value != null && dataValidate(row.value))
            return true;
        else
            return false;
    }

    // test a column is a key column or not
    //private boolean isKeyColumn(Integer colID){
    //    HashMap colProperties = (HashMap)metaData.getColumns().get(colID);
    //    if (colProperties == null){
    //        dtrace.trace(222);
    //        return false;
    //    }
    //    if (((Integer)colProperties.get("K")).intValue() >= 0) // the column set contains a key column
    //        return true;
    //    else
    //        return false;
    //}
    
    public FyMetaData init(){
        return null;
    }
    
    // build a insert SQL statement
    private String buildInsertStatement(){
        if (metaData == null || metaData.getColumns() == null)
        {
            dtrace.trace(222);
            return null;
        }
        String stmt = new String();
        int columnNum = 0;;
        stmt = "insert into "+tableName+" ( ";
        // to align with the sequence of bind variable, we should not iterate value but key
        Iterator it = metaData.getColumns().keySet().iterator();
        // build insert columns (value) clause
        while (it.hasNext()) {
            Integer colID = (Integer)it.next();
            HashMap colProperties = (HashMap)metaData.getColumns().get(colID);
            if (colProperties == null)
                return null;
            stmt += (columnNum==0?"":", ")+(String)colProperties.get("N");
            columnNum++;
        }

        // build the values clause
        stmt += " ) values ( ";
        for (int i=0; i<columnNum; i++){
            stmt += (i>0?", ?":" ?");
        }
        stmt += " )";
        return stmt;
    }
    
    // build a delete SQL statement
    private String buildDeleteStatement(){
        if (metaData == null || metaData.getColumns() == null)
        {
            dtrace.trace(222);
            return null;
        }
        String stmt = new String();
        int columnNum = 0;;
        stmt = "delete from "+tableName+" where (";
        // to align with the sequence of bind variable, we should not iterate value but key
        Iterator it = metaData.getKeyColumns().iterator();
        // build delete where (key as filter) clause 
        while (it.hasNext()) {
            Integer colID = (Integer)it.next();
            HashMap colProperties = (HashMap)metaData.getColumns().get(colID);
            if (colProperties == null)
            {
                dtrace.trace(222);
                return null;
            }
            if (((Integer)colProperties.get("K")).intValue() >= 0){
                // stmt += (columnNum==0?"":" and ")+(String)colProperties.get("N")+"=?";
                // allow key column be null, we should generate a compitable where clause for null judgement.
                // (OBJECT_ID = ? or (? is null and OBJECT_ID is null))
                // notice: we should bind variable twice here.
                stmt += (columnNum==0?"(":" and (")+(String)colProperties.get("N")+"=? or (? is null and "+(String)colProperties.get("N")+" is null))";
                columnNum++;
            }
        }
        stmt += ")" + (where!=null&&where.trim().length()>0?" and "+where.trim():"");

        return stmt;
    }
    
    public FyMetaData init(String fileDir, String fileName, int workMode){return null;} // for file initialize

    public FyMetaData init(Connection dbconn, String tableName, ArrayList keyColNames, String where){
        if (phyinited)
            return null;
        this.tableName = tableName;
        this.keyColumns = keyColNames;
        this.where = where;
        try{
            Statement stmt = dbconn.createStatement(); 
            rset = stmt.executeQuery ("select * from "+tableName + (where!=null&&where.trim().length()>0?" where "+where.trim():""));
            ResultSetMetaData reMeta = rset.getMetaData();
            ArrayList columnNames = new ArrayList();
            HashMap columns = new HashMap();
            for (int i=1; i<=reMeta.getColumnCount(); i++){
                Map colProperties = new HashMap();
                colProperties.clear();
                colProperties.put("N",reMeta.getColumnName(i)); // name
                colProperties.put("T", new Integer(colTypeMap(reMeta.getColumnType(i)))); // data type
                colProperties.put("K", new Integer(keyColNames.indexOf(reMeta.getColumnName(i)))); // -1 means it is not a key, >=0 is key column 
                colProperties.put("NL", new Integer(reMeta.isNullable(i))); // 0 means not allow null, 1 allow null
                columns.put(new Integer(i-1) ,colProperties);
                columnNames.add(reMeta.getColumnName(i));
            }
            if (!columnNames.containsAll(keyColNames)){
                dtrace.trace(503);
                return null;
            }
            this.metaData = new FyMetaData(dtrace, tableName, 0, columns, keyColNames);
            this.insertStatement = buildInsertStatement();
            this.deleteStatement = buildDeleteStatement();
            this.dbconn = dbconn;
        }catch (SQLException e){
            //e.printStackTrace();
            dtrace.trace(501);
            //dtrace.trace(e.getMessage());
            return null;
        }

        phyinited = true;
        return metaData;
    }

    public FyDataEntry next(BP bp){
        if (!phyinited)
            return null;

        try{
            if (rset.next()){
                ResultSetMetaData reMeta = rset.getMetaData();
                HashMap key = new HashMap();
                HashMap value = new HashMap();
                for (int i=1; i<=reMeta.getColumnCount(); i++)
                {
                    String strVal = readToString(rset, i, reMeta.getColumnType(i));
                    if (strVal == null) // to save space, null values will not be stored
                        continue;
                    if (keyColumns.contains(reMeta.getColumnName(i)))
                        key.put(new Integer(i-1), strVal);
                    else
                        value.put(new Integer(i-1), strVal);
                }
                //if (bp == 0)
                //    bp = beeper.getBP();
                FyDataEntry row = new FyDataEntry(key, value);
                return row;
            }
            else
                return null;
        }catch (SQLException e){
            //e.printStackTrace();
            dtrace.trace(502);
            //dtrace.trace(e.getMessage());
            return null;
        }
    }
    
    // generate a PreparedStatement for insert
    private boolean bindInsertValues(PreparedStatement ps, HashMap key, HashMap value){
        try{// build a insert SQL, bind variables, then execute
            int varID = 1;
            Iterator it = metaData.getColumns().keySet().iterator();
            // build insert columns (value) clause
            while (it.hasNext()) {
                Integer colID = (Integer)it.next();
                HashMap colProperties = (HashMap)metaData.getColumns().get(colID);
                if (colProperties == null)
                    return false;
                int type = ((Integer)colProperties.get("T")).intValue(); 
                String strVal = (String)key.get(colID);
                strVal = strVal==null?(String)value.get(colID):strVal;
                //if (strVal == null)
                //    return null;
                if (type == Consts.LONG){
                    try{
                        if (strVal==null) 
                            ps.setNull(varID, Types.NULL);
                        else
                            ps.setLong(varID, Long.parseLong(strVal));
                    }catch (NumberFormatException e){
                        dtrace.trace((String)colProperties.get("N")+":"+strVal);
                        dtrace.trace(223);
                        return false;
                    }
                }else if (type == Consts.INTEGER){
                    try{
                        if (strVal==null) 
                            ps.setNull(varID, Types.NULL);
                        else
                            ps.setInt(varID, Integer.parseInt(strVal));
                    }catch (NumberFormatException e){
                        dtrace.trace((String)colProperties.get("N")+":"+strVal);
                        dtrace.trace(251);
                        return false;
                    }
                }else if (type == Consts.DOUBLE){
                    try{
                        if (strVal==null) 
                            ps.setNull(varID, Types.NULL);
                        else
                            ps.setDouble(varID, Double.parseDouble(strVal));
                    }catch (NumberFormatException e){
                        dtrace.trace((String)colProperties.get("N")+":"+strVal);
                        dtrace.trace(224);
                        return false;
                    }
                }else if (type == Consts.STRING){
                    try{
                        if (strVal==null) 
                            ps.setNull(varID, Types.NULL);
                        else
                            ps.setString(varID, strVal);
                    }catch (NumberFormatException e){
                        dtrace.trace((String)colProperties.get("N")+":"+strVal);
                        dtrace.trace(252);
                        return false;
                    }
                }else if (type == Consts.DATE){
                    try{
                        if (strVal==null) 
                            ps.setNull(varID, Types.NULL);
                        else
                            ps.setTimestamp(varID, Timestamp.valueOf(strVal));
                    }catch (IllegalArgumentException e){
                        dtrace.trace((String)colProperties.get("N")+":"+strVal);
                        dtrace.trace(225);
                        return false;
                    }
                }else if (type == Consts.TIMESTAMP){
                    try{
                        if (strVal==null) 
                            ps.setNull(varID, Types.NULL);
                        else
                            ps.setTimestamp(varID, Timestamp.valueOf(strVal));
                    }catch (IllegalArgumentException e){
                        dtrace.trace((String)colProperties.get("N")+":"+strVal);
                        dtrace.trace(225);
                        return false;
                    }
                }else if (type == Consts.BOOLEAN){
                    try{
                        if (strVal==null) 
                            ps.setNull(varID, Types.NULL);
                        else
                            ps.setBoolean(varID, Boolean.parseBoolean(strVal));
                    }catch (Exception e){
                        dtrace.trace((String)colProperties.get("N")+":"+strVal);
                        dtrace.trace(226);
                        if (debuger.isDebugMode())
                            e.printStackTrace();
                        return false;
                    }
                }else {
                    dtrace.trace((String)colProperties.get("N")+"("+type+"):"+strVal);
                    dtrace.trace(227);
                    return false;
                }
                varID++;
            }
            ps.addBatch();
        }catch(SQLException e)
        {
            dtrace.trace(insertStatement);
            dtrace.trace(506);
            return false;
        }
        return true;
    }

    // insert data to data source (db)
    public boolean insertData(HashMap key, HashMap value){
        if (key == null || key.size() == 0){
            dtrace.trace(231);
            return false;
        }

        try{// build a insert SQL, bind variables, then execute
            if (insertStatement == null)
            {
                dtrace.trace(234);
                return false;
            }
            PreparedStatement ps = this.dbconn.prepareStatement(insertStatement);
            if (!bindInsertValues(ps, key, value))
                return false;
            ps.executeBatch();
        }catch(SQLException e)
        {
            dtrace.trace(insertStatement);
            dtrace.trace(506);
            return false;
        }
        return true;
    }

    // implement a insert data log entry  (op = INSERT)
    private boolean insertData(FyDataLogEntry.LogContent logData){
        if (logData == null)
            return true;
        if (logData.op != Consts.INSERT) // wrong operation
            return false;
        //if (log.logData.value == null || log.logData.value.size() == 0){
        //    dtrace.trace(230);
        //    return false;
        //}
        if (logData.key == null || logData.key.size() == 0){
            dtrace.trace(231);
            return false;
        }

        return insertData(logData.key, logData.value);
    }
    
    // batch insert data to data source (db), input a Array of FyDataEntry, return fails number
    public int insertData(ArrayList datas, boolean ignoreFails){
        int fails = 0;
        if (datas == null || datas.size() == 0){
            return fails;
        }
        
        // batch execute insert
        try{
            if (insertStatement == null)
            {
                dtrace.trace(234);
                return fails;
            }
            PreparedStatement ps = this.dbconn.prepareStatement(insertStatement);
            for (int i=0; i<datas.size(); i++){
                FyDataEntry data = (FyDataEntry)datas.get(i);
                if (!bindInsertValues(ps, data.key, data.value)){
                    fails++;
                    if (!ignoreFails)
                        return fails;
                }
            }
            ps.executeBatch();
       }catch(SQLException e)
        {
            dtrace.trace(insertStatement);
            dtrace.trace(506);
        }
 
        return fails;
    }

    // generate a PreparedStatement for insert
    private boolean bindDeleteValues(PreparedStatement ps, HashMap key){
        try{// build a insert SQL, bind variables, then execute
            int varID = 1;
            Iterator it = metaData.getKeyColumns().iterator();
            // build insert columns (value) clause
            while (it.hasNext()) {
                Integer colID = (Integer)it.next();
                HashMap colProperties = (HashMap)metaData.getColumns().get(colID);
                if (colProperties == null)
                    return false;
                if (((Integer)colProperties.get("K")).intValue() >= 0){
                    int type = ((Integer)colProperties.get("T")).intValue(); 
                    String strKey = (String)key.get(colID);
                    //if (strVal == null)
                    //    return null;
                    if (type == Consts.LONG){
                        try{
                            if (strKey==null) {
                                ps.setNull(varID, Types.NULL);
                                ps.setNull(varID+1, Types.NULL);
                            }else{
                                ps.setLong(varID, Long.parseLong(strKey));
                                ps.setLong(varID+1, Long.parseLong(strKey));
                            }
                        }catch (NumberFormatException e){
                            dtrace.trace((String)colProperties.get("N")+":"+strKey);
                            dtrace.trace(223);
                            return false;
                        }
                    }else if (type == Consts.DOUBLE){
                        try{
                            if (strKey==null) {
                                ps.setNull(varID, Types.NULL);
                                ps.setNull(varID+1, Types.NULL);
                            }else{
                                ps.setDouble(varID, Double.parseDouble(strKey));
                                ps.setDouble(varID+1, Double.parseDouble(strKey));
                            }
                        }catch (NumberFormatException e){
                            dtrace.trace((String)colProperties.get("N")+":"+strKey);
                            dtrace.trace(224);
                            return false;
                        }
                    }else if (type == Consts.INTEGER){
                        try{
                            if (strKey==null) {
                                ps.setNull(varID, Types.NULL);
                                ps.setNull(varID+1, Types.NULL);
                            }else{
                                ps.setInt(varID, Integer.parseInt(strKey));
                                ps.setInt(varID+1, Integer.parseInt(strKey));
                            }
                        }catch (NumberFormatException e){
                            dtrace.trace((String)colProperties.get("N")+":"+strKey);
                            dtrace.trace(251);
                            return false;
                        }
                    }else if (type == Consts.STRING){
                        try{
                            if (strKey==null) {
                                ps.setNull(varID, Types.NULL);
                                ps.setNull(varID+1, Types.NULL);
                            }else{
                                ps.setString(varID, strKey);
                                ps.setString(varID+1, strKey);
                            }
                        }catch (NumberFormatException e){
                            dtrace.trace((String)colProperties.get("N")+":"+strKey);
                            dtrace.trace(252);
                            return false;
                        }
                    }else if (type == Consts.DATE){
                        try{
                            if (strKey==null) {
                                ps.setNull(varID, Types.NULL);
                                ps.setNull(varID+1, Types.NULL);
                            }else{
                                ps.setTimestamp(varID, Timestamp.valueOf(strKey));
                                ps.setTimestamp(varID+1, Timestamp.valueOf(strKey));
                            }
                        }catch (IllegalArgumentException e){
                            dtrace.trace((String)colProperties.get("N")+":"+strKey);
                            dtrace.trace(225);
                            return false;
                        }
                    }else if (type == Consts.TIMESTAMP){
                        try{
                            if (strKey==null) {
                                ps.setNull(varID, Types.NULL);
                                ps.setNull(varID+1, Types.NULL);
                            }else{
                                ps.setTimestamp(varID, Timestamp.valueOf(strKey));
                                ps.setTimestamp(varID+1, Timestamp.valueOf(strKey));
                            }
                        }catch (IllegalArgumentException e){
                            dtrace.trace((String)colProperties.get("N")+":"+strKey);
                            dtrace.trace(225);
                            return false;
                        }
                    }else if (type == Consts.BOOLEAN){
                        try{
                            if (strKey==null) {
                                ps.setNull(varID, Types.NULL);
                                ps.setNull(varID+1, Types.NULL);
                            }else{
                                ps.setBoolean(varID, Boolean.parseBoolean(strKey));
                                ps.setBoolean(varID+1, Boolean.parseBoolean(strKey));
                            }
                        }catch (Exception e){
                            dtrace.trace((String)colProperties.get("N")+":"+strKey);
                            dtrace.trace(226);
                            if (debuger.isDebugMode())
                                e.printStackTrace();
                            return false;
                        }
                    }else {
                        dtrace.trace((String)colProperties.get("N")+"("+type+"):"+strKey);
                        dtrace.trace(227);
                        return false;
                    }
                    //varID++;
                    varID+=2; // to compitable for binding null value, we should bind data twice
                }
            }
            ps.addBatch();
        }catch(SQLException e)
        {
            dtrace.trace(deleteStatement);
            dtrace.trace(506);
            return false;
        }
        return true;
    }

    // implement a delete data log entry  (op = 3)
    public boolean deleteData(HashMap key){
        if (key == null || key.size() == 0){
            dtrace.trace(231);
            return false;
        }

        try{// build a delete SQL, bind variables, then execute
            if (deleteStatement == null)
            {
                dtrace.trace(235);
                return false;
            }
            PreparedStatement ps = this.dbconn.prepareStatement(deleteStatement);
            if (!bindDeleteValues(ps, key))
                return false;
            ps.executeBatch();
        }catch(SQLException e)
        {
            dtrace.trace(deleteStatement);
            dtrace.trace(506);
            return false;
        }
        return true;
    }
    
    // implement a delete data log entry  (op = DELETE)
    private boolean deleteData(FyDataLogEntry.LogContent logData){
        if (logData == null)
            return true;
        if (logData.op != Consts.DELETE) // wrong operation
            return false;
        //if (log.logData.value == null || log.logData.value.size() == 0){
        //    dtrace.trace(230);
        //    return false;
        //}
        if (logData.key == null || logData.key.size() == 0){
            dtrace.trace(231);
            return false;
        }

        return deleteData(logData.key);
    }
    
    // batch delete data to data source (db), input a Array of key(HashMap), return fails number
    public int deleteData(ArrayList datas, boolean ignoreFails){
        int fails = 0;
        if (datas == null || datas.size() == 0){
            return fails;
        }
        
        // batch execute insert
        try{
            if (deleteStatement == null)
            {
                dtrace.trace(234);
                return fails;
            }
            PreparedStatement ps = this.dbconn.prepareStatement(deleteStatement);
            for (int i=0; i<datas.size(); i++){
                HashMap key = (HashMap)datas.get(i);
                if (!bindDeleteValues(ps, key)){
                    fails++;
                    if (!ignoreFails)
                        return fails;
                }
            }
            ps.executeBatch();
       }catch(SQLException e)
        {
            dtrace.trace(deleteStatement);
            dtrace.trace(506);
        }
    
        return fails;
    }

    // implement a data change log entry  (op = MODIFY)
    private boolean updateData(FyDataLogEntry.LogContent logData){
        if (logData == null)
            return true;
        if (logData.op != Consts.MODIFY) // wrong operation
            return false;
        if (logData.value == null || logData.value.size() == 0){
            dtrace.trace(228);
            return false;
        }
        if (logData.key == null || logData.key.size() == 0){
            dtrace.trace(229);
            return false;
        }

        String stmt = new String();
        try{// build a uodate SQL, bind variables, then execute
            stmt = "update "+tableName+" set ";

            // build update columns clause
            Iterator it = logData.value.keySet().iterator();
            boolean firstUpdateColumn = true;
            while (it.hasNext()) {
                Integer colID = (Integer)it.next();
                FyDataLogEntry.valueChangeEntry valChange = (FyDataLogEntry.valueChangeEntry)logData.value.get(colID);
                if (valChange == null) // null is suitable for all data type
                    continue;
                HashMap colProperties = (HashMap)metaData.getColumns().get(colID);
                if (colProperties == null)
                    continue;
                stmt += (firstUpdateColumn?"":",")+(String)colProperties.get("N") + "=?";
                firstUpdateColumn = false;
            }
            // build where clause, key is filter
            stmt += " where ";
            it = logData.key.keySet().iterator();
            firstUpdateColumn = true;
            while (it.hasNext()) {
                Integer colID = (Integer)it.next();
                FyDataLogEntry.valueChangeEntry keyData = (FyDataLogEntry.valueChangeEntry)logData.key.get(colID);
                //if (keyData == null) // null is suitable for all data type
                //    continue;
                HashMap colProperties = (HashMap)metaData.getColumns().get(colID);
                if (colProperties == null)
                    continue;
                //stmt += (firstUpdateColumn?"":" and ")+(String)colProperties.get("N") + "=?";
                // allow key column be null, we should generate a compitable where clause for null judgement.
                // (OBJECT_ID = ? or (? is null and OBJECT_ID is null))
                // notice: we should bind variable twice here.
                stmt += (firstUpdateColumn?"(":" and (")+(String)colProperties.get("N")+"=? or (? is null and "+(String)colProperties.get("N")+" is null))";
                firstUpdateColumn = false;
            }
            PreparedStatement ps = this.dbconn.prepareStatement(stmt);

            // bind variables in update columns clause, will also detect the data type compatibility
            int varID = 1;
            it = logData.value.keySet().iterator();
            while (it.hasNext()) {
                Integer colID = (Integer)it.next();
                FyDataLogEntry.valueChangeEntry valChange = (FyDataLogEntry.valueChangeEntry)logData.value.get(colID);
                if (valChange == null) // null is suitable for all data type
                    continue;
                HashMap colProperties = (HashMap)metaData.getColumns().get(colID);
                if (colProperties == null)
                    continue;
                int type = ((Integer)colProperties.get("T")).intValue(); 
                if (type == Consts.LONG){
                    try{
                        if (valChange.newValue == null)
                            ps.setNull(varID, Types.NULL);
                        else
                            ps.setLong(varID, Long.parseLong(valChange.newValue));
                    }catch (NumberFormatException e){
                        dtrace.trace((String)colProperties.get("N")+":"+valChange.newValue);
                        dtrace.trace(223);
                        return false;
                    }
                }else if (type == Consts.INTEGER){
                    try{
                        if (valChange.newValue == null)
                            ps.setNull(varID, Types.NULL);
                        else
                            ps.setInt(varID, Integer.parseInt(valChange.newValue));
                    }catch (NumberFormatException e){
                        dtrace.trace((String)colProperties.get("N")+":"+valChange.newValue);
                        dtrace.trace(251);
                        return false;
                    }
                }else if (type == Consts.DOUBLE){
                    try{
                        if (valChange.newValue == null)
                            ps.setNull(varID, Types.NULL);
                        else
                            ps.setDouble(varID, Double.parseDouble(valChange.newValue));
                    }catch (NumberFormatException e){
                        dtrace.trace((String)colProperties.get("N")+":"+valChange.newValue);
                        dtrace.trace(224);
                        return false;
                    }
                }else if (type == Consts.STRING){
                    try{
                        if (valChange.newValue == null)
                            ps.setNull(varID, Types.NULL);
                        else
                            ps.setString(varID, valChange.newValue);
                    }catch (NumberFormatException e){
                        dtrace.trace((String)colProperties.get("N")+":"+valChange.newValue);
                        dtrace.trace(252);
                        return false;
                    }
                }else if (type == Consts.DATE){
                    try{
                        if (valChange.newValue == null)
                            ps.setNull(varID, Types.NULL);
                        else
                            ps.setTimestamp(varID, Timestamp.valueOf(valChange.newValue));
                    }catch (IllegalArgumentException e){
                        dtrace.trace((String)colProperties.get("N")+":"+valChange.newValue);
                        dtrace.trace(225);
                        return false;
                    }
                }else if (type == Consts.TIMESTAMP){
                    try{
                        if (valChange.newValue == null)
                            ps.setNull(varID, Types.NULL);
                        else
                            ps.setTimestamp(varID, Timestamp.valueOf(valChange.newValue));
                    }catch (IllegalArgumentException e){
                        dtrace.trace((String)colProperties.get("N")+":"+valChange.newValue);
                        dtrace.trace(225);
                        return false;
                    }
                }else if (type == Consts.BOOLEAN){
                    try{
                        if (valChange.newValue == null)
                            ps.setNull(varID, Types.NULL);
                        else
                            ps.setBoolean(varID, Boolean.parseBoolean(valChange.newValue));
                    }catch (Exception e){
                        dtrace.trace((String)colProperties.get("N")+":"+valChange.newValue);
                        dtrace.trace(226);
                        if (debuger.isDebugMode())
                            e.printStackTrace();
                        return false;
                    }
                }else {
                    dtrace.trace((String)colProperties.get("N")+"("+type+"):"+valChange.newValue);
                    dtrace.trace(227);
                    return false;
                }
                varID++;
            }
            // bind variables in where clause, will also detect the data type compatibility
            it = logData.key.keySet().iterator();
            while (it.hasNext()) {
                Integer colID = (Integer)it.next();
                String strKey = (String)logData.key.get(colID);
                //if (strKey == null) // null is suitable for all data type
                //    continue;
                HashMap colProperties = (HashMap)metaData.getColumns().get(colID);
                if (colProperties == null)
                    continue;
                int type = ((Integer)colProperties.get("T")).intValue(); 
                if (type == Consts.LONG){
                    try{
                        if (strKey==null) {
                            ps.setNull(varID, Types.NULL);
                            ps.setNull(varID+1, Types.NULL);
                        }else{
                            ps.setLong(varID, Long.parseLong(strKey));
                            ps.setLong(varID+1, Long.parseLong(strKey));
                        }
                    }catch (NumberFormatException e){
                        dtrace.trace((String)colProperties.get("N")+":"+strKey);
                        dtrace.trace(223);
                        return false;
                    }
                }else if (type == Consts.INTEGER){
                    try{
                        if (strKey==null) {
                            ps.setNull(varID, Types.NULL);
                            ps.setNull(varID+1, Types.NULL);
                        }else{
                            ps.setInt(varID, Integer.parseInt(strKey));
                            ps.setInt(varID+1, Integer.parseInt(strKey));
                        }
                    }catch (NumberFormatException e){
                        dtrace.trace((String)colProperties.get("N")+":"+strKey);
                        dtrace.trace(251);
                        return false;
                    }
                }else if (type == Consts.DOUBLE){
                    try{
                        if (strKey==null) {
                            ps.setNull(varID, Types.NULL);
                            ps.setNull(varID+1, Types.NULL);
                        }else{
                            ps.setDouble(varID, Double.parseDouble(strKey));
                            ps.setDouble(varID+1, Double.parseDouble(strKey));
                        }
                    }catch (NumberFormatException e){
                        dtrace.trace((String)colProperties.get("N")+":"+strKey);
                        dtrace.trace(224);
                        return false;
                    }
                }else if (type == Consts.STRING){
                    try{
                        if (strKey==null) {
                            ps.setNull(varID, Types.NULL);
                            ps.setNull(varID+1, Types.NULL);
                        }else{
                            ps.setString(varID, strKey);
                            ps.setString(varID+1, strKey);
                        }
                    }catch (NumberFormatException e){
                        dtrace.trace((String)colProperties.get("N")+":"+strKey);
                        dtrace.trace(252);
                        return false;
                    }
                }else if (type == Consts.DATE){
                    try{
                        if (strKey==null) {
                            ps.setNull(varID, Types.NULL);
                            ps.setNull(varID+1, Types.NULL);
                        }else{
                            ps.setTimestamp(varID, Timestamp.valueOf(strKey));
                            ps.setTimestamp(varID+1, Timestamp.valueOf(strKey));
                        }
                    }catch (IllegalArgumentException e){
                        dtrace.trace((String)colProperties.get("N")+":"+strKey);
                        dtrace.trace(225);
                        return false;
                    }
                }else if (type == Consts.TIMESTAMP){
                    try{
                        if (strKey==null) {
                            ps.setNull(varID, Types.NULL);
                            ps.setNull(varID+1, Types.NULL);
                        }else{
                            ps.setTimestamp(varID, Timestamp.valueOf(strKey));
                            ps.setTimestamp(varID+1, Timestamp.valueOf(strKey));
                        }
                    }catch (IllegalArgumentException e){
                        dtrace.trace((String)colProperties.get("N")+":"+strKey);
                        dtrace.trace(225);
                        return false;
                    }
                }else if (type == Consts.BOOLEAN){
                    try{
                        if (strKey==null) {
                            ps.setNull(varID, Types.NULL);
                            ps.setNull(varID+1, Types.NULL);
                        }else{
                            ps.setBoolean(varID, Boolean.parseBoolean(strKey));
                            ps.setBoolean(varID+1, Boolean.parseBoolean(strKey));
                        }
                    }catch (Exception e){
                        dtrace.trace((String)colProperties.get("N")+":"+strKey);
                        dtrace.trace(226);
                        if (debuger.isDebugMode())
                            e.printStackTrace();
                        return false;
                    }
                }else {
                    dtrace.trace((String)colProperties.get("N")+"("+type+"):"+strKey);
                    dtrace.trace(227);
                    return false;
                }
                //varID++;
                varID+=2; // to compitable for binding null value, we should bind data twice
            }
            ps.execute();
        }catch(SQLException e)
        {
            dtrace.trace(stmt);
            dtrace.trace(506);
            return false;
        }
        return true;
    }

    // update data to physical data source
    private boolean implementLog(FyDataLogEntry.LogContent logData){
        if (logData == null)
            return true;
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

    // batch update data to physical data source, return failed number
    public int implementLog(TreeMap logs, boolean ignoreFails){
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
        return fails;
    }

    public long getCount(){
        return 0;
    }

    public HashMap getDataProps(){
        HashMap dataProps = new HashMap();
        dataProps.put("where", where);
        return dataProps;
    }

    // for push data from remote server
    public void assignMetaData(FyMetaData metaData){
        phyinited = true;
        this.metaData = metaData;
    }
    
    // for push data from remote server
    public void assignDataProps(HashMap dataProps){
        where = (String)dataProps.get("where");
    }

    // release resource
    public boolean release(){
        try{
            if (rset != null)
                rset.close();
            if (dbconn != null && !dbconn.isClosed())
                dbconn.close();
        }catch(Exception e){
            dtrace.trace(106);
            if (debuger.isDebugMode())
                e.printStackTrace();
            return false;
        }finally{
            if (metaData != null)
                metaData.releaseData();
            if (keyColumns != null)
                keyColumns.clear();
            phyinited = false;
            return true;
        }
    }
}
