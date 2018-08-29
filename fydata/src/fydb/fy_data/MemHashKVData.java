/*
 * @(#)MemHashData.java	0.01 11/04/19
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_data;

//import java.util.ArrayList;
import fydb.fy_comm.Consts;
//import fydb.fy_comm.InitParas;
import fydb.fy_comm.FyDataEntry;
import fydb.fy_comm.Tracer;

import java.io.Serializable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MemHashKVData<K,V> extends HashMap<K,V> implements MemBaseData, Serializable {
    public Tracer dtrace; // = new Tracer();
    private int memType;

    public MemHashKVData(Tracer dtrace) {
        this.dtrace = dtrace;
        this.memType = Consts.HASHMAP;
    }

    public void init(){
    }

    public int getMemType(){
        return this.memType;
    }
    
    public boolean add(FyDataEntry row){
        super.put(row.key, row.value);
        return true;
    }
    
    public long getSize(){
        return super.size();
    }

    public V get(K key){
        return super.get(key);
    }

    public Set keySet(){
        return super.keySet();
    }

    public Collection values(){
        return super.values();
    }

    public void set(K key, V value){
        //super.remove(key);
        super.put(key, value);
    }

    public V remove(K key){
        return super.remove(key);
    }

    public void putAll(Map datas){
        super.putAll(datas);
    }

    public boolean containsKey(K key){
        return super.containsKey(key);
    }

    public void releaseAll(){
        super.clear();;
    }
    
    public int size(){
        return super.size();
    }
}
