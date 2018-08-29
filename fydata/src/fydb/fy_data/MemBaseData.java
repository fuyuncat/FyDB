/*
 * @(#)MemBaseData.java	0.01 11/04/19
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_data;

import fydb.fy_comm.FyDataEntry;
import fydb.fy_comm.InitParas;

//import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public interface MemBaseData<K,V> {
    public void init(); //initialize
    
    public int getMemType(); // get type the memory data
    
    public boolean add(FyDataEntry row); // add a data row to memory dataset
    
    public V get(K key); // get a data 
    
    public void set(K key, V value); // set a value

    public V remove(K key); // remove a  data entry
    
    public Set keySet(); // return the key set

    public void putAll(Map datas); // put a data set in

    public Collection values(); // return values

    public long getSize(); // get entry count (size)
    
    public boolean containsKey(K key); // test if contain key
    
    public void releaseAll(); // release all data
    
    public int size();  // return size 
}
