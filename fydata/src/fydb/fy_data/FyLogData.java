/*
 * @(#)FyLogData.java	0.01 11/05/10
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_data;

import java.util.TreeMap;
//import fydb.fy_main.BP;

import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;

public class FyLogData<K,V> extends TreeMap<K,V> {
    K lastBp = new BP();

    public FyLogData() {
    }

    public FyLogData(Comparator<? super K> c) {
        super(c);
    }

    public FyLogData(SortedMap<K, ? extends V> m) {
        super(m);
    }

    public V put(K key, V value){
        lastBp = key;
        return super.put(key, value);
    }

    public void putAll(Map<? extends K, ? extends V> map){
        lastBp = ((TreeMap)map).lastKey();
        super.putAll(map);
    }
}
