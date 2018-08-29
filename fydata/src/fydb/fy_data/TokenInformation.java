/*
 * @(#)TokenInformation.java	0.01 11/06/23
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_data;

public final class TokenInformation {
    public static boolean isBeeperHost;

    public TokenInformation() {
    }
    
    public static void setBeeperHost(boolean is){
        isBeeperHost = is;
    }
}
