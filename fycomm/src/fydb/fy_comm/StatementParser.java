/**
 * @(#)Prediction.java	0.01 11/05/27
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_comm;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class StatementParser {
    private String predString;
    private Prediction prediction;
    private int analyzedPos = 0;

    private final static String[] junctionWords = new String[] {"AND", "OR"};
    private final static String[] junctionSplitors = new String[] {" AND ", " OR "};
    private final static String[] comparators = new String[] {"=", "!=", ">=", "<=", ">", "<"}; // ">=", "<=" should be before ">", "<"

    public StatementParser(String predString) {
        this.predString = predString;
        prediction = new Prediction();
    }
    
    // detect if string start with special words
    public static int startsWithWords(String str, String[] words, int offset){
        for (int i=0;i<words.length;i++){
            if (str.startsWith(words[i], offset))
                return i;
        }
        return -1;
    }

    // detect if string start with special words
    public static int startsWithWords(String str, String[] words){
        return startsWithWords(str,words,0);
    }
    
    public static int decodeComparator(String str){
        if ("=".equals(str))
            return Consts.EQ;
        else if (">".equals(str))
            return Consts.LT;
        else if ("<".equals(str))
            return Consts.ST;
        else if ("!=".equals(str))
            return Consts.NEQ;
        else if (">=".equals(str))
            return Consts.LE;
        else if ("<=".equals(str))
            return Consts.SE;
        else
            return Consts.UNKNOWN;
    }

    public static int decodeJunction(String str){
        if ("AND".equals(str))
            return Consts.AND;
        else if ("OR".equals(str))
            return Consts.OR;
        else
            return Consts.UNKNOWN;
    }

    public static boolean isJunctionWord(String word){
        for (int i=0;i<junctionWords.length;i++)
            if (junctionWords[i].equalsIgnoreCase(word))
                return true;
        return false;
    }

    // remove space
    public static String removeSpace (String originalStr, String keepPattern){
        //if (keepPattern == null)
        //    keepPattern =  "(\\s+OR\\s+|\\s+AND\\s+)"; //default pattern
            // keepPattern =  "\\s+NOT\\s+|\\s+OR\\s+|\\s+AND\\s+|\\s+IN\\s+|\\s+LIKE\\s+"; //default pattern
        String[] keepWords = {" OR ", " AND "};

        String cleanedStr = "";
        int i = 0;

        //Pattern keeper = Pattern.compile(keepPattern);
        //Matcher matcher = keeper.matcher(originalStr.substring(i).toUpperCase());
        while (i < originalStr.length()) {
        int matchedWordId = startsWithWords(originalStr.substring(i).toUpperCase(), keepWords);
        if (originalStr.charAt(i) != ' ') {// ' ' to be removed
          cleanedStr = cleanedStr+originalStr.charAt(i);
          i++;
        }else if (matchedWordId >= 0) {
            cleanedStr = cleanedStr+keepWords[matchedWordId];
            i+=keepWords[matchedWordId].length();
        }else
          i++;
      }
      return cleanedStr;
    }

    // build a leaf node from special string
    private static void buildLeafNodeFromStr(Prediction node, String str){
        boolean quoteStarted = false;
        int lastPos = 0;
        for (int i=0;i<str.length();i++){
            if (str.charAt(i) == '"'){
                quoteStarted = !quoteStarted;
                str = str.substring(0, i)+(i<str.length()-1?str.substring(i+1):"");
                i--;
            }else if(str.charAt(i) == '\\' && i<str.length()-1 && str.charAt(i+1) == '"'){
                i++; // skip escaped " :\"
                str = str.substring(0, i-1)+str.substring(i);
            }else if(!quoteStarted && startsWithWords(str.substring(i), comparators) >= 0){ // splitor that not between quato are the real splitor
                String compStr = comparators[startsWithWords(str.substring(i), comparators)];
                node.comparator = decodeComparator(compStr);
                node.type = Consts.LEAF;
                node.leftExpression = new String(str.substring(0,i).trim());
                node.rightExpression = new String(CommUtility.trim(str.substring(i+compStr.length()).trim(),'"'));
                return;
            }
        }
    }

    // split input command line into pieces; \ is escape char, " and splitor could be escaped.
    // space splitor in "" should be ignored. \" is character '"'
    // splitString({ad cas asdfa}, ' ', {'{','}'}, true)  => {ad,cas,asdfa}
    // splitString("ad "cas\" asdfa"", ' ', {'"','"'}, true) => {ad,cas\" asdfa}
    private static void buildPrediction(Prediction node, String initialString, String splitor, char[] quoters){
        //System.out.println(String.format("%d",deep) +":"+initialString);
        if (initialString == null){
            System.out.println("");
            System.out.println("Error: No statement found!");
            System.out.println("");
            return;
        }else
            initialString = initialString.trim();
        initialString = initialString.trim();
        //node.predStr = new String(initialString);
        char stringQuoter = '"';
        if (quoters == null || quoters.length != 2)
            quoters = new char[]{'(',')'};
        int quoteDeep = 0;
        int quoteStart = -1;  // top quoter start position
        int quoteEnd = -1;;   // top quoter end position
        boolean stringStart = false;
        for (int i=0;i<initialString.length();i++){
            if (initialString.charAt(i) == stringQuoter &&(i==0 || (i>0 && initialString.charAt(i-1)!='\\'))){ // \ is ignore character
                stringStart = !stringStart;
                continue;
            }
            if (stringStart) // ignore all character being a string
                continue;
            if (initialString.charAt(i) == quoters[0]){
                quoteDeep++;
                if (quoteDeep == 1 && quoteStart < 0)
                    quoteStart = i;
            }else if (initialString.charAt(i) == quoters[1]){
                quoteDeep--;
                if (quoteDeep == 0 && quoteEnd < 0)
                    quoteEnd = i;
                if  (quoteDeep < 0){
                    System.out.println("");
                    System.out.println("Error: Left quoter missed!");
                    System.out.println("");
                }
            }else if(initialString.charAt(i) == '\\' && i<initialString.length()-1 && 
                    ((initialString.charAt(i+1) == quoters[0] || initialString.charAt(i+1) == quoters[1] || initialString.charAt(i+1) == ' '))){
                i++; // skip escaped " \"
                initialString = initialString.substring(0, i-1)+initialString.substring(i);
            }else if(quoteDeep == 0 && initialString.charAt(i) == ' '){ // splitor that not between quato are the real splitor
                if ( initialString.substring(i).toUpperCase().startsWith(splitor)){
                    node.type = Consts.BRANCH;
                    node.junction = decodeJunction(splitor.trim());
                    node.leftNode = new Prediction();
                    node.leftNode.parentNode = node;
                    buildPrediction(node.leftNode, initialString.substring(0, i)," OR ",quoters); // OR priority higher than AND
                    node.rightNode = new Prediction();
                    node.rightNode.parentNode = node;
                    buildPrediction(node.rightNode, initialString.substring(i+splitor.length())," OR ",quoters);
                    return;
                }
            }
        }

        if (quoteDeep != 0){
            System.out.println("");
            System.out.println("Error: Right quoter missed!");
            System.out.println("");
        }

        if (quoteStart == 0 && quoteEnd == initialString.length()-1){ // sub expression quoted 
            initialString = initialString.substring(1,initialString.length()-1);  // trim the quoters
            buildPrediction(node, initialString," OR ",quoters);
        }else if (" OR ".equals(splitor)){
            buildPrediction(node, initialString," AND ",quoters);
        }else
            buildLeafNodeFromStr(node, initialString);
    }
    
    public static Prediction buildPrediction(String initialString){
        Prediction node = new Prediction();
        buildPrediction(node, initialString, " OR ", new char[]{'(',')'});
        //node.dump();
        return node;
    }

    // detect if quoters matched. 
    // listStr string to be detected;
    // offset, off set to begin test;
    // quoters,  eg. {'(',')'}
    // 0 means all matched
    public static int matchQuoters(String listStr, int offset, char[] quoters){
        if (quoters == null || quoters.length != 2 || offset < 0)
            return -1;
        int deep = 0;
        for (int i=offset;i<listStr.length();i++) {
            if (listStr.charAt(i) == quoters[0])
                deep++;
            else if (listStr.charAt(i) == quoters[1])
                deep--;
        }
        return deep;
    }

    //get the first matched token from a string
    public static String getFirstToken(String str, String token){
        Pattern searcher = Pattern.compile(token);
        Matcher matcher = searcher.matcher(str.toUpperCase());
        if (matcher.find()){
            return str.substring(matcher.start(),matcher.end());
        }else
            return null;
    }
    
    //get all matched token from a string
    public static String[] getAllTokens(String str, String token){
        String[] tokens = new String[0];
        Pattern searcher = Pattern.compile(token);
        Matcher matcher = searcher.matcher(str.toUpperCase());
        while (matcher.find()){
            //String[] temp = tokens;
            //tokens = new String[temp.length+1];
            //System.arraycopy(temp,0,tokens,0,temp.length);
            //tokens[tokens.length-1] = str.substring(matcher.start(),matcher.end());
            tokens = (String[])CommUtility.appendToArray(tokens,new String(str.substring(matcher.start(),matcher.end())));
        }
        return tokens;
    }    

    // check if matched token
    public static boolean matchToken(String str, String token){
        Pattern searcher = Pattern.compile(token);
        Matcher matcher = searcher.matcher(str.toUpperCase());
        return matcher.find();
    }
}
