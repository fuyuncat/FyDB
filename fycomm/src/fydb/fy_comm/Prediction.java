/**
 * @(#)Prediction.java	0.01 11/05/24
 *
 * Copyright 2011 Fuyuncat. All rights reserved.
 * FUYUNCAT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * Email: fuyucat@gmail.com
 * WebSite: www.HelloDBA.com
 */
package fydb.fy_comm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class Prediction implements java.io.Serializable{
    public int type = Consts.UNKNOWN;       // 1: branch; 2: leaf
    public int junction = Consts.UNKNOWN;   // if type is BRANCH, 1: and; 2: or. Otherwise, it's meaningless
    public int comparator = Consts.UNKNOWN; // if type is LEAF, 1: ==; 2: >; 3: <; 4: !=; 5: >=; 6: <=. Otherwise, it's meaningless
    public int leftColId = -1;              // if type is LEAF, it's id of column on the left to be predicted. Otherwise, it's meaningless
    public int rightColId = -1;             // if type is LEAF, it's id of column on the right to be predicted. Otherwise, it's meaningless
    public String leftExpression = null;    // if type is LEAF, it's id of column to be predicted. Otherwise, it's meaningless
    public String rightExpression = null;   // if type is LEAF, it's data to be predicted. Otherwise, it's meaningless
    public Prediction leftNode = null;      // if type is BRANCH, it links to left child node. Otherwise, it's meaningless
    public Prediction rightNode = null;     // if type is BRANCH, it links to right child node. Otherwise, it's meaningless
    public Prediction parentNode = null;    // for all types except the root, it links to parent node. Otherwise, it's meaningless
    //public String predStr = null;

    private boolean metaDataAnzlyzed = false; // analyze column name to column id.

    public Prediction(){
    }

    public Prediction(Prediction node){
        this();
        this.type = node.type;
        this.junction = node.junction;
        this.comparator = node.comparator;
        this.leftColId = node.leftColId;
        this.rightExpression = node.rightExpression;
        this.leftExpression = node.leftExpression;
        this.leftNode = node.leftNode;
        this.rightNode = node.rightNode;
        this.parentNode = node.parentNode;
        this.metaDataAnzlyzed = node.metaDataAnzlyzed;
        //this.predStr = node.predStr;
    }

    // construct a branch
    public Prediction(int junction, Prediction leftNode, Prediction rightNode){
        this();
        this.type = Consts.BRANCH;
        this.junction = junction;
        this.leftNode = leftNode;
        this.rightNode = rightNode;
        //this.leftNode = leftNode==null?null:new Prediction(leftNode);
        //this.rightNode = rightNode==null?null:new Prediction(rightNode);;
    }

    // construct a leaf
    public Prediction(int comparator, int colId, String data){
        this();
        this.type = Consts.LEAF;
        this.comparator = comparator;
        this.leftColId = colId;
        this.rightExpression = data;
    }
    
    // get left tree Height
    public int getLeftHeight(){
        int height = 1;
        if (type == Consts.BRANCH && leftNode != null)
            height += leftNode.getLeftHeight();

        return height;
    }
    
    // get left tree Height
    public int getRightHeight(){
        int height = 1;
        if (type == Consts.BRANCH && rightNode != null)
            height += rightNode.getRightHeight();
        
        return height;
    }

    // add a NEW preiction into tree
    public void add(Prediction node, int junction, boolean leafGrowth, boolean addOnTop){
        // not add any null or UNKNOWN node
        if (node == null || node.type ==  Consts.UNKNOWN) 
            return;
        if (type ==  Consts.UNKNOWN){ // not assinged
            node.copyTo(this);
        }else if (type == Consts.LEAF){
            Prediction existingNode = new Prediction();
            copyTo(existingNode);
            type = Consts.BRANCH;
            this.junction = junction;
            comparator = Consts.UNKNOWN;   
            leftColId = -1;        
            leftExpression = null;
            rightExpression = null;
            if (leafGrowth){
                rightNode = existingNode;
                rightNode.parentNode = this;
                leftNode = node;
                leftNode.parentNode = this;
            }else{
                leftNode = existingNode;
                leftNode.parentNode = this;
                rightNode = node;
                rightNode.parentNode = this;
            }
        }else{
            if (addOnTop){
                Prediction existingNode = new Prediction();
                copyTo(existingNode);
                this.junction = junction;
                if (leafGrowth){
                    leftNode = node;
                    leftNode.parentNode = this;
                    rightNode = existingNode;
                    rightNode.parentNode = this;
                }else{
                    leftNode = existingNode;
                    leftNode.parentNode = this;
                    rightNode = node;
                    rightNode.parentNode = this;
                }
            }else{
                if (leafGrowth){
                    if (leftNode != null)
                        leftNode.add(node, junction, leafGrowth, addOnTop);
                    else 
                        rightNode.add(node, junction, leafGrowth, addOnTop);
                }else{
                    if (rightNode != null)
                        rightNode.add(node, junction, leafGrowth, addOnTop);
                    else 
                        leftNode.add(node, junction, leafGrowth, addOnTop);
                }
            }
        }
    }

    private void dump(int deep){
        if (type == Consts.BRANCH){
            System.out.println("("+deep+")"+Consts.decodeJunction(junction));
            System.out.print("L-");leftNode.dump(deep+1);
            System.out.print("R-");rightNode.dump(deep+1);
        }else{
            System.out.print("("+deep+")"+leftExpression+"("+leftColId+")");
            System.out.print(Consts.decodeComparator(comparator));
            System.out.println(rightExpression);
        }
    }

    public void dump(){
        dump(0);
    }

    // detect if predication contains special colId    
    public boolean containsColId(int colId){
        boolean contain = false;
        if (type == Consts.BRANCH){
            contain = contain || leftNode.containsColId(colId);
            contain = contain || rightNode.containsColId(colId);
        }else
            contain = (this.leftColId == colId);

        return contain;
    }
    
    // detect if predication contains special colId    
    public Prediction getFirstPredByColId(int colId, boolean leftFirst){
        Prediction node = null;
        if (type == Consts.BRANCH){
            if (leftFirst){
                if (leftNode != null)
                    node = leftNode.getFirstPredByColId(colId, leftFirst);
                if (node == null)
                    node = rightNode.getFirstPredByColId(colId, leftFirst);
            }else{
                if (rightNode != null)
                    node = rightNode.getFirstPredByColId(colId, leftFirst);
                if (node == null)
                    node = leftNode.getFirstPredByColId(colId, leftFirst);
            }
        }else if (type == Consts.LEAF)
            if (this.leftColId == colId)
                node = this;

        return node;
    }
    
    // analyze column ID & name from metadata
    public boolean analyzeColumns(FyMetaData metaData1, FyMetaData metaData2){
        if (type == Consts.BRANCH){
            metaDataAnzlyzed = true;
            if (leftNode != null)
                metaDataAnzlyzed = metaDataAnzlyzed && leftNode.analyzeColumns(metaData1, metaData2);
            if (!metaDataAnzlyzed)
                return metaDataAnzlyzed;
            if (rightNode != null)
                metaDataAnzlyzed = metaDataAnzlyzed &&  rightNode.analyzeColumns(metaData1, metaData2);
        }else if (type == Consts.LEAF){
            if (metaData1 != null){
                if (leftExpression.charAt(0) == '"') {// quoted, treat as expression, otherwise, as columns
                    leftExpression = CommUtility.trim(leftExpression,'"'); // remove quoters
                    leftColId = -1;
                }else {
                    try{ // check if the name is ID already
                        leftColId = Integer.parseInt(leftExpression);
                        leftExpression = metaData1.getColumnName(Integer.valueOf(leftColId));
                    }catch(Exception e){
                        leftColId = metaData1.getColumnID(leftExpression);
                    }
                }
            }
            if (metaData2 != null){
                if (rightExpression.charAt(0) == '"') {// quoted, treat as expression, otherwise, as columns
                    rightExpression = CommUtility.trim(rightExpression,'"'); // remove quoters
                    rightColId = -1;
                }else {
                    try{ // check if the name is ID already
                        rightColId = Integer.parseInt(rightExpression);
                        rightExpression = metaData2.getColumnName(Integer.valueOf(rightColId));
                    }catch(Exception e){
                        rightColId = metaData2.getColumnID(rightExpression);
                    }
                }
            }
            if(leftColId != -1 && rightColId != -1){
                if (metaData1.getColumnType(leftColId) != metaData2.getColumnType(rightColId)){
                    //dtrace.trace(254);
                    return false;
                }else
                    return true;
            }else
                return true;
        }
        return metaDataAnzlyzed;
    }
    
    public boolean columnsAnalyzed(){
        return metaDataAnzlyzed;
    }
    
    public Prediction cloneMe(){
        Prediction node = new Prediction();
        node.metaDataAnzlyzed = this.metaDataAnzlyzed;
        //node.predStr = this.predStr;
        node.type = this.type;
        node.junction = this.junction;
        node.comparator = this.comparator;
        node.leftColId = this.leftColId;
        node.rightExpression = this.rightExpression;
        node.leftExpression = this.leftExpression;
        if (this.type == Consts.BRANCH){
            node.leftNode = this.leftNode.cloneMe();
            node.rightNode = this.rightNode.cloneMe();
            node.leftNode.parentNode = node;
            node.rightNode.parentNode = node;
        }
        return node;
    }

    public void copyTo(Prediction node){
        node.metaDataAnzlyzed = this.metaDataAnzlyzed;
        //node.predStr = this.predStr;
        node.type = this.type;
        node.junction = this.junction;
        node.comparator = this.comparator;
        node.leftColId = this.leftColId;
        node.rightExpression = this.rightExpression;
        node.leftExpression = this.leftExpression;
        if (this.type == Consts.BRANCH){
            node.leftNode = new Prediction();
            if (this.leftNode!=null)
                this.leftNode.copyTo(node.leftNode);
            node.leftNode.parentNode = node;
            node.rightNode = new Prediction();
            if (this.rightNode!=null)
                this.rightNode.copyTo(node.rightNode);
            node.rightNode.parentNode = node;
        }
    }
    
    // get all involved colIDs in this prediction
    public HashSet getAllColIDs(int side){
        HashSet colIDs = new HashSet();
        if (this.type == Consts.BRANCH){
            if (leftNode!=null)
                colIDs.addAll(leftNode.getAllColIDs(side));
            if (rightNode!=null)
                colIDs.addAll(rightNode.getAllColIDs(side));
        }else if(this.type == Consts.LEAF){
            if (side == Consts.LEFT && leftColId>=0)
                colIDs.add(Integer.valueOf(leftColId));
            else if (side == Consts.RIGHT && rightColId>=0)
                colIDs.add(Integer.valueOf(rightColId));
        }
        return colIDs;
    }

    // build the prediction as a HashMap
    public HashMap buildMap(){
        HashMap datas = new HashMap();
        if (this.type == Consts.BRANCH){
            if (leftNode!=null)
                datas.putAll(leftNode.buildMap());
            if (rightNode!=null)
                datas.putAll(rightNode.buildMap());
        }else if(this.type == Consts.LEAF){
            if (leftColId>=0)
                datas.put(Integer.valueOf(leftColId), rightExpression);
        }
        return datas;
    }

    // calculate an expression prediction
    public boolean calculateExpression(){
        boolean result=true;
        if (this.type == Consts.BRANCH){
            if (leftNode == null || rightNode == null)
                return false;
            if (this.junction == Consts.AND)
                result = leftNode.calculateExpression() && rightNode.calculateExpression();
            else
                result = leftNode.calculateExpression() || rightNode.calculateExpression();
        }else if(this.type == Consts.LEAF){
            return CommUtility.anyDataCompare(leftExpression, comparator, rightExpression, Consts.STRING) == 1;
        }else{ // no predication means alway true
            return true;
        }
        return result;
    }

    // get all involved colIDs in this prediction
    public int size(){
        int size = 0;
        if (this.type == Consts.BRANCH){
            if (leftNode!=null)
                size += leftNode.size();
            if (rightNode!=null)
                size += rightNode.size();
        }else if (this.type == Consts.LEAF)
            size = 1;
        else 
            size = 0;
        return size;
    }
    
    // clear predictin
    public void clear(){
        if (leftNode!=null){
            leftNode.clear();
            leftNode = null;
        }
        if (rightNode!=null){
            rightNode.clear();
            rightNode = null;
        }
        this.type = Consts.UNKNOWN;
        this.junction = Consts.UNKNOWN;
        this.comparator = Consts.UNKNOWN;
        this.leftColId = -1;
        this.rightExpression = null;
        this.leftExpression = null;
    }

    // remove a node from prediction. Notics, the input node is the address of the node contains in current prediction
    //   0                      0                  2                 1
    //  1  2  (remove 3) =>   4   2 (remove 1) =>      (remove 2)  3   4
    //3  4
    public boolean remove(Prediction node){
        boolean removed = false;
        if (leftNode!=null){
            if (leftNode == node){
                leftNode.clear();
                leftNode = null;
                return true;
            }else{
                removed = removed || leftNode.remove(node);
                if (removed)
                    return removed;
            }
        }
        if (rightNode!=null){
            if (rightNode == node){
                rightNode.clear();
                rightNode = null;
                return true;
            }else{
                removed = removed || rightNode.remove(node);
                if (removed)
                    return removed;
            }
        }
        if (this == node){
            this.type = Consts.UNKNOWN;
            this.junction = Consts.UNKNOWN;
            this.comparator = Consts.UNKNOWN;
            this.leftColId = -1;
            this.rightExpression = null;
            this.leftExpression = null;
            return true;
        }else
            return removed;

        /*if (this == node){
            if (this.parentNode != null){
                if (this.parentNode.leftNode == this ) // this is leftnode
                    this.parentNode.rightNode.copyTo(this); // assign right brother as parent
                else if (this.parentNode.rightNode == this ) // this is rihtnode
                     this.parentNode.leftNode.copyTo(this); // assign left brother as parent
            }
            return true;
        }else if (type == Consts.BRANCH){
            return (leftNode != null && leftNode.remove(node)) || (rightNode != null && rightNode.remove(node));
        }
        return false;//*/
    }

     // build a data list for a set of column, keeping same sequence, complete the absent column with NULL
     public void fillDataForColumns(ArrayList dataList, ArrayList columns){
         if (columns == null)
            return;
        
        if (type == Consts.BRANCH){
            if (leftNode != null)
                leftNode.fillDataForColumns(dataList, columns);
            if (rightNode != null)
                rightNode.fillDataForColumns(dataList, columns);
        }else if (type == Consts.LEAF && leftColId >= 0)
            dataList.set(columns.indexOf(Integer.valueOf(leftColId)), rightExpression);
     }
}
