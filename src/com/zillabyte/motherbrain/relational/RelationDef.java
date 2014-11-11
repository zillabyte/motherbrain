package com.zillabyte.motherbrain.relational;


import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.Lists;
import com.zillabyte.motherbrain.flow.MetaTuple;

public class RelationDef implements Serializable {
  
  /**
   * 
   */
  private static final long serialVersionUID = 5129083835231978140L;
  private String tableName;
  private List<ColumnDef> _valueColumns;
  private String concreteName;
  
  
  public RelationDef(String tableName) {
    this(tableName, tableName);
  }
  
  public RelationDef(String tableName, String concreteName) {
    this(tableName, concreteName, new LinkedList<ColumnDef>());
  }
  
  public RelationDef(String tableName, String concreteName, List<ColumnDef> valueColumns) {
    this.tableName = tableName;
    this.concreteName = concreteName;
    this._valueColumns = valueColumns;
  }
  
  public RelationDef(String tableName, String concreteName, ColumnDef... valueColumns) {
    this(tableName, concreteName, Arrays.asList(valueColumns));
  }


  public List<ColumnDef> valueColumns() {
    return _valueColumns;
  }
  
  public List<ColumnDef> metaColumns() {
    return MetaTuple.columns();
  }
  
  
  public String name() {
    return tableName;
  }
  
  
  public String concreteName() {
    return this.concreteName;
  }
  
  
  public RelationBackend getCurrentBackend() {
    return RelationBackend.POSTGRES;
  }
  

  public List<ColumnDef> allColumns() {
    List<ColumnDef> l = Lists.newArrayList();
    l.addAll(valueColumns());
    l.addAll(metaColumns());
    return l;
  }

  
  
  public void addColumn(ColumnDef col) {
    this._valueColumns.add(col);
  }

  public void setConcreteName(String concreteTableName) {
    this.concreteName = concreteTableName;
  }

  
}
