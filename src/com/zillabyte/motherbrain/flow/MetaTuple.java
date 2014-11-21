package com.zillabyte.motherbrain.flow;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import com.google.common.collect.Lists;
import com.zillabyte.motherbrain.relational.ColumnDef;
import com.zillabyte.motherbrain.relational.DataType;
import com.zillabyte.motherbrain.utils.DateHelper;

public final class MetaTuple implements Serializable {

  /**
   * Serialization ID
   */
  private static final long serialVersionUID = -9002578129801868470L;
  private Date since = null;
  private Double confidence = null;
  private String source = null;

  public MetaTuple() {
    this("");
  }

  public MetaTuple(String source) {
    this(DateHelper.now(), Double.valueOf(1.0), source);
  }

  public MetaTuple(Date since, String source) {
    this(since, Double.valueOf(1.0), source);
  }

  public MetaTuple(long since, String source) {
    this(new Date(since), Double.valueOf(1.0), source);
  }

  public MetaTuple(Double confidence, String source) {
    this(DateHelper.now(), confidence, source);
  }

  public MetaTuple(Date since, Double confidence, String source) {
    this.since = since;
    this.confidence = confidence;
    this.source = source;
  }

  public MetaTuple(long since, Double confidence, String source) {
    this(new Date(since), confidence, source);
  }

  public List<? extends Serializable> asList() {
    return Lists.newArrayList(
        (Serializable) since, 
        (Serializable) confidence,
        (Serializable) source
        );
  }

  @Override
  public String toString() {
    return "|" + asList().toString() + "|";
  }

  public Date getSince() {
    return since;
  }

  public void setSince(Date since) {
    this.since = since;
  }

  public Double getConfidence() {
    return confidence;
  }

  public void setConfidence(Double confidence) {
    this.confidence = confidence;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  private static final ColumnDef _sinceColumn;
  private static final ColumnDef _confidenceColumn;
  private static final ColumnDef _sourceColumn;
  private static final List<ColumnDef> _columns;

  static {    
    _sinceColumn = new ColumnDef("since", DataType.DATE);
    _confidenceColumn = new ColumnDef("confidence", DataType.DOUBLE);
    _sourceColumn = new ColumnDef("source", DataType.STRING);
    _columns = Lists.newArrayList(_sinceColumn, _confidenceColumn, _sourceColumn);
  }

  public static List<ColumnDef> columns() {
    return _columns;
  }

  public static int size() {
    return _columns.size();
  }

  public static ColumnDef sinceColumn() {
    return _sinceColumn;
  }

  public static ColumnDef confidenceColumn() {
    return _confidenceColumn;
  }

  public static ColumnDef sourceColumn() {
    return _sourceColumn;
  }

  public Serializable get(int i) {
    return asList().get(i);
  }

  public void set(int index, Serializable obj) {
    switch(index) {
    case 0: 
      since = (Date) obj;
      break;
    case 1:
      confidence = (Double) obj;
      break;
    case 2:
      source = (String) obj;
      break;
    default:
      throw new IndexOutOfBoundsException("unknown meta field"); 
    }
  }

  public void put(String name, Object object) {
    if (_sinceColumn.getName().equals(name)) {
      since = (Date) object;
    } else if (_confidenceColumn.getName().equals(name)) {
      
      confidence = (Double) object;
    } else if (_sourceColumn.getName().equals(name)) { 
      source = (String) object;
    } else {
      throw new RuntimeException("unknown column");
    }
  }

  public static String[] columnsNames() {
    return new String[] {_sinceColumn.getName(), _confidenceColumn.getName(), _sourceColumn.getName()};
  }

}
