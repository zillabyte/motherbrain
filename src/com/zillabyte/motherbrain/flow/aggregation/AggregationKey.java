package com.zillabyte.motherbrain.flow.aggregation;

import java.io.Serializable;
import java.util.List;

import com.google.common.collect.Lists;

public class AggregationKey implements Serializable {
  
  private static final long serialVersionUID = 7831807834723292657L;
  
  private List<Object> _groupValues = null;
  

  public AggregationKey(List<Object> list) {
    this._groupValues = list;
  }

  
  @Override
  public int hashCode() {
    int hash = 0;
    if (this._groupValues != null) 
      hash += this._groupValues.hashCode(); 
    return hash;
  }
  

  @Override
  public boolean equals(Object o) {
    
    if (o == null) return false;
    if (o instanceof AggregationKey == false) return false;
    AggregationKey ak = (AggregationKey)o;
    
    // Compare keys
    return _groupValues.equals(ak._groupValues);
    
  }


  public int groupValueSize() {
    return this._groupValues.size();
  }


  public Object getGroupValue(int i) {
    return this._groupValues.get(i);
  }


  public boolean containsGroupValue(Object val) {
    return this._groupValues.contains(val);
  }

  
  
  @Override
  public String toString() {
    return _groupValues.toString();
  }


  public static AggregationKey create(Object... subkeys) {
    return new AggregationKey(Lists.newArrayList(subkeys));
  }
  
  
  
}
