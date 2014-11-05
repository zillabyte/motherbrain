package com.zillabyte.motherbrain.flow;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONArray;

import org.eclipse.jdt.annotation.NonNullByDefault;

import com.google.common.collect.Lists;

@NonNullByDefault
public final class Fields implements Iterable<String>, Serializable {

  private static final long serialVersionUID = -2286408858067807309L;

  public static final String COLUMN_ALIASES = "_column_aliases";
  
  private ArrayList<String> _list;
  public static final String DEFAULT = "_default_field";

  public Fields(String... strings) {
    this(Lists.newArrayList(strings));
  }

  public Fields(List<String> fieldNames) {
    this._list = new ArrayList<>(fieldNames);
  }
  
  public Fields(JSONArray fieldNames) {
    this((String[])fieldNames.toArray(new String[] {}));
  }

  public Fields(Collection<String> fields) {
    this(fields.toArray(new String[] {}));
  }

  public String[] asStrings() {
    final String[] array = _list.toArray(new String[] {});
    assert (array != null);
    return array;
  }

  public int getIndex(String key) {
    return _list.indexOf(key);
  }
  
  public Fields add(String s) {
    this._list.add(s);
    return this;
  }
  
  public Fields addAll(Collection<String> c) {
    this._list.addAll(c);
    return this;
  }
  
  public Fields addAll(Fields f) {
    for(final String s : f) {
      /*
       * Never allow a null field
       */
      assert (s != null);
      if (this._list.contains(s) == false) {
        this.add(s);
      }
    }
    return this;
  }

  /**
   * Contract: Every element that returned iterator goes through is non-null. 
   */
  @Override
  public Iterator<String> iterator() {
    final Iterator<String> iter = _list.iterator();
    /*
     * By contract with List.
     */
    assert (iter != null);
    return iter;
  }

  public int size() {
    return _list.size();
  }
  
  @Override
  public Fields clone() {
    return new Fields(new ArrayList<>(_list));
  }

  public Fields remove(Fields groupFields) {
    Fields f = this.clone();
    for(String i : groupFields._list) {
      f._list.remove(i);
    }
    return f;
  }
  
  
  
  /***
   * 
   */
  @Override
  public String toString() {
    final String string = _list.toString();
    assert (string != null);
    return string;
  }

  /**
   * @param i Index of field
   * @return Non-null String representing field name.
   */
  public String get(int i) {
    final String value = _list.get(i);
    /*
     * Either we're out of bounds or the value is non-null
     */
    assert (value != null);
    return value;
  }

  public boolean contains(String field) {
    return this._list.contains(field);
  }

}
