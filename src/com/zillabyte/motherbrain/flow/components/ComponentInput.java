package com.zillabyte.motherbrain.flow.components;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.eclipse.jdt.annotation.NonNullByDefault;

import com.google.common.collect.Lists;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;
import com.zillabyte.motherbrain.flow.config.FlowConfig;
import com.zillabyte.motherbrain.flow.operations.Function;
import com.zillabyte.motherbrain.flow.operations.LoopException;
import com.zillabyte.motherbrain.flow.operations.Operation;
import com.zillabyte.motherbrain.relational.ColumnDef;


/***
 * Represents the 'input' of a component; 
 * @author jake
 *
 */
@NonNullByDefault
public class ComponentInput extends Function {


  private static final long serialVersionUID = -9197905078512195657L;
  private final List<ColumnDef> _componentExpectedFields;
  private final HashMap<String, String> _renameFields;
  
  
  public ComponentInput(final String name, ColumnDef... expectedFields) {
    super(name);
    _componentExpectedFields = Lists.newArrayList(expectedFields);
    _renameFields = new HashMap<>();
  }
  
  public ComponentInput(final String name, final ArrayList<ColumnDef> expectedFields) {
    super(name);
    _componentExpectedFields = expectedFields;
    _renameFields = new HashMap<>();
  }
  
  
  public ComponentInput(JSONObject node, FlowConfig _flowConfig) {
    super(node.getString("name"));
    
    // Get the fields of embedded component input
    JSONArray sourceFields = node.getJSONArray("fields");
    
    // Get the column defs
    ArrayList<ColumnDef> colDefs = new ArrayList<>();
    int i=0;
    for(Object o : sourceFields) {
      JSONObject colDef = (JSONObject) o;
      final String colName = (String) colDef.keys().next();
      final String colType = colDef.getString(colName);
      assert (colName != null);
      colDefs.add(new ColumnDef(i, ColumnDef.convertStringToDataType(colType), colName));
      i++;
    }
    
    // Create the component input
    _componentExpectedFields = colDefs;
    _renameFields = new HashMap<>();
  }
  
  

  public List<ColumnDef> componentExpectedFields() {
    return _componentExpectedFields;
  }


  @Override
  public String type() {
    return "consumer";
  }


  public void renameField(String from, String to) {
    _renameFields.put(from, to);
  }

  public boolean shouldMerge() {
    return this.getContainerFlow().getFlowConfig().getShouldMerge();
  }
  
  

  @Override
  protected void process(MapTuple t, OutputCollector c) throws LoopException {
    for(Entry<String, String> e : _renameFields.entrySet()) {
      final String key = e.getKey();
      /* We never put null keys in */
      assert (key != null);
      /* Or null values */
      final String value = e.getValue();
      assert (value != null);
      t.put(value, t.get(key));
    }
    
    if(shouldMerge()) {
      t.put(getParentComponentCarryFieldName(), t.getValuesJSON());
    }
    
    c.emit(t);
  }

  @Override
  public final void prepare() {
  }
  

  public String getParentComponentCarryFieldName() throws LoopException {
    return Operation.COMPONENT_CARRY_FIELD_PREFIX + this.getTopFlow().getId();
  }
  
}
