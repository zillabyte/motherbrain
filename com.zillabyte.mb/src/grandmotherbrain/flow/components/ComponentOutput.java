package grandmotherbrain.flow.components;

import grandmotherbrain.flow.Fields;
import grandmotherbrain.flow.FlowCompilationException;
import grandmotherbrain.flow.MapTuple;
import grandmotherbrain.flow.collectors.OutputCollector;
import grandmotherbrain.flow.config.FlowConfig;
import grandmotherbrain.flow.graph.Connection;
import grandmotherbrain.flow.operations.Function;
import grandmotherbrain.flow.operations.Operation;
import grandmotherbrain.flow.operations.OperationException;
import grandmotherbrain.flow.operations.multilang.MultiLangException;
import grandmotherbrain.relational.ColumnDef;

import java.util.ArrayList;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.google.common.collect.Lists;


public class ComponentOutput extends Function {
  
  private static final long serialVersionUID = -2209824682828708006L;
  private final List<ColumnDef> _componentFields;



  public ComponentOutput(final String name, ColumnDef... fields) {
    super(name);
    _componentFields = Lists.newArrayList(fields);
  }
  
  public ComponentOutput(final String name, List<ColumnDef> fields) {
    super(name);
    _componentFields = fields;
  }
  
  public ComponentOutput(JSONObject node, FlowConfig _flowConfig) throws FlowCompilationException {
    super(node.getString("name"));
    
    // Get the fields
    JSONArray sinkFields = node.getJSONArray("columns");
    
    // Get the column defs
    ArrayList<ColumnDef> colDefs = new ArrayList<>();
    int i=0;
    for(Object o : sinkFields) {
      JSONObject colDef = (JSONObject) o;
      final String colName = (String) colDef.keys().next();
      final String colType = colDef.getString(colName);
      assert (colName != null);
      colDefs.add(new ColumnDef(i, ColumnDef.convertStringToDataType(colType), colName));
      i++;
    }
    
    // Create the component
    _componentFields = colDefs;
    
  }

  public List<ColumnDef> componentFields() {
    return this._componentFields;
  }


  @Override
  public String type() {
    return "producer";
  }



  public String produceStream() {
    return this.namespaceName() + "-stream";
  }


  @Override
  protected void process(MapTuple t, OutputCollector c) throws OperationException, InterruptedException {
    MapTuple outputTuple = new MapTuple(t);
    boolean carryFieldSeen = false;
    
    if(parentComponentShouldMerge()) {
      
      String carryFieldName = getParentComponentCarryFieldName();
      final JSONObject inputTuple = (JSONObject) t.get(carryFieldName);
      if(inputTuple != null) {
        for(Object o : inputTuple.keySet()) {
          final String ifield = (String) o;
          if(ifield != null && !outputTuple.containsValueKey(ifield)) {
            final Object i = inputTuple.get(o);
            if (i != null) {
              outputTuple.add(ifield, i);
            }
          }
        }
        carryFieldSeen = true;
      }

      if(!carryFieldSeen) throw new OperationException(this, "Attempted to merge fields in ComponentOutput, but no input tuple given!");
      outputTuple.remove(getParentComponentCarryFieldName());
      
    }
    c.emit(outputTuple);
  }

  @Override
  public void onSetExpectedFields() throws OperationException {
    for(final Connection c : this.prevConnections()) {

      // Because we are a simple pass-through, any expected fields of us will be expected of the 
      // previous operation as well.
      for(ColumnDef cd : _componentFields) {
        for(String s : cd.getAliases()) {
          Fields f = new Fields(s);
          c.source().addExpectedFields(c.streamName(), f);
        }
      }

      if(parentComponentShouldMerge()) {
        // If this component output needs to merge the input, then its previous operation will
        // emit all of the fields declared for the output as well as a carry field.
        c.source().addExpectedFields(c.streamName(), new Fields(getParentComponentCarryFieldName()));
      } 
    }
    super.onSetExpectedFields();
  }
  
  @Override
  public final void prepare() throws MultiLangException, InterruptedException {
    /* Noop */
  }  

  public boolean parentComponentShouldMerge() {
    return this.getContainerFlow().getFlowConfig().getShouldMerge();
  }
  
  
  public String getParentComponentCarryFieldName() throws OperationException {
    return Operation.COMPONENT_CARRY_FIELD_PREFIX + this.getTopFlow().getId();
  }
  
  
  @Override
  public String prefixifyStreamName(String stream) {
    // We don't prefix here because we want to translate from the embedded stream name to the
    // outer context's name. 
    // TODO: there's a bug here with 2+ nested components.  We need a new function that can 
    // return the parent's prefix name. 
    return stream;
  }
  
  
}
