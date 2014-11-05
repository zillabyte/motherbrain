package grandmotherbrain.relational;

import grandmotherbrain.flow.FlowCompilationException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.jdt.annotation.NonNullByDefault;


@NonNullByDefault
public final class ColumnDef implements Serializable {

  
  /**
   * 
   */
  private static final long serialVersionUID = -632446241439185013L;
  private String _name;
  private DataType _dataType;
  private final List<String> _aliases = new ArrayList<>(1);
  
  
  public ColumnDef(String name, DataType dataType, String... aliases) {
    this._name = name;
    this._dataType = dataType;
    for(String s : aliases) {
      if (s == null) {
        throw new NullPointerException("No aliases can be null!");
      }
      _aliases.add(s);
    }
  }
  
  public ColumnDef(int index, DataType dataType) {
    this("v" + index, dataType);
  }
  

  public ColumnDef(int index, DataType dataType, String... aliases) {
    this("v" + index, dataType, aliases);
  }
  
  


  public String getName() {
    return _name;
  }
  
  public DataType getDataType() {
    return _dataType;
  }

  
  
  
  /// Test Helpers
  
  public static ColumnDef createString(String name) {
    return new ColumnDef(name, DataType.STRING, name);
  }
  
  public static ColumnDef createInteger(String name) {
    return new ColumnDef(name, DataType.INTEGER, name);
  }
  
  public static ColumnDef createDate(String name) {
    return new ColumnDef(name, DataType.DATE, name);
  }
  
  public static ColumnDef createDouble(String name) {
    return new ColumnDef(name, DataType.DOUBLE, name);
  }
  
  public static ColumnDef createBoolean(String name) {
    return new ColumnDef(name, DataType.BOOLEAN, name);
  }

  public static DataType convertStringToDataType(String type) throws FlowCompilationException {
    if(type.equalsIgnoreCase("string")) {
      return DataType.STRING;
    } else if(type.equalsIgnoreCase("integer")) {
      return DataType.INTEGER;
    } else if(type.equalsIgnoreCase("float") || type.equalsIgnoreCase("double")) {
      return DataType.DOUBLE;
    } else if(type.equalsIgnoreCase("boolean")) {
      return DataType.BOOLEAN;
    } else if(type.equalsIgnoreCase("date")) {
      return DataType.DATE;
    } else if(type.equalsIgnoreCase("array")) {
      return DataType.ARRAY;
    } else if(type.equalsIgnoreCase("map")) {
      return DataType.MAP;
    } else {
      throw new FlowCompilationException("Unknown data type "+type);
    }
  }
  
  public List<String> getAliases() {
    return this._aliases;
  }
  
  public String getFirstAlias() {
    if (this._aliases.isEmpty()) {
      return null;
    } else {
      return this._aliases.get(0);
    }
  }
  
  public boolean hasAliases() {
    return this._aliases.size() > 0;
  }
  
  
  @Override
  public String toString() {
    return "Column:{" + this.getName() + "," + this._dataType.toString() + "}";
  }
  
}
