package grandmotherbrain.flow.aggregation;

import grandmotherbrain.top.MotherbrainException;

public class AggregationException extends MotherbrainException {

  /**
   * 
   */
  private static final long serialVersionUID = 6359238031407959440L;

  public AggregationException(String string) {
    super(string);
  }

  public AggregationException(Exception ex) {
    super(ex);
  }
  
}
