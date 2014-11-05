package grandmotherbrain.flow.operations.multilang;

import grandmotherbrain.top.MotherbrainException;

public class MultiLangProcessException extends MotherbrainException {

  /**
   * 
   */
  private static final long serialVersionUID = 6630075521786627663L;

  public MultiLangProcessException(MultiLangProcess multiLangProcess, String string) {
    super(string);
  }

  public MultiLangProcessException(MultiLangProcess multiLangProcess, Exception e) {
    super(e);
  }

  public MultiLangProcessException(MultiLangProcess proc) {
  }

  public MultiLangProcessException(MultiLangProcess proc, String string, Exception e) {
    super(string, e);
  }

}
