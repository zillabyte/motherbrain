package com.zillabyte.motherbrain.flow.operations.multilang;


public class MultiLangProcessDiedUnexpectedlyException extends MultiLangProcessException {

  private static final long serialVersionUID = -8176250408690563998L;

  
  public MultiLangProcessDiedUnexpectedlyException(MultiLangProcess multiLangProcess, String string) {
    super(multiLangProcess, string);
  }


  public MultiLangProcessDiedUnexpectedlyException(MultiLangProcess proc) {
    super(proc);
    setInternalMessage("Process " + proc.getProcessBuilder().command().toString() + " died!");
    setUserMessage("Process died unexpectedly");
  }


}
