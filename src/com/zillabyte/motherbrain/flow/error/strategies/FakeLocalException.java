package com.zillabyte.motherbrain.flow.error.strategies;

import com.zillabyte.motherbrain.flow.operations.OperationException;
import com.zillabyte.motherbrain.utils.Utils;


/***
 * This exception is thrown in local mode when we don't want to kill the jvm.
 * 
 * Why? The only way to kill an operation in prod is to throw an exception up 
 * to storm.  That presents problems in local mode, because the storm will
 * kill the entire process.  Because we sometimes want to test that errors
 * are being handled, we instead throw this exception.  This exception 
 * is caught at all key places in GMB and simply waits before continuing
 * on with the loop.  That is, the JVM should never die when this is thrown
 * @author jake
 *
 */
public class FakeLocalException extends RuntimeException {

  private static final long WAIT_INTERVAL_MS = 1000 * 1L;
  private static final long serialVersionUID = -6173318037038700144L;

  public FakeLocalException(OperationException error) {
    super(error);
  }

  public void printAndWait() {
    System.err.println("fake local error: " + this.toString());
    Utils.sleep(WAIT_INTERVAL_MS);
  }

}
