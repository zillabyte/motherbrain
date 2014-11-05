package com.zillabyte.motherbrain.utils;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;

import javax.annotation.concurrent.Immutable;

import com.github.rholder.retry.Attempt;


@Immutable
public final class ExceptionAttempt<R> implements Attempt<R>, Serializable {

  /**
   * Serialization ID
   */
  private static final long serialVersionUID = -7756832421673481835L;
  private final ExecutionException e;

  public ExceptionAttempt(Throwable cause) {
    this.e = new ExecutionException(cause);
  }

  @Override
  public R get() throws ExecutionException {
    throw e;
  }

  @Override
  public boolean hasResult() {
    return false;
  }

  @Override
  public boolean hasException() {
    return true;
  }

  @Override
  public R getResult() throws IllegalStateException {
    throw new IllegalStateException("The attempt resulted in an exception, not in a result");
  }

  @Override
  public Throwable getExceptionCause() throws IllegalStateException {
    return e.getCause();
  }
}