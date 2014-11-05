package com.zillabyte.motherbrain.utils;

import org.eclipse.jdt.annotation.Nullable;

public final class ByteArrayWrapper {
  final byte[] bytes;

  ByteArrayWrapper(final byte[] bytes) {
    this.bytes = bytes;
  }

  public static final @Nullable ByteArrayWrapper wrap(final @Nullable byte[] bytes) {
    final ByteArrayWrapper wrapper;
    if (bytes == null) {
      wrapper = null;
    } else {
      wrapper = new ByteArrayWrapper(bytes);
    }
    return wrapper;
  }

  public static @Nullable byte[] unwrap(final @Nullable ByteArrayWrapper wrapper) {
    final byte[] bytes;
    if (wrapper == null) {
      bytes = null;
    } else {
      bytes = wrapper.bytes;
    }
    return bytes;
  }
}