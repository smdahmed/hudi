/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.exception;

import java.io.Serializable;

/**
 * <p> Exception thrown for Hoodie failures. The root of the exception hierarchy. </p> <p> Hoodie
 * Write/Read clients will throw this exception if any of its operations fail. This is a runtime
 * (unchecked) exception. </p>
 */
public class HoodieException extends RuntimeException implements Serializable {

  public HoodieException() {
    super();
  }

  public HoodieException(String message) {
    super(message);
  }

  public HoodieException(String message, Throwable t) {
    super(message, t);
  }

  public HoodieException(Throwable t) {
    super(t);
  }

  protected static String format(String message, Object... args) {
    String[] argStrings = new String[args.length];
    for (int i = 0; i < args.length; i += 1) {
      argStrings[i] = String.valueOf(args[i]);
    }
    return String.format(String.valueOf(message), (Object[]) argStrings);
  }

}
