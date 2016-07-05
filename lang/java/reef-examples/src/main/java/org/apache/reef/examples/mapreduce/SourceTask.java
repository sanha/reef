/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.examples.mapreduce;

import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.Random;

/**
 * A Source Task of MapReduce application.
 */
public final class SourceTask implements Task {

  @Inject
  private SourceTask() {
  }

  @Override
  public byte[] call(final byte[] memento) {
    System.out.println("Hello, REEF! This is source task");
    char[] str;
    for(;;) {
      try {
        str = stringGenerator();

        Thread.sleep(1000);
      } catch(InterruptedException e){
        System.out.println(e.getMessage());
        break;
      }
    }
    return null;
  }

  private char[] stringGenerator() {
    char[] str = new char[10];
    Random r = new Random();
    for (int i=0; i < r.nextInt(10) + 1; i++) {
      if (r.nextInt(2) == 0) {
        str[i] = (char) (r.nextInt(26) + 'a');
      } else {
        str[i] = (char) (r.nextInt(26) + 'A');
      }
    }
    return str;
  }
}
