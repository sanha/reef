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

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;

/**
 * A Source Task of MapReduce application.
 */
public final class ReduceTask implements Task {
  private final Reducer reducer;

  @Inject
  private ReduceTask(@Parameter(MapReduce.ReducerNP.class) final Reducer reducer) {
    this.reducer = reducer;
  }

  @Override
  public byte[] call(final byte[] memento) {
    System.out.println("Hello, REEF! This is reduce task");

    for(int i=0; i<10; i++) {
      try {
        reducer.reduce();

        Thread.sleep(1000);
      } catch(InterruptedException e){
        System.err.println(e.getMessage());
        break;
      }
    }

    return null;
  }
}
