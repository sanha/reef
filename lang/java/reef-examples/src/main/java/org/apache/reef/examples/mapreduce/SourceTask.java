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

import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.task.Task;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.Random;

/**
 * A Source Task of MapReduce application.
 */
public final class SourceTask implements Task {

  /**
   * String codec is used to encode the results to send to the map task.
   */
  private static final ObjectSerializableCodec<String> CODEC_STR = new ObjectSerializableCodec<>();

  /**
   * Network Connection Service for source task.
   */
  private NetworkConnectionService sourceNCS;

  private EventHandler eventHandler;

  @Inject
  private SourceTask(final IdentifierFactory idf) {
    try {
      Identifier factoryId = idf.getNewInstance("SourceMapFactoryId");
      Identifier localEndId = idf.getNewInstance("SourceNCS");
      this.eventHandler = null;

      Injector injector = Tang.Factory.getTang().newInjector();
      this.sourceNCS = injector.getInstance(NetworkConnectionService.class);
      sourceNCS.registerConnectionFactory(factoryId, CODEC_STR, eventHandler, null, localEndId);
    } catch(InjectionException e){
      System.err.println(e.getMessage());
    }
  }

  @Override
  public byte[] call(final byte[] memento) {
    System.out.println("Hello, REEF! This is source task");
    char[] str;
    for(int i=0; i<10; i++) {
      try {
        str = stringGenerator();
        System.out.println(str);

        Thread.sleep(1000);
      } catch(InterruptedException e){
        System.err.println(e.getMessage());
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
