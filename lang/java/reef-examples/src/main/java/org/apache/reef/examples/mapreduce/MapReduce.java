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

//import org.apache.reef.client.DriverConfiguration;
//import org.apache.reef.client.DriverLauncher;
//import org.apache.reef.client.LauncherStatus;
//import org.apache.reef.examples.hello.HelloDriver;
//import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
//import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
//import org.apache.reef.util.EnvironmentUtils;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Client for Map Reduce example.
 */
public final class MapReduce {
  private static final Logger LOG = Logger.getLogger(MapReduce.class.getName());

  /**
   * Start Hello REEF job.
   *
   * @param args command line parameters.
   * @throws BindException      configuration error.
   * @throws InjectionException configuration error.
   */
  public static void main(final String[] args) throws BindException, InjectionException {
    LOG.log(Level.INFO, "Hello reef!");
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private MapReduce() {
  }
}
