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

import org.apache.commons.cli.ParseException;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Client for Map Reduce example.
 */
public final class MapReduce {
  private static final Logger LOG = Logger.getLogger(MapReduce.class.getName());

  /**
   * The upper limit on the number of Evaluators that the local resourcemanager will hand out concurrently.
   */
  private static final int MAX_NUMBER_OF_EVALUATORS = 3;

  /**
   * Mapper that map task would use.
   */
  @NamedParameter(short_name = "mapper")
  public static final class MapperNP implements Name<Mapper> {
  }

  /**
   * Reducer that map task would use.
   */
  @NamedParameter(short_name = "reducer")
  public static final class ReducerNP implements Name<Reducer> {
  }

  /**
   * Mapper implementation.
   */
  public static class MapperImpl implements Mapper{
    @Inject
    public MapperImpl() {
      System.out.println("Hello Mapper!");
    }

    @Override
    public void map() {

    }
  }

  /**
   * Reducer implementation.
   */
  public static class ReducerImpl implements Reducer {
    @Inject
    public ReducerImpl() {
      System.out.println("Hello Reducer!");
    }

    @Override
    public void reduce() {

    }
  }

  /**
   * @return the configuration of the runtime
   */
  private static Configuration getRuntimeConfiguration() {
    return LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
        .build();
  }

  /**
   * @return the configuration of the MapReduce driver.
   */
  private static Configuration getDriverConfiguration() {
    return DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(MapReduceDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "MapReduce")
        .set(DriverConfiguration.ON_DRIVER_STARTED, MapReduceDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, MapReduceDriver.EvaluatorAllocatedHandler.class)
        .build();
  }

  /**
   * Run the Task driver.
   * @param runtimeConf The runtime configuration (e.g. Local, YARN, etc)
   * @throws InjectionException
   * @throws java.io.IOException
   */
  public static void runTaskDriver(final Configuration runtimeConf)
          throws InjectionException, IOException, ParseException {
    // Merge the configurations to run Driver
    final Configuration driverConf = Tang.Factory.getTang()
            .newConfigurationBuilder(getDriverConfiguration())
            .bindNamedParameter(MapperNP.class, MapperImpl.class)
            .bindNamedParameter(ReducerNP.class, ReducerImpl.class)
            .build();

    DriverLauncher.getLauncher(runtimeConf).run(driverConf);
  }

  /**
   * Start Map Reduce job.
   *
   * @param args command line parameters.
   * @throws BindException      configuration error.
   * @throws InjectionException configuration error.
   */
  public static void main(final String[] args) throws BindException, InjectionException, IOException, ParseException {
    final Configuration runtimeConf = getRuntimeConfiguration();

    runTaskDriver(runtimeConf);
    LOG.log(Level.INFO, "REEF job completed");
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private MapReduce() {
  }
}
