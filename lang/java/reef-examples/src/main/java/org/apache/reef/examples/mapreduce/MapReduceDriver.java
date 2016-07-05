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

import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Driver code for the MapReduce Application.
 */
@Unit
public final class MapReduceDriver {

  private static final Logger LOG = Logger.getLogger(MapReduceDriver.class.getName());

  private final EvaluatorRequestor requestor;

  /**
   * Job driver constructor - instantiated via TANG.
   *
   * @param requestor evaluator requestor object used to create new evaluator containers.
   */
  @Inject
  private MapReduceDriver(
      final EvaluatorRequestor requestor,
      @Parameter(MapReduce.MapperNP.class) final Mapper mapper,
      @Parameter(MapReduce.ReducerNP.class) final Reducer reducer) {
    this.requestor = requestor;
    LOG.log(Level.FINE, "Instantiated 'MapReduceDriver'");
  }

  /**
   * Handles the StartTime event: Request as single Evaluator.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      MapReduceDriver.this.requestor.newRequest()
          .setNumber(3)
          .setMemory(64)
          .submit();
      LOG.log(Level.INFO, "Requested Evaluator.");
    }
  }

  /**
   * Handles AllocatedEvaluator: Submit source, map, and reduce tasks.
   */
  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Evaluator is ready");
      LOG.log(Level.INFO, "Submitting source task of MapReduce application.");
      final Configuration sourceTaskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, "SourceTask")
          .set(TaskConfiguration.TASK, SourceTask.class)
          .build();
      allocatedEvaluator.submitTask(sourceTaskConfiguration);

      /*LOG.log(Level.INFO, "Submitting map task of MapReduce application.");
      final Configuration mapTaskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, "MapTask")
          .set(TaskConfiguration.TASK, MapTask.class)
          .build();
      allocatedEvaluator.submitTask(mapTaskConfiguration);

      LOG.log(Level.INFO, "Submitting reduce task of MapReduce application.");
      final Configuration reduceTaskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, "ReduceTask")
          .set(TaskConfiguration.TASK, ReduceTask.class)
          .build();
      allocatedEvaluator.submitTask(reduceTaskConfiguration);*/
    }
  }
}
