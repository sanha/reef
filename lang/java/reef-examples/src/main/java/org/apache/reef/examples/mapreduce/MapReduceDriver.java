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

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
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

  private final int nTotalTask = 3;

  private int nActiveTask = 0;

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
          .setNumber(nTotalTask)
          .setMemory(64)
          .submit();
      LOG.log(Level.INFO, "Requested Evaluator.");
    }
  }

  /**
   * Handles AllocatedEvaluator: Submit contexts to identify evlauator.
   */
  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Submitting an id context to AllocatedEvaluator: {0}", allocatedEvaluator);
      final Configuration contextConfiguration = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, "MapReduceContext-" + Integer.toString(nActiveTask++))
          .build();
      allocatedEvaluator.submitContext(contextConfiguration);
    }
  }

  /**
   * ActiveContext handler : Submit source, map, and reduce tasks.
   */
  public class ContextActiveHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext activeContext) {

      LOG.log(Level.FINE, "Got active context: {0}", activeContext.getId());

      if (activeContext.getId().equals("MapReduceContext-0")) {
        LOG.log(Level.INFO, "Submitting source task of MapReduce application.");
        final Configuration sourceTaskConfiguration = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, "SourceTask")
            .set(TaskConfiguration.TASK, SourceTask.class)
            .build();
        activeContext.submitTask(sourceTaskConfiguration);
      } else if (activeContext.getId().equals("MapReduceContext-1")) {
        LOG.log(Level.INFO, "Submitting map task of MapReduce application.");
        final Configuration mapTaskConfiguration = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, "MapTask")
            .set(TaskConfiguration.TASK, MapTask.class)
            .build();
        activeContext.submitTask(mapTaskConfiguration);
      } else if (activeContext.getId().equals("MapReduceContext-2")) {
        LOG.log(Level.INFO, "Submitting reduce task of MapReduce application.");
        final Configuration reduceTaskConfiguration = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, "ReduceTask")
            .set(TaskConfiguration.TASK, ReduceTask.class)
            .build();
        activeContext.submitTask(reduceTaskConfiguration);
      }
    }
  }
}

