/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.twill.yarn;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.google.common.base.Stopwatch;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.Constants;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * test changing log level for a twil runnable
 */
public class LogLevelChangeTestRun extends BaseYarnTest {
  public static final Logger LOG = LoggerFactory.getLogger(LogLevelChangeTestRun.class);

  /**
   * twill runnable
   */
  public static final class LogLevelTestRunnable extends AbstractTwillRunnable {
    private volatile Thread runThread;

    @Override
    public void run() {
      this.runThread = Thread.currentThread();
      while (!Thread.interrupted()) {
        try {
          TimeUnit.MILLISECONDS.sleep(500);
        } catch (InterruptedException e) {
          break;
        }
      }
    }

    @Override
    public void stop() {
      if (runThread != null) {
        runThread.interrupt();
      }
    }
  }

  /**
   * A test TwillApplication to test setting log level to DEBUG.
   */
  public static final class LogLevelTestApplication implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName(LogLevelTestApplication.class.getSimpleName())
        .withRunnable()
        .add(LogLevelTestRunnable.class.getSimpleName(), new LogLevelTestRunnable()).noLocalFiles()
        .anyOrder()
        .build();
    }

  }

  @Test
  public void testMultiThreadLogBack() throws Exception {
    LOG.info("Object for Logger {}", LoggerFactory.getLogger(LogLevelChangeTestRun.class).hashCode());
    ExecutorService executorService = Executors.newFixedThreadPool(5);
    final CountDownLatch latch = new CountDownLatch(5);
    final CountDownLatch changeLatch = new CountDownLatch(1);
    final Object sync = new Object();

    for (int i = 0; i < 5; i++) {
      executorService.execute(new Runnable() {
        @Override
        public void run() {
          synchronized (sync) {
            if (changeLatch.getCount() == 1) {
              LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
              ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(LogLevelChangeTestRun.class);
              System.out.println("Current rootLogger level is " + rootLogger.getLevel());
              System.out.println("Setting rootLogger level to " + Level.INFO);
              rootLogger.setLevel(Level.ERROR);
              rootLogger.setLevel(Level.INFO);
              changeLatch.countDown();
            }
          }
          try {
            changeLatch.await();
          } catch (Exception e) {
            e.printStackTrace();
          }
          LOG.info("Hashcode for Logger {}", LoggerFactory.getLogger(LogLevelChangeTestRun.class).hashCode());
          LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
          ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(LogLevelChangeTestRun.class);
          System.out.println("Current rootLogger level is " + rootLogger.getLevel());
          latch.countDown();
        }
      });
    }

    while (latch.getCount() != 0) {
      TimeUnit.MILLISECONDS.sleep(200);
    }
    executorService.shutdown();
  }

  @Test
  public void testDebugLogLevel()throws Exception {
    YarnTwillRunnerService runner = getTwillRunner();
    runner.start();

    // Set log level to DEBUG
    TwillController controller = runner.prepare(new LogLevelTestApplication())
      .setLogLevel(LogEntry.Level.ERROR)
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
      .start();

    // Lets wait until the service is running
    final CountDownLatch running = new CountDownLatch(1);
    controller.onRunning(new Runnable() {
      @Override
      public void run() {
        running.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    Assert.assertTrue(running.await(200, TimeUnit.SECONDS));

    // assert that log level is ERROR
    waitForLogLevel(controller, LogLevelTestRunnable.class.getSimpleName(),
                    200L, TimeUnit.SECONDS, LogEntry.Level.ERROR);

    // change the log level to INFO
    controller.setRunnableLogLevel(LogLevelTestRunnable.class.getSimpleName(), LogEntry.Level.INFO).get();

    // assert log level has changed to INFO
    waitForLogLevel(controller, LogLevelTestRunnable.class.getSimpleName(),
                    200L, TimeUnit.SECONDS, LogEntry.Level.INFO);
    // stop
    controller.terminate().get(120, TimeUnit.SECONDS);

    // Sleep a bit for full cleanup
    TimeUnit.SECONDS.sleep(2);
  }

  // Need helper method here to wait for getting resource report because {@link TwillController#getResourceReport()}
  // could return null if the application has not fully started.
  private void waitForLogLevel(TwillController controller, String runnable, long timeout,
                               TimeUnit timeoutUnit, LogEntry.Level expected) throws InterruptedException {

    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    LogEntry.Level actual = null;
    do {
      ResourceReport report = controller.getResourceReport();
      if (report == null || report.getRunnableResources(runnable) == null) {
        continue;
      }
      for (TwillRunResources resources : report.getRunnableResources(runnable)) {
        actual = resources.getLogLevel();
        if (actual != null && actual.equals(expected)) {
          break;
        }
      }
      TimeUnit.MILLISECONDS.sleep(100);
    } while (stopwatch.elapsedTime(timeoutUnit) < timeout);

    Assert.assertEquals(expected, actual);
  }
}
