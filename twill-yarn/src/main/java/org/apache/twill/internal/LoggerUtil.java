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
package org.apache.twill.internal;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.util.ContextInitializer;
import ch.qos.logback.core.joran.spi.JoranException;
import com.google.common.base.Throwables;
import org.apache.twill.internal.logging.KafkaAppender;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.File;
import java.io.StringReader;

/**
 * Utility to configure logging
 */
public class LoggerUtil {
  private final String hostName;
  private final String kafkaZKConnect;
  private final String runnableName;

  public LoggerUtil(String hostName, String kafkaZKConnect, String runnableName) {
    this.hostName = hostName;
    this.kafkaZKConnect = kafkaZKConnect;
    this.runnableName = runnableName;
  }

  public void configureLogger(String configLevel) {
    // Check if SLF4J is bound to logback in the current environment
    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
    if (!(loggerFactory instanceof LoggerContext)) {
      return;
    }

    LoggerContext context = (LoggerContext) loggerFactory;
    context.reset();
    JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(context);

    try {
      File twillLogback = new File(Constants.Files.LOGBACK_TEMPLATE);
      if (twillLogback.exists()) {
        configurator.doConfigure(twillLogback);
      }
      new ContextInitializer(context).autoConfig();
    } catch (JoranException e) {
      throw Throwables.propagate(e);
    }
    doConfigure(configurator, getLogConfig(configLevel));
  }

  private String getLogConfig(String rootLevel) {
    return
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<configuration>\n" +
        "    <appender name=\"KAFKA\" class=\"" + KafkaAppender.class.getName() + "\">\n" +
        "        <topic>" + Constants.LOG_TOPIC + "</topic>\n" +
        "        <hostname>" + hostName + "</hostname>\n" +
        "        <zookeeper>" + kafkaZKConnect + "</zookeeper>\n" +
        appendRunnable() +
        "    </appender>\n" +
        "    <logger name=\"org.apache.twill.internal.logging\" additivity=\"false\" />\n" +
        "    <root level=\"" + rootLevel + "\">\n" +
        "        <appender-ref ref=\"KAFKA\"/>\n" +
        "    </root>\n" +
        "</configuration>";
  }

  private void doConfigure(JoranConfigurator configurator, String config) {
    try {
      configurator.doConfigure(new InputSource(new StringReader(config)));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private String appendRunnable() {
    // RunnableName for AM is null, so append runnable name to log config only if the name is not null.
    if (runnableName == null) {
      return "";
    } else {
      return "        <runnableName>" + runnableName + "</runnableName>\n";
    }
  }

}


