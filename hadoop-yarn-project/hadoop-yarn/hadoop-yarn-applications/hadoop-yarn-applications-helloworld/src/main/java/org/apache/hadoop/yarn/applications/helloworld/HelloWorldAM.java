/**
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

package org.apache.hadoop.yarn.applications.helloworld;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.log4j.LogManager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * A simple synchronous application master.
 * It launches containers and prints a Hello World! message to the clients.
 * It should help to demonstrate Yarn applications.
 * There is no client provided. Launch this application using the REST API.
 */
public class HelloWorldAM {

  // Logger object
  private static final Log LOG = LogFactory.getLog(HelloWorldAM.class);
  // Configuration
  private Configuration conf = new YarnConfiguration();
  // Handles Resource Manager communication
  @VisibleForTesting
  AMRMClient amRMClient;
  // Handles Node Manager communication
  @VisibleForTesting
  NMClient nmClient;
  // Tracking server for the application master
  @VisibleForTesting
  ServerSocket serverSocket;
  // Server thread that responds to clients
  @VisibleForTesting
  Thread amServer;
  // Configured number of seconds that each container runs for
  @VisibleForTesting
  int containerTimeToLive = 1;
  // Configured number of containers to launch
  @VisibleForTesting
  int numTotalContainers = 1;
  // Configured memory of each container
  @VisibleForTesting
  int containerMemoryMB = 10;
  // Configured number of virtual cores per container
  @VisibleForTesting
  int virtualCoreCountPerContainer = 1;
  // Configured priority of each container request
  @VisibleForTesting
  int requestPriority;
  // Count of failed containers
  @VisibleForTesting
  int numFailedContainers = 0;
  // Security tokens to pass to containers
  @VisibleForTesting
  ByteBuffer containerTokens;
  // Set of completed containers
  @VisibleForTesting
  Set<ContainerId> completedContainers = Sets.newHashSet();

  /**
   * Starting a Hello World application master.
   * @param args input arguments
   */
  public static void main(String[] args) {
    LOG.info("Starting Application Master");
    try {
      HelloWorldAM appMaster = new HelloWorldAM();
      appMaster.init(args);
      appMaster.run();
      if (!appMaster.finish()) {
        LOG.error("Application Master failed");
        LogManager.shutdown();
        System.exit(2);
      }
    } catch (ParseException|YarnException|IOException e) {
      LOG.error("Exception running Application Master", e);
      LogManager.shutdown();
      System.exit(1);
    }
    LOG.info("Application Master succeeded");
    LogManager.shutdown();
  }

  /**
   * Default constructor.
   */
  HelloWorldAM() {
  }

  /**
   * Parse command line options.
   *
   * @param args Command line args
   * @throws ParseException input could not be parsed
   */
  @VisibleForTesting
  void init(String[] args) throws ParseException, NumberFormatException {
    LOG.info("Parsing arguments");
    Options opts = new Options();
    opts.addOption("containers", true,
        "Number of containers to execute. Default 1");
    opts.addOption("runtime", true,
        "Number of seconds each container runs for. Default 1");
    opts.addOption("memory", true,
        "Amount of memory in MB. Default 10");
    opts.addOption("vcores", true,
        "Amount of virtual cores per container. Default 1");
    opts.addOption("priority", true,
        "Application Priority. Default 0");
    opts.addOption("help", false, "Print this help");
    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (cliParser.hasOption("help")) {
      new HelpFormatter().printHelp(
          "java " + this.getClass().toString(), opts);
      System.exit(0);
    }

    String arguments = "";
    for (String arg : args) {
      arguments += (arguments.length()==0 ? "" : " ") + arg;
    }
    LOG.info(String.format("Arguments: %s", arguments));
    numTotalContainers = Math.max(0,
        Integer.parseInt(cliParser.getOptionValue("containers", "1")));
    containerTimeToLive = Math.max(0,
        Integer.parseInt(cliParser.getOptionValue("runtime", "1")));
    requestPriority =
        Integer.parseInt(cliParser.getOptionValue("priority", "0"));
    containerMemoryMB =
        Integer.parseInt(cliParser.getOptionValue("memory", "10"));
    virtualCoreCountPerContainer =
        Integer.parseInt(cliParser.getOptionValue("vcores", "1"));
  }

  /**
   * Yarn specific application master code.
   *
   * @throws YarnException Yarn configuration error
   * @throws IOException Network error has occurred
   */
  @SuppressWarnings("unchecked")
  void run() throws YarnException, IOException {
    ensureAMRMClient();
    ensureNMClient();
    collectContainerTokens();
    registerWithRM();
    requestContainers();
    runContainers();
  }

  /**
   * Starting AM heart beat loop until all containers completed.
   * @throws YarnException allocation or start failed
   * @throws IOException AM or NM connection closed
   */
  private void runContainers() throws YarnException, IOException {
    float progress = 0.0f;
    LOG.info("Starting AM heart beat loop until all containers completed");
    while (completedContainers.size() < numTotalContainers &&
        numFailedContainers == 0) {
      AllocateResponse allocResponse = amRMClient.allocate(progress);

      for (Container allocatedContainer :
          allocResponse.getAllocatedContainers()) {
        ContainerLaunchContext clc = ContainerLaunchContext.newInstance(
            null,
            null,
            Collections.singletonList(String.format("sleep %d",
                containerTimeToLive)),
            null,
            containerTokens.duplicate(),
            null
        );
        nmClient.startContainer(allocatedContainer, clc);
        progress += 0.5 / numTotalContainers;
        LOG.info(String.format("Started container %s",
            allocatedContainer.getId().toString()));
      }

      for (ContainerStatus completedContainer :
          allocResponse.getCompletedContainersStatuses()) {
        LOG.info(String.format("Completed container %s status:%d %s",
            completedContainer.getContainerId().toString(),
            completedContainer.getExitStatus(),
            completedContainer.getDiagnostics()));
        if (completedContainer.getState() == ContainerState.COMPLETE &&
            completedContainers.add(completedContainer.getContainerId())) {
          numFailedContainers +=
              completedContainer.getExitStatus() != 0 ? 1 : 0;
          progress += 0.5 / numTotalContainers;
        }
      }
    }
    LOG.info("Finished heart beat loop");
  }

  /**
   * Create client to Node Manager to start containers.
   */
  private void ensureNMClient() {
    LOG.info("Create client to Node Manager to start containers");
    if (nmClient == null) {
      nmClient = NMClient.createNMClient();
    }
    nmClient.init(conf);
    nmClient.start();
  }

  /**
   * Create client to Resource Manager to allocate containers.
   */
  private void ensureAMRMClient() {
    LOG.info("Create client to Resource Manager to allocate containers");
    if (amRMClient == null) {
      amRMClient = AMRMClient.createAMRMClient();
    }
    amRMClient.init(conf);
    amRMClient.start();
  }

  /**
   * Remove AMRM token from container token list.
   * @throws IOException could not write to token storage
   */
  private void collectContainerTokens() throws IOException {
    LOG.info("Collect Resource Manager tokens to pass to containers");
    Credentials cred = UserGroupInformation.getCurrentUser().getCredentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    cred.writeTokenStorageToStream(dob);
    // Now remove the AM->RM token so that containers cannot access it.
    Iterator<Token<?>> iter = cred.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      LOG.info(token);
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    containerTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
  }

  /**
   * Request containers.
   */
  @SuppressWarnings("unchecked")
  private void requestContainers() {
    LOG.info(
        String.format("Request %d containers from RM", numTotalContainers));
    for (int i = 0; i < numTotalContainers; ++i) {
      ContainerRequest containerAsk =
          new ContainerRequest(
              Resource.newInstance(
                  containerMemoryMB, virtualCoreCountPerContainer),
              null,
              null,
              Priority.newInstance(requestPriority));
      amRMClient.addContainerRequest(containerAsk);
    }
  }

  /**
   * Register with resource manager.
   * @throws IOException network error
   * @throws YarnException registration failed
   */
  private void registerWithRM() throws IOException, YarnException {
    serverSocket = new ServerSocket(0);
    final int serverPort = serverSocket.getLocalPort();
    final String serverName = InetAddress.getLocalHost().getHostAddress();
    final String serverUrl =
        String.format("http://%s:%d", serverName, serverPort);
    LOG.info(String.format("Launching tracking url %s", serverUrl));
    amServer = new Thread(new TrackingURLServer());
    amServer.start();

    LOG.info("Register with ResourceManager");
    amRMClient
        .registerApplicationMaster(
            serverName,
            serverPort,
            serverUrl);
  }
  /**
   * Yarn specific application master cleanup code.
   * @return application status
   */
  boolean finish() {
    LOG.info("Application completed.");
    LOG.info("Stopping tracking url");
    try {
      serverSocket.close();
      amServer.join();
    } catch (IOException e) {
      LOG.error("Socket close error", e);
    } catch (InterruptedException e) {
      LOG.error("Interrupted exception", e);
    }

    LOG.info("Stopping running containers");
    nmClient.stop();

    FinalApplicationStatus appStatus =
        numFailedContainers == 0 &&
            completedContainers.size() == numTotalContainers ?
            FinalApplicationStatus.SUCCEEDED :
            FinalApplicationStatus.FAILED;
    String appMessage =
        "Application results: " +
            "requested=" + numTotalContainers
            + ", completed=" + completedContainers.size() + ", failed="
            + numFailedContainers;
    LOG.info(appMessage);
    try {
      amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
    } catch (Exception e) {
      LOG.error("Failed to unregister application", e);
    }

    LOG.info("Stopping AM-RM communication");
    amRMClient.stop();

    return appStatus == FinalApplicationStatus.SUCCEEDED;
  }

  class TrackingURLServer implements Runnable {
    public void run() {
      while (!serverSocket.isClosed()) {
        try {
          Socket clientSocket = serverSocket.accept();
          ByteBuffer response = StandardCharsets.US_ASCII.encode(
              "HTTP/1.1 200 ok\r\n" +
                  "Content-Type: text/plain\r\n" +
                  "Content-Length: 12\r\n" +
                  "\r\n" +
                  "Hello world!");
          clientSocket.getOutputStream().write(response.array());
          clientSocket.getOutputStream().flush();
          clientSocket.close();
        } catch (IOException ignored) {
        }
      }
    }
  }
}
