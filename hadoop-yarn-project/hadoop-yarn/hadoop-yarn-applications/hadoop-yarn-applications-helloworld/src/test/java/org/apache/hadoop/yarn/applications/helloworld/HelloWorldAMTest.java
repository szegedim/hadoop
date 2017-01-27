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

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * Unit test that tests the class with mock AM and NM communication.
 */
public class HelloWorldAMTest {

  @Test
  public void testHelloWorldInit() throws Exception {
    HelloWorldAM helloWorld = new HelloWorldAM();

    String[] empty = {};
    helloWorld.init(empty);
  }

  @Test
  public void testHelloWorldParams() throws Exception {
    HelloWorldAM helloWorldAM = new HelloWorldAM();

    String[] params = {"-runtime", "30", "-memory", "1025", "-vcores", "3",
        "-containers", "2", "-priority", "5"};
    helloWorldAM.init(params);
    assertEquals(30, helloWorldAM.containerTimeToLive);
    assertEquals(1025, helloWorldAM.containerMemoryMB);
    assertEquals(3, helloWorldAM.virtualCoreCountPerContainer);
    assertEquals(2, helloWorldAM.numTotalContainers);
    assertEquals(5, helloWorldAM.requestPriority);
  }

  @Test
  public void testHelloWorldRun() throws Exception {
    HelloWorldAM helloWorldAM = new HelloWorldAM();

    String[] empty = {};
    helloWorldAM.init(empty);

    setupSingleContainerRun(helloWorldAM, 0);

    helloWorldAM.run();
    assertTrue(helloWorldAM.finish());
    assertTrue(helloWorldAM.serverSocket.isClosed());
    assertTrue(!helloWorldAM.amServer.isAlive());
    assertTrue(helloWorldAM.numFailedContainers == 0);
    assertTrue(helloWorldAM.containerTokens.capacity() > 0);
    assertTrue(helloWorldAM.completedContainers.size() == 1);
  }

  @Test
  public void testHelloWorldRunFail() throws Exception {
    HelloWorldAM helloWorldAM = new HelloWorldAM();

    String[] empty = {};
    helloWorldAM.init(empty);

    setupSingleContainerRun(helloWorldAM, 1);
    helloWorldAM.nmClient = mock(NMClient.class);

    helloWorldAM.run();
    assertFalse(helloWorldAM.finish());
    assertTrue(helloWorldAM.serverSocket.isClosed());
    assertTrue(!helloWorldAM.amServer.isAlive());
    assertTrue(helloWorldAM.numFailedContainers == 1);
    assertTrue(helloWorldAM.containerTokens.capacity() > 0);
    assertTrue(helloWorldAM.completedContainers.size() == 1);
  }

  private void setupSingleContainerRun(HelloWorldAM helloWorld, int status)
      throws YarnException, IOException {
    Resource maxResource = mock(Resource.class);
    when(maxResource.getMemorySize()).thenReturn(8192L);
    when(maxResource.getVirtualCores()).thenReturn(16);
    RegisterApplicationMasterResponse mockResponse =
        mock(RegisterApplicationMasterResponse.class);
    when(mockResponse.getMaximumResourceCapability()).thenReturn(maxResource);
    ContainerId mockContainerId = mock(ContainerId.class);
    Container mockContainer = mock(Container.class);
    when(mockContainer.getId()).thenReturn(mockContainerId);
    List<Container> containers =
        Collections.singletonList(mockContainer);
    AllocateResponse mockAllocateResponse = mock(AllocateResponse.class);
    when(mockAllocateResponse.getAllocatedContainers())
        .thenReturn(containers);
    ContainerStatus completedContainerStatus = mock(ContainerStatus.class);
    when(completedContainerStatus.getContainerId()).thenReturn(mockContainerId);
    when(completedContainerStatus.getExitStatus()).thenReturn(status);
    when(completedContainerStatus.getState())
        .thenReturn(ContainerState.COMPLETE);
    AllocateResponse mockAllocateResponseCompleted =
        mock(AllocateResponse.class);
    when(mockAllocateResponseCompleted.getAllocatedContainers())
        .thenReturn(Collections.emptyList());
    when(mockAllocateResponseCompleted.getCompletedContainersStatuses())
        .thenReturn(Collections.singletonList(completedContainerStatus));

    helloWorld.amRMClient = mock(AMRMClient.class);
    when(helloWorld.amRMClient.
        registerApplicationMaster(anyString(), anyInt(), anyString())).
        thenReturn(mockResponse);
    when(helloWorld.amRMClient.
        allocate(0)).
        thenReturn(mockAllocateResponse);
    when(helloWorld.amRMClient.
        allocate(0.5f)).
        thenReturn(mockAllocateResponseCompleted);

    helloWorld.nmClient = mock(NMClient.class);
  }
}
