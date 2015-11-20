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

package org.apache.slider.funtest.lifecycle

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.ResourceKeys
import org.apache.slider.api.types.NodeInformationList
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.core.launch.SerializedApplicationReport
import org.apache.slider.funtest.ResourcePaths
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.junit.After
import org.junit.Before
import org.junit.Test

@CompileStatic
@Slf4j
public class AASleepIT extends AgentCommandTestBase
    implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {


  static String NAME = "test-aa-sleep"

  static String TEST_RESOURCE = ResourcePaths.SLEEP_RESOURCES
  static String TEST_METADATA = ResourcePaths.SLEEP_META
  public static final String SLEEP_100 = "SLEEP_100"
  public static final String SLEEP_LONG = "SLEEP_LONG"

  @Before
  public void prepareCluster() {
    setupCluster(NAME)
  }

  @After
  public void destroyCluster() {
    cleanup(NAME)
  }

  @Test
  public void testAASleepIt() throws Throwable {
    describe("Test Anti-Affinity Placement")

    describe "diagnostics"

    slider([ACTION_DIAGNOSTICS, ARG_VERBOSE, ARG_CLIENT, ARG_YARN, ARG_CREDENTIALS])

    describe "list nodes"

    def healthyNodes = listNodes(true)

    def healthyNodeCount = healthyNodes.size()
    describe("Cluster nodes : ${healthyNodeCount}")
    log.info(NodeInformationList.createSerializer().toJson(healthyNodes))

    File launchReportFile = createTempJsonFile();

    int desired = buildDesiredCount(healthyNodeCount)
    def clusterpath = buildClusterPath(NAME)

    SliderShell shell = createSliderApplicationMinPkg(NAME,
        TEST_METADATA,
        TEST_RESOURCE,
        ResourcePaths.SLEEP_APPCONFIG,
        [ARG_RES_COMP_OPT, SLEEP_LONG, ResourceKeys.COMPONENT_INSTANCES, Integer.toString(desired)],
        launchReportFile)

    logShell(shell)

    def appId = ensureYarnApplicationIsUp(launchReportFile)

    //at this point the cluster should exist.
    assertPathExists(
        clusterFS,
        "Cluster parent directory does not exist",
        clusterpath.parent)

    assertPathExists(clusterFS, "Cluster directory does not exist", clusterpath)

    status(0, NAME)

    def expected = buildExpectedCount(desired)
    expectLiveContainerCountReached(NAME, SLEEP_100, expected,
        CONTAINER_LAUNCH_TIMEOUT)

    operations(NAME, loadAppReport(launchReportFile), desired, expected)

    //stop
    freeze(0, NAME,
        [
            ARG_WAIT, Integer.toString(FREEZE_WAIT_TIME),
            ARG_MESSAGE, "final-shutdown"
        ])

    assertInYarnState(appId, YarnApplicationState.FINISHED)
    destroy(0, NAME)

    //cluster now missing
    exists(EXIT_UNKNOWN_INSTANCE, NAME)
  }

  protected int buildExpectedCount(int desired) {
    desired - 1
  }

  protected int buildDesiredCount(int clustersize) {
    clustersize + 1
  }

  protected void operations(String name,
      SerializedApplicationReport appReport,
      int desired,
      int expected ) {


    // now here await for the cluster size to grow: if it does, there's a problem
    ClusterDescription cd
    // spin for a while and fail if the number ever goes above it.
    5.times {
      cd = assertContainersLive(NAME, SLEEP_LONG, expected)
      sleep(1000 * 10)
    }

    // here cluster is still 1 below expected

  }
}
