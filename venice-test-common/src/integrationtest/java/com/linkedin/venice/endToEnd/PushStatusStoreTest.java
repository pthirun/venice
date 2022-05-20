package com.linkedin.venice.endToEnd;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.controller.init.ClusterLeaderInitializationRoutine;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.IncrementalPushVersionsResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.DaVinciTestContext;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.pushstatushelper.PushStatusStoreRecordDeleter;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.common.PushStatusStoreUtils.*;
import static com.linkedin.venice.hadoop.VenicePushJob.*;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.*;
import static com.linkedin.venice.meta.IngestionMode.*;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.*;
import static com.linkedin.venice.utils.TestPushUtils.*;
import static org.testng.Assert.*;


public class PushStatusStoreTest {
  private static final int TEST_TIMEOUT = 60_000; // ms
  private static final int NUMBER_OF_SERVERS = 2;
  private static final int PARTITION_COUNT = 2;
  private static final int REPLICATION_FACTOR = 2;

  private VeniceClusterWrapper cluster;
  private ControllerClient controllerClient;
  private D2Client d2Client;
  private PushStatusStoreReader reader;
  private String storeName;
  private VeniceControllerWrapper parentController;
  private ZkServerWrapper parentZkServer;

  @BeforeClass
  public void setUp() {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));
    // all tests in this class will be reading incremental push status from push status store
    extraProperties.setProperty(USE_PUSH_STATUS_STORE_FOR_INCREMENTAL_PUSH, String.valueOf(true));
    // Need to set this up as test testParentControllerAutoMaterializeDaVinciPushStatusSystemStore will get stuck.
    extraProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, String.valueOf(false));
    Utils.thisIsLocalhost();
    cluster = ServiceFactory.getVeniceCluster(
        1,
        NUMBER_OF_SERVERS,
        1,
        REPLICATION_FACTOR,
        10000,
        false,
        false,
        extraProperties);
    controllerClient = cluster.getControllerClient();
    d2Client = D2TestUtils.getAndStartD2Client(cluster.getZk().getAddress());
    reader = new PushStatusStoreReader(d2Client, TimeUnit.MINUTES.toSeconds(10));
    extraProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, String.valueOf(true));
    parentZkServer = ServiceFactory.getZkServer();
    parentController = ServiceFactory.getVeniceParentController(cluster.getClusterName(), parentZkServer.getAddress(),
        cluster.getKafka(), cluster.getVeniceControllers().toArray(new VeniceControllerWrapper[0]),
        new VeniceProperties(extraProperties), false);
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(reader);
    D2ClientUtils.shutdownClient(d2Client);
    Utils.closeQuietlyWithErrorLogged(controllerClient);
    Utils.closeQuietlyWithErrorLogged(parentController);
    Utils.closeQuietlyWithErrorLogged(cluster);
    Utils.closeQuietlyWithErrorLogged(parentZkServer);
  }

  @BeforeMethod
  public void setUpStore() {
    storeName = Utils.getUniqueString("store");
    String owner = "test";
    // set up push status store
    TestUtils.assertCommand(controllerClient.createNewStore(storeName, owner, DEFAULT_KEY_SCHEMA, "\"string\""));
    TestUtils.createMetaSystemStore(controllerClient, storeName, Optional.of(logger));
    TestUtils.assertCommand(controllerClient.updateStore(storeName, new UpdateStoreQueryParams()
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
        .setLeaderFollowerModel(true)
        .setPartitionCount(PARTITION_COUNT)
        .setAmplificationFactor(1)
        .setIncrementalPushEnabled(true)));
    String daVinciPushStatusSystemStoreName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName);
    VersionCreationResponse versionCreationResponseForDaVinciPushStatusSystemStore =
        controllerClient.emptyPush(daVinciPushStatusSystemStoreName, "test_da_vinci_push_status_system_store_push_1", 10000);
    assertFalse(versionCreationResponseForDaVinciPushStatusSystemStore.isError(),
        "New version creation for Da Vinci push status system store: " + daVinciPushStatusSystemStoreName + " should success, but got error: "
            + versionCreationResponseForDaVinciPushStatusSystemStore.getError());
    TestUtils.waitForNonDeterministicPushCompletion(versionCreationResponseForDaVinciPushStatusSystemStore.getKafkaTopic(),
        controllerClient, 30, TimeUnit.SECONDS, Optional.empty());
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT * 2)
  public void testKafkaPushJob(boolean isIsolated) throws Exception {
    Properties h2vProperties = getH2VProperties();
    // setup initial version
    runH2V(h2vProperties, 1, cluster);

    Map<String, Object> extraBackendConfigMap = isIsolated ? TestUtils.getIngestionIsolationPropertyMap() : new HashMap<>();
    extraBackendConfigMap.put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true);
    extraBackendConfigMap.put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 10);
    extraBackendConfigMap.put(PUSH_STATUS_STORE_ENABLED, true);

    try (DaVinciClient<Integer, Integer> daVinciClient = ServiceFactory.getGenericAvroDaVinciClientWithRetries(
        storeName, cluster.getZk().getAddress(), new DaVinciConfig(), extraBackendConfigMap)) {
      daVinciClient.subscribeAll().get();
      runH2V(h2vProperties, 2, cluster);
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        assertEquals(reader.getPartitionStatus(storeName, 2, 0, Optional.empty()).size(), 1);
      });
    }
    Admin admin = cluster.getVeniceControllers().get(0).getVeniceAdmin();
    String pushStatusStoreTopic =
        Version.composeKafkaTopic(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName), 1);
    assertTrue(admin.isResourceStillAlive(pushStatusStoreTopic));
    assertFalse(admin.isTopicTruncated(pushStatusStoreTopic));
    TestUtils.assertCommand(controllerClient.disableAndDeleteStore(storeName));

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      assertFalse(admin.isResourceStillAlive(pushStatusStoreTopic));
      assertTrue(!admin.getTopicManager().containsTopic(pushStatusStoreTopic)
          || admin.isTopicTruncated(pushStatusStoreTopic));
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testIncrementalPush() throws Exception {
    VeniceProperties backendConfig = getBackendConfigBuilder().build();
    Properties h2vProperties = getH2VProperties();
    runH2V(h2vProperties, 1, cluster);
    try (DaVinciClient daVinciClient = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, new DaVinciConfig(), backendConfig)) {
      daVinciClient.subscribeAll().get();
      h2vProperties = getH2VProperties();
      h2vProperties.setProperty(INCREMENTAL_PUSH, "true");
      runH2V(h2vProperties, 1, cluster);
      assertEquals(daVinciClient.get(1).get().toString(), "name 1");
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testIncrementalPushStatusStoredInPushStatusStore() throws Exception {
    Properties h2vProperties = getH2VProperties();
    runH2V(h2vProperties, 1, cluster);
    try (AvroGenericStoreClient storeClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setD2Client(d2Client)
            .setD2ServiceName(D2TestUtils.DEFAULT_TEST_SERVICE_NAME))) {
      h2vProperties = getH2VProperties();
      h2vProperties.setProperty(INCREMENTAL_PUSH, "true");
      int expectedVersionNumber = 1;
      long h2vStart = System.currentTimeMillis();
      String jobName = Utils.getUniqueString("batch-job-" + expectedVersionNumber);
      try (VenicePushJob job = new VenicePushJob(jobName, h2vProperties)) {
        job.run();
        cluster.waitVersion(storeName, expectedVersionNumber, controllerClient);
        logger.info("**TIME** H2V" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - h2vStart));
        assertEquals(storeClient.get(1).get().toString(), "name 1");
        Optional<String> incPushVersion = job.getIncrementalPushVersion();
        for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
          Map<CharSequence, Integer> statuses = reader.getPartitionStatus(
              storeName, 1, partitionId, incPushVersion, Optional.of(SERVER_INCREMENTAL_PUSH_PREFIX));
          assertNotNull(statuses);
          assertEquals(statuses.size(), REPLICATION_FACTOR);
          for (Integer status : statuses.values()) {
            assertTrue(ExecutionStatus.isIncrementalPushStatus(status));
          }
        }
      }
    }
  }

  /* The following test is targeted at verifying the behavior of controller when queryJobStatus is invoked for inc-push */
  @Test(timeOut = TEST_TIMEOUT)
  public void testIncrementalPushStatusReadingFromPushStatusStoreInController() throws Exception {
    Properties h2vProperties = getH2VProperties();
    runH2V(h2vProperties, 1, cluster);
    try (AvroGenericStoreClient storeClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setD2Client(d2Client)
            .setD2ServiceName(D2TestUtils.DEFAULT_TEST_SERVICE_NAME))) {
      h2vProperties.setProperty(INCREMENTAL_PUSH, "true");
      int expectedVersionNumber = 1;
      long h2vStart = System.currentTimeMillis();
      String jobName = Utils.getUniqueString("batch-job-" + expectedVersionNumber);
      try (VenicePushJob job = new VenicePushJob(jobName, h2vProperties)) {
        job.run();
        cluster.waitVersion(storeName, expectedVersionNumber, controllerClient);
        logger.info("**TIME** H2V" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - h2vStart));
        assertEquals(storeClient.get(1).get().toString(), "name 1");
        Optional<String> incPushVersion = job.getIncrementalPushVersion();
        // verify partition replicas have reported their status to the push status store
        Map<Integer, Map<CharSequence, Integer>> pushStatusMap = reader.getPartitionStatuses(storeName, 1, incPushVersion.get(), 2);
        assertNotNull(pushStatusMap, "Server incremental push status cannot be null");
        assertEquals(pushStatusMap.size(), PARTITION_COUNT, "Incremental push status of some partitions is missing");
        for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
          Map<CharSequence, Integer> pushStatus = pushStatusMap.get(partitionId);
          assertNotNull(pushStatus, "Push status of a partition is missing");
          for (Integer status : pushStatus.values()) {
            assertEquals(status.intValue(), ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED.getValue());
          }
        }

        // expect NOT_CREATED when all non-existing incremental push version is used to query the status
        JobStatusQueryResponse response = controllerClient.queryJobStatus(job.getTopicToMonitor(), Optional.of("randomIPVersion"));
        assertEquals(response.getStatus(), ExecutionStatus.NOT_CREATED.name());

        // verify that controller responds with EOIP when all partitions have sufficient replicas with EOIP
        response = controllerClient.queryJobStatus(job.getTopicToMonitor(), job.getIncrementalPushVersion());
        assertEquals(response.getStatus(), ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED.name());

        PushStatusStoreRecordDeleter statusStoreDeleter = new PushStatusStoreRecordDeleter(
            cluster.getLeaderVeniceController().getVeniceHelixAdmin().getVeniceWriterFactory());

        // after deleting the the inc push status belonging to just one partition we should expect
        // SOIP from the controller since other partition has replicas with EOIP status
        statusStoreDeleter.deletePartitionIncrementalPushStatus(storeName, 1, incPushVersion.get(), 1)
            .get();
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          // N.B.: Even though we block on the deleter's future, that only means the delete message is persisted into
          // Kafka, but querying the system store may still yield a stale result, hence the need for retrying.
          JobStatusQueryResponse jobStatusQueryResponse =
              controllerClient.queryJobStatus(job.getTopicToMonitor(), job.getIncrementalPushVersion());
          assertEquals(jobStatusQueryResponse.getStatus(), ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED.name());
        });

        // expect NOT_CREATED when statuses of all partitions are not available in the push status store
        statusStoreDeleter.deletePartitionIncrementalPushStatus(storeName, 1, incPushVersion.get(), 0)
            .get();
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          JobStatusQueryResponse jobStatusQueryResponse =
              controllerClient.queryJobStatus(job.getTopicToMonitor(), job.getIncrementalPushVersion());
          assertEquals(jobStatusQueryResponse.getStatus(), ExecutionStatus.NOT_CREATED.name());
        });
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testAutomaticPurge() throws Exception {
    VeniceProperties backendConfig = getBackendConfigBuilder().build();
    Properties h2vProperties = getH2VProperties();
    // setup initial version
    runH2V(h2vProperties, 1, cluster);
    try (DaVinciClient daVinciClient = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, new DaVinciConfig(), backendConfig)) {
      daVinciClient.subscribeAll().get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        assertEquals(reader.getPartitionStatus(storeName, 1, 0, Optional.empty()).size(), 1);
      });
      runH2V(h2vProperties, 2, cluster);
      runH2V(h2vProperties, 3, cluster);
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        assertEquals(reader.getPartitionStatus(storeName, 1, 0, Optional.empty()).size(), 0);
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testParentControllerAutoMaterializeDaVinciPushStatusSystemStore() {
    try (ControllerClient parentControllerClient = new ControllerClient(cluster.getClusterName(),
        parentController.getControllerUrl())) {
      String zkSharedDaVinciPushStatusSchemaStoreName =
          AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getSystemStoreName();
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        Store readOnlyStore = parentController.getVeniceAdmin()
            .getReadOnlyZKSharedSystemStoreRepository()
            .getStore(zkSharedDaVinciPushStatusSchemaStoreName);
        assertNotNull(readOnlyStore, "Store: " + zkSharedDaVinciPushStatusSchemaStoreName + " should be initialized by "
            + ClusterLeaderInitializationRoutine.class.getSimpleName());
        assertTrue(readOnlyStore.isHybrid(),
            "Store: " + zkSharedDaVinciPushStatusSchemaStoreName + " should be configured to hybrid");
      });
      String userStoreName = Utils.getUniqueString("new-user-store");
      NewStoreResponse
          newStoreResponse = parentControllerClient.createNewStore(userStoreName, "venice-test", DEFAULT_KEY_SCHEMA, "\"string\"");
      assertFalse(newStoreResponse.isError(), "Unexpected new store creation failure");
      String daVinciPushStatusSystemStoreName =
          VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(userStoreName);
      TestUtils.waitForNonDeterministicPushCompletion(Version.composeKafkaTopic(daVinciPushStatusSystemStoreName, 1),
          parentControllerClient, 30, TimeUnit.SECONDS, Optional.empty());
      Store daVinciPushStatusSystemStore = parentController.getVeniceAdmin().getStore(cluster.getClusterName(), daVinciPushStatusSystemStoreName);
      assertEquals(daVinciPushStatusSystemStore.getLargestUsedVersionNumber(), 1);

      // Do empty pushes to increase the system store's version
      final int emptyPushAttempt = 2;
      for (int i = 0; i < emptyPushAttempt; i++) {
        final int newVersion = parentController.getVeniceAdmin()
            .incrementVersionIdempotent(cluster.getClusterName(), daVinciPushStatusSystemStoreName,
                "push job ID placeholder " + i, 1, 1)
            .getNumber();
        parentController.getVeniceAdmin().writeEndOfPush(cluster.getClusterName(), daVinciPushStatusSystemStoreName, newVersion, true);
        TestUtils.waitForNonDeterministicPushCompletion(Version.composeKafkaTopic(daVinciPushStatusSystemStoreName, newVersion),
            parentControllerClient, 30, TimeUnit.SECONDS, Optional.empty());
      }
      daVinciPushStatusSystemStore = parentController.getVeniceAdmin().getStore(cluster.getClusterName(), daVinciPushStatusSystemStoreName);
      final int systemStoreCurrVersionBeforeBeingDeleted = daVinciPushStatusSystemStore.getLargestUsedVersionNumber();
      assertEquals(systemStoreCurrVersionBeforeBeingDeleted, 1 + emptyPushAttempt);

      TestUtils.assertCommand(parentControllerClient.disableAndDeleteStore(userStoreName));
      // Both the system store and user store should be gone at this point
      assertNull(parentController.getVeniceAdmin().getStore(cluster.getClusterName(), userStoreName));
      assertNull(parentController.getVeniceAdmin().getStore(cluster.getClusterName(), daVinciPushStatusSystemStoreName));

      // Create the same regular store again
      TestUtils.assertCommand(
          parentControllerClient.createNewStore(userStoreName, "venice-test", DEFAULT_KEY_SCHEMA, "\"string\""),
          "Unexpected new store creation failure");

      // The re-created/materialized per-user store system store should contain a continued version from its last life
      daVinciPushStatusSystemStore = parentController.getVeniceAdmin().getStore(cluster.getClusterName(), daVinciPushStatusSystemStoreName);
      // TODO: Fix non-deterministic bug where (very rarely) the below assertion fails with "expected [4] but found [1]"
      assertEquals(daVinciPushStatusSystemStore.getLargestUsedVersionNumber(), systemStoreCurrVersionBeforeBeingDeleted + 1);

      TestUtils.waitForNonDeterministicPushCompletion(Version.composeKafkaTopic(daVinciPushStatusSystemStoreName, systemStoreCurrVersionBeforeBeingDeleted + 1),
          parentControllerClient, 30, TimeUnit.SECONDS, Optional.empty());
    }
  }

  private PropertyBuilder getBackendConfigBuilder() {
    return DaVinciTestContext.getDaVinciPropertyBuilder(cluster.getZk().getAddress())
        .put(PUSH_STATUS_STORE_ENABLED, true);
  }

  private Properties getH2VProperties() throws Exception {
    // Setup H2V job properties.
    // Produce input data.
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    writeSimpleAvroFileWithIntToStringSchema(inputDir, true);
    return defaultH2VProps(cluster, inputDirPath, storeName);
  }

  private void runH2V(Properties h2vProperties, int expectedVersionNumber, VeniceClusterWrapper cluster) {
    long h2vStart = System.currentTimeMillis();
    String jobName = Utils.getUniqueString("batch-job-" + expectedVersionNumber);
    try (VenicePushJob job = new VenicePushJob(jobName, h2vProperties)) {
      job.run();
      String storeName = (String) h2vProperties.get(VenicePushJob.VENICE_STORE_NAME_PROP);
      cluster.waitVersion(storeName, expectedVersionNumber, controllerClient);
      logger.info("**TIME** H2V" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - h2vStart));
    }
  }
}
