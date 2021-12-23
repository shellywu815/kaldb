package com.slack.kaldb.clusterManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.google.common.base.Stopwatch;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class ObjectStorageReconciliationServiceTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(ObjectStorageReconciliationServiceTest.class);

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;

  private String bucket;
  private S3Client s3Client;
  private S3BlobFs s3BlobFs;

  private MetadataStore metadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;

  @Before
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("ReplicaCreatorServiceTest")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    metadataStore = ZookeeperMetadataStoreImpl.fromConfig(meterRegistry, zkConfig);
    snapshotMetadataStore = spy(new SnapshotMetadataStore(metadataStore, true));

    bucket = "object-storage-" + UUID.randomUUID();
    s3Client = S3_MOCK_RULE.createS3ClientV2();
    s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());

    s3BlobFs = spy(new S3BlobFs());
    s3BlobFs.init(s3Client);
  }

  @After
  public void shutdown() throws IOException {
    snapshotMetadataStore.close();
    metadataStore.close();

    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldReturnFilesWithoutSnapshots() {
    Set<String> filePaths =
        Set.of(
            "s3://bucket/foo/bar/baz.txt",
            "s3://bucket/foo/bar/qux.txt",
            "s3://bucket/foo/garply.txt",
            "s3://bucket/bar/baz/qux.txt",
            "s3://bucket/bar/baz/quux.txt",
            "s3://bucket/bar/quuz.txt",
            "s3://bucket/corge.txt");

    List<SnapshotMetadata> snapshotMetadata =
        List.of(
            new SnapshotMetadata(
                UUID.randomUUID().toString(),
                "s3://bucket/foo/",
                Instant.now().minusSeconds(90).toEpochMilli(),
                Instant.now().toEpochMilli(),
                0,
                "0"),
            new SnapshotMetadata(
                UUID.randomUUID().toString(),
                "s3://bucket/grault/",
                Instant.now().minusSeconds(90).toEpochMilli(),
                Instant.now().toEpochMilli(),
                0,
                "0"),
            new SnapshotMetadata(
                UUID.randomUUID().toString(),
                "s3://bucket/bar/baz/",
                Instant.now().minusSeconds(90).toEpochMilli(),
                Instant.now().toEpochMilli(),
                0,
                "0"));

    KaldbConfigs.ManagerConfig managerConfig = KaldbConfigs.ManagerConfig.newBuilder().build();

    ObjectStorageReconciliationService objectStorageReconciliationService =
        new ObjectStorageReconciliationService(
            managerConfig, snapshotMetadataStore, meterRegistry, s3BlobFs);

    Set<String> files =
        objectStorageReconciliationService.filesWithoutSnapshots(filePaths, snapshotMetadata);
    assertThat(files)
        .containsExactlyInAnyOrder("s3://bucket/bar/quuz.txt", "s3://bucket/corge.txt");
  }

  @Test
  public void shouldReturnSnapshotsWithoutFiles() {
    Set<String> filePaths =
        Set.of(
            "s3://bucket/foo/bar/baz.txt",
            "s3://bucket/foo/bar/qux.txt",
            "s3://bucket/bar/baz/qux.txt",
            "s3://bucket/bar/baz/quux.txt",
            "s3://bucket/bar/quuz.txt",
            "s3://bucket/corge.txt");

    List<SnapshotMetadata> snapshotMetadata =
        List.of(
            new SnapshotMetadata(
                UUID.randomUUID().toString(),
                "s3://bucket/foo/",
                Instant.now().minusSeconds(90).toEpochMilli(),
                Instant.now().toEpochMilli(),
                0,
                "0"),
            new SnapshotMetadata(
                UUID.randomUUID().toString(),
                "s3://bucket/grault/",
                Instant.now().minusSeconds(90).toEpochMilli(),
                Instant.now().toEpochMilli(),
                0,
                "0"),
            new SnapshotMetadata(
                UUID.randomUUID().toString(),
                "s3://bucket/bar/baz/",
                Instant.now().minusSeconds(90).toEpochMilli(),
                Instant.now().toEpochMilli(),
                0,
                "0"));

    KaldbConfigs.ManagerConfig managerConfig = KaldbConfigs.ManagerConfig.newBuilder().build();

    ObjectStorageReconciliationService objectStorageReconciliationService =
        new ObjectStorageReconciliationService(
            managerConfig, snapshotMetadataStore, meterRegistry, s3BlobFs);

    Set<SnapshotMetadata> snapshots =
        objectStorageReconciliationService.snapshotsWithoutFiles(snapshotMetadata, filePaths);
    assertThat(snapshots.size()).isEqualTo(1);
    assertThat(
            snapshots
                .stream()
                .allMatch(snapshot -> snapshot.snapshotPath.equals("s3://bucket/grault/")))
        .isTrue();
  }

  @Test
  public void veryLargeNumbers() throws Exception {
    int matchingSnapshotsToCreate = 100_000;
    int nonMatchingSnapshotsToCreate = 150;
    int nonMatchingFilesToCreate = 150;
    int filesPerSnapshot = 10;

    Set<String> filePaths = new HashSet<>();
    List<String> usedUuids = new ArrayList<>();

    for (int i = 0; i < matchingSnapshotsToCreate; i++) {
      String uuid = UUID.randomUUID().toString();
      usedUuids.add(uuid);
      for (int j = 0; j < filesPerSnapshot; j++) {
        filePaths.add("s3://bucket/" + uuid + "/" + UUID.randomUUID() + ".txt");
      }
    }

    Set<String> nonMatchingFilePaths = new HashSet<>();
    for (int i = 0; i < nonMatchingFilesToCreate; i++) {
      String uuid = UUID.randomUUID().toString();
      for (int j = 0; j < filesPerSnapshot; j++) {
        String filePath = "s3://bucket/" + uuid + "/" + UUID.randomUUID() + ".txt";
        filePaths.add(filePath);
        nonMatchingFilePaths.add(filePath);
      }
    }

    List<SnapshotMetadata> snapshotMetadataList = new ArrayList<>();
    for (int i = 0; i < matchingSnapshotsToCreate; i++) {
      SnapshotMetadata matchingSnapshotMetadata =
          new SnapshotMetadata(
              UUID.randomUUID().toString(),
              "s3://bucket/" + usedUuids.get(i) + "/",
              Instant.now().minusSeconds(90).toEpochMilli(),
              Instant.now().toEpochMilli(),
              0,
              "0");
      snapshotMetadataList.add(matchingSnapshotMetadata);
    }

    List<SnapshotMetadata> nonMatchingSnapshotMetadataList = new ArrayList<>();
    for (int i = 0; i < nonMatchingSnapshotsToCreate; i++) {
      SnapshotMetadata nonMatchingSnapshotMetadata =
          new SnapshotMetadata(
              UUID.randomUUID().toString(),
              "s3://bucket/" + UUID.randomUUID() + "/",
              Instant.now().minusSeconds(90).toEpochMilli(),
              Instant.now().toEpochMilli(),
              0,
              "0");
      nonMatchingSnapshotMetadataList.add(nonMatchingSnapshotMetadata);
      snapshotMetadataList.add(nonMatchingSnapshotMetadata);
    }

    Collections.shuffle(snapshotMetadataList);
    // the filePaths is a set, which by definition is already unordered so it cannot be shuffled

    KaldbConfigs.ManagerConfig managerConfig = KaldbConfigs.ManagerConfig.newBuilder().build();

    ObjectStorageReconciliationService objectStorageReconciliationService =
        new ObjectStorageReconciliationService(
            managerConfig, snapshotMetadataStore, meterRegistry, s3BlobFs);

    Stopwatch snapshotStopwatch = Stopwatch.createStarted();
    Set<SnapshotMetadata> snapshots =
        objectStorageReconciliationService.snapshotsWithoutFiles(snapshotMetadataList, filePaths);
    LOG.info(
        "snapshotsWithoutFiles took {}ms with {} matching snapshots, {} files per snapshot, {} non-matching snapshots, {} non-matching files",
        snapshotStopwatch.elapsed(TimeUnit.MILLISECONDS),
        matchingSnapshotsToCreate,
        filesPerSnapshot,
        nonMatchingSnapshotsToCreate,
        nonMatchingFilesToCreate);
    assertThat(snapshots).containsExactlyInAnyOrderElementsOf(nonMatchingSnapshotMetadataList);

    Stopwatch filesStopwatch = Stopwatch.createStarted();
    Set<String> files =
        objectStorageReconciliationService.filesWithoutSnapshots(filePaths, snapshotMetadataList);
    LOG.info(
        "filesWithoutSnapshots took {}ms with {} matching snapshots, {} files per snapshot, {} non-matching snapshots, {} non-matching files",
        filesStopwatch.elapsed(TimeUnit.MILLISECONDS),
        matchingSnapshotsToCreate,
        filesPerSnapshot,
        nonMatchingSnapshotsToCreate,
        nonMatchingFilesToCreate);
    assertThat(files).containsExactlyInAnyOrderElementsOf(nonMatchingFilePaths);

    objectStorageReconciliationService.shutDown();
  }
}
