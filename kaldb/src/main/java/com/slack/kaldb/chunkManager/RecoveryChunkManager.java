package com.slack.kaldb.chunkManager;

import static com.slack.kaldb.server.KaldbConfig.CHUNK_DATA_PREFIX;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.*;
import com.slack.kaldb.blobfs.BlobFs;
import com.slack.kaldb.chunk.*;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A recovery chunk manager is the chunk manager for the recovery task. This implementation supports
 * appending messages to open chunks. This also is responsible for cleanly transitioning from a full
 * chunk to a new chunk, and uploading that contents to S3, and notifying ZK of state changes.
 *
 * <p>All chunks except one is considered active. The chunk manager writes the message to the
 * currently active chunk. Once a chunk reaches a roll over point(defined by a roll over strategy),
 * the current chunk is marked as read only. At that point a new chunk is created which becomes the
 * active chunk.
 */
public class RecoveryChunkManager<T> extends ChunkManagerBase<T> {
  private static final Logger LOG = LoggerFactory.getLogger(RecoveryChunkManager.class);

  private final ChunkFactory<T> recoveryChunkFactory;
  private final ChunkRolloverFactory chunkRolloverFactory;
  private boolean readOnly;
  private ReadWriteChunk<T> activeChunk;

  private final AtomicLong liveMessagesIndexedGauge;
  private final AtomicLong liveBytesIndexedGauge;

  public static final String LIVE_MESSAGES_INDEXED = "live_messages_indexed";
  public static final String LIVE_BYTES_INDEXED = "live_bytes_indexed";

  // fields related to roll over
  private final ListeningExecutorService rolloverExecutorService;
  private boolean rollOverFailed;

  @VisibleForTesting
  public List<Chunk<T>> getChunkList() {
    return chunkList;
  }

  /**
   * For capacity planning, we want to control how many roll overs are in progress at the same time.
   * For recovery for now we only expect at most 1-2 chunks per recovery task. So, a single threaded
   * executor will be sufficient to upload the data in sequence. If we have multiple chunks waiting
   * and upload is the bottleneck, consider multiple threads in the thread pool. Unlike the indexer,
   * we expect the recovery task to ingest a lot more data in parallel. So, there may be some chunks
   * waiting.
   */
  public static ListeningExecutorService makeDefaultRecoveryRollOverExecutor() {
    return MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
  }

  public RecoveryChunkManager(
      ChunkFactory<T> recoveryChunkFactory,
      ChunkRolloverFactory chunkRolloverFactory,
      MeterRegistry registry) {
    this(
        recoveryChunkFactory,
        chunkRolloverFactory,
        makeDefaultRecoveryRollOverExecutor(),
        registry);
  }

  public RecoveryChunkManager(
      ChunkFactory<T> recoveryChunkFactory,
      ChunkRolloverFactory chunkRolloverFactory,
      ListeningExecutorService rollOverExecutorService,
      MeterRegistry registry) {

    // TODO: Pass in id of index in LuceneIndexStore to track this info.
    liveMessagesIndexedGauge = registry.gauge(LIVE_MESSAGES_INDEXED, new AtomicLong(0));
    liveBytesIndexedGauge = registry.gauge(LIVE_BYTES_INDEXED, new AtomicLong(0));
    this.recoveryChunkFactory = recoveryChunkFactory;

    this.chunkRolloverFactory = chunkRolloverFactory;
    this.rolloverExecutorService = rollOverExecutorService;
    this.rollOverFailed = false;

    activeChunk = null;

    LOG.info("Created a recovery chunk manager");
  }

  /**
   * TODO: Change description. This function ingests a message into a chunk in the chunk manager. It
   * performs the following steps: 1. Find an active chunk. 2. Ingest the message into the active
   * chunk. 3. Calls the shouldRollOver function to check if the chunk is full. 4. If the chunk is
   * full, queue the active chunk for roll over.
   *
   * <p>Unlike the IndexingChunkManager, in a recovery task we allow multiple chunks to be pending
   * roll over before new messages are ingested. Also, the addMessage call on a recovery node is
   * called from multiple threads for increased throughput.
   */
  public void addMessage(final T message, long msgSize, String kafkaPartitionId, long offset)
      throws IOException {
    if (rollOverFailed || readOnly) {
      // We stop ingestion on chunk roll over failures or if the chunk manager is shutting down.
      LOG.warn("Ingestion is stopped since the chunk is closing or roll over failed");
      throw new IllegalStateException("Ingestion due to shutdown or rollover failures");
    }

    // find the active chunk and add a message to it
    ReadWriteChunk<T> currentChunk = getOrCreateActiveChunk(kafkaPartitionId);
    currentChunk.addMessage(message, kafkaPartitionId, offset);
    long currentIndexedMessages = liveMessagesIndexedGauge.incrementAndGet();
    long currentIndexedBytes = liveBytesIndexedGauge.addAndGet(msgSize);

    // If active chunk is full roll it over.
    if (chunkRolloverFactory
        .getChunkRolloverStrategy()
        .shouldRollOver(currentIndexedBytes, currentIndexedMessages)) {
      LOG.info(
          "After {} messages and {} bytes rolling over chunk {}.",
          currentIndexedMessages,
          currentIndexedBytes,
          currentChunk.id());
      doRollover(currentChunk);
    }
  }

  /**
   * This method initiates a roll over of the active chunk. In future, consider moving the some of
   * the roll over logic into ChunkImpl.
   */
  private void doRollover(ReadWriteChunk<T> currentChunk) {
    // Set activeChunk to null first, so we can initiate the roll over.
    activeChunk = null;
    liveBytesIndexedGauge.set(0);
    liveMessagesIndexedGauge.set(0);
    // Set the end time of the chunk and start the roll over.
    currentChunk.info().setChunkLastUpdatedTimeEpochMs(Instant.now().toEpochMilli());

    RollOverChunkTask<T> rollOverChunkTask =
        chunkRolloverFactory.getRollOverChunkTask(currentChunk, currentChunk.info().chunkId);

    ListenableFuture<Boolean> rolloverFuture = rolloverExecutorService.submit(rollOverChunkTask);
    Futures.addCallback(
        rolloverFuture,
        new FutureCallback<>() {
          @Override
          public void onSuccess(Boolean success) {
            if (success == null || !success) {
              LOG.warn("Roll over failed");
              rollOverFailed = true;
            }
          }

          @Override
          public void onFailure(Throwable t) {
            LOG.warn("Roll over failed with an exception", t);
            rollOverFailed = true;
          }
        },
        MoreExecutors.directExecutor());
  }

  /*
   * When the ChunkManager is being closed, roll over active chunk so we can be sure that it's data is persisted in
   * a remote store.
   */
  public void rollOverActiveChunk() {
    LOG.info("Rolling over active chunk");
    doRollover(getActiveChunk());
  }

  @VisibleForTesting
  public ReadWriteChunk<T> getActiveChunk() {
    return activeChunk;
  }

  /**
   * getChunk returns the active chunk. If no chunk is active because of roll over or this is the
   * first message, create one chunk and set is as active.
   */
  private ReadWriteChunk<T> getOrCreateActiveChunk(String kafkaPartitionId) throws IOException {
    if (activeChunk == null) {
      recoveryChunkFactory.setKafkaPartitionId(kafkaPartitionId);
      ReadWriteChunk<T> newChunk = recoveryChunkFactory.makeChunk();
      chunkList.add(newChunk);
      // Run post create actions on the chunk.
      newChunk.postCreate();
      activeChunk = newChunk;
    }
    return activeChunk;
  }

  // The callers need to wait for rollovers to complete and the status of the roll overs. So, we
  // expose this function to wait for rollovers and report their status.
  // We don't call this function during shutdown, so callers should call this function before close.
  public boolean waitForRollOvers() {
    LOG.info("Waiting for rollovers to complete");
    // Stop accepting new writes to the chunks.
    readOnly = true;

    // Roll over active chunk.
    if (activeChunk != null) {
      rollOverActiveChunk();
    }

    // Stop executor service from taking on new tasks.
    rolloverExecutorService.shutdown();

    // Close roll over executor service.
    try {
      // A short timeout here is fine here since there are no more tasks.
      rolloverExecutorService.awaitTermination(10, TimeUnit.MINUTES);
      rolloverExecutorService.shutdownNow();
    } catch (InterruptedException e) {
      LOG.warn("Encountered error shutting down roll over executor.", e);
      return false;
    }

    if (rollOverFailed) {
      LOG.info("Some roll rollovers failed");
      return false;
    } else {
      LOG.info("All rollovers completed");
      return true;
    }
  }

  @Override
  protected void startUp() throws Exception {
    // No startup actions.
  }

  /**
   * Close the chunk manager safely by finishing all the pending roll overs and closing chunks
   * cleanly. To ensure data integrity don't throw exceptions before chunk close.
   *
   * <p>TODO: When closing a ChunkManager we need to ensure that all the active chunks are closed,
   * and the data is uploaded safely to a remote store. If the active chunks are not closed
   * correctly, we would throw away indexed data and would need to index the same data again.
   *
   * <p>TODO: Consider implementing async close. Also, stop new writes once close is called.
   */
  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closing recovery chunk manager.");

    readOnly = true;

    // Close all chunks.
    for (Chunk<T> chunk : chunkList) {
      try {
        chunk.close();
      } catch (IOException e) {
        LOG.error("Failed to close chunk.", e);
      }
    }

    LOG.info("Closed recovery chunk manager.");
  }

  public static RecoveryChunkManager<LogMessage> fromConfig(
      MeterRegistry meterRegistry,
      MetadataStore metadataStore,
      KaldbConfigs.IndexerConfig indexerConfig,
      BlobFs blobFs,
      KaldbConfigs.S3Config s3Config)
      throws Exception {

    ChunkRollOverStrategy chunkRollOverStrategy =
        ChunkRollOverStrategyImpl.fromConfig(indexerConfig);
    // TODO: Pass these metadata stores in and close them correctly.
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, false);
    SearchMetadataStore searchMetadataStore = new SearchMetadataStore(metadataStore, false);
    SearchContext searchContext = SearchContext.fromConfig(indexerConfig.getServerConfig());

    RecoveryChunkFactoryImpl<LogMessage> recoveryChunkFactory =
        new RecoveryChunkFactoryImpl<>(
            indexerConfig,
            CHUNK_DATA_PREFIX,
            meterRegistry,
            searchMetadataStore,
            snapshotMetadataStore,
            searchContext);

    ChunkRolloverFactory chunkRolloverFactory =
        new ChunkRolloverFactory(
            chunkRollOverStrategy, blobFs, s3Config.getS3Bucket(), meterRegistry);

    return new RecoveryChunkManager<>(recoveryChunkFactory, chunkRolloverFactory, meterRegistry);
  }
}
