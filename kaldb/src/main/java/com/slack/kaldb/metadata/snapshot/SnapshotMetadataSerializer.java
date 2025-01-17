package com.slack.kaldb.metadata.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.kaldb.metadata.core.MetadataSerializer;
import com.slack.kaldb.proto.metadata.Metadata;

public class SnapshotMetadataSerializer implements MetadataSerializer<SnapshotMetadata> {
  private static Metadata.SnapshotMetadata toSnapshotMetadataProto(
      SnapshotMetadata snapshotMetadata) {
    return Metadata.SnapshotMetadata.newBuilder()
        .setName(snapshotMetadata.name)
        .setSnapshotId(snapshotMetadata.snapshotId)
        .setSnapshotPath(snapshotMetadata.snapshotPath)
        .setStartTimeEpochMs(snapshotMetadata.startTimeEpochMs)
        .setEndTimeEpochMs(snapshotMetadata.endTimeEpochMs)
        .setPartitionId(snapshotMetadata.partitionId)
        .setMaxOffset(snapshotMetadata.maxOffset)
        .build();
  }

  private static SnapshotMetadata fromSnapshotMetadataProto(
      Metadata.SnapshotMetadata protoSnapshotMetadata) {
    return new SnapshotMetadata(
        protoSnapshotMetadata.getSnapshotId(),
        protoSnapshotMetadata.getSnapshotPath(),
        protoSnapshotMetadata.getStartTimeEpochMs(),
        protoSnapshotMetadata.getEndTimeEpochMs(),
        protoSnapshotMetadata.getMaxOffset(),
        protoSnapshotMetadata.getPartitionId());
  }

  @Override
  public String toJsonStr(SnapshotMetadata metadata) throws InvalidProtocolBufferException {
    if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");

    return printer.print(toSnapshotMetadataProto(metadata));
  }

  @Override
  public SnapshotMetadata fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.SnapshotMetadata.Builder snapshotMetadataBuiler =
        Metadata.SnapshotMetadata.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(data, snapshotMetadataBuiler);
    return fromSnapshotMetadataProto(snapshotMetadataBuiler.build());
  }
}
