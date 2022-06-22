package com.fys.easyraft.core.storage;

import com.fys.easyraft.core.protobuf.RaftProto;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

public class LogSegmentsTest {

  public static void main(String[] args) {
    String raftDataDir = "/tmp/easyraft";
    Segments segments = new Segments(raftDataDir, 1024000);

    List<RaftProto.LogEntry> entries = new ArrayList<>();
    for (int i = 1; i < 10; i++) {
      RaftProto.LogEntry entry = RaftProto.LogEntry.newBuilder()
        .setData(ByteString.copyFrom(("testEntryData" + i).getBytes()))
        .setType(RaftProto.EntryType.ENTRY_TYPE_DATA)
        .setIndex(i)
        .setTerm(i)
        .build();
      entries.add(entry);
    }
    long lastLogIndex = segments.append(entries);
  }
}
