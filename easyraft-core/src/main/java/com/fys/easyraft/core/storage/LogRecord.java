package com.fys.easyraft.core.storage;

import com.fys.easyraft.core.protobuf.RaftProto;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class LogRecord {

  private long offset;
  private RaftProto.LogEntry entry;

}
