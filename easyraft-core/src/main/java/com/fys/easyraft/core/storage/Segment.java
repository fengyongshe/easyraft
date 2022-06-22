package com.fys.easyraft.core.storage;

import com.fys.easyraft.core.protobuf.RaftProto;
import lombok.Data;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

@Data
public class Segment {

  private boolean isOpen;
  private long startIndex;
  private long endIndex;
  private long fileSize;
  private String fileName;
  private RandomAccessFile randomAccessFile;
  private List<LogRecord> entries = new ArrayList<>();

  public RaftProto.LogEntry getEntry(long index) {
    if (startIndex == 0 || endIndex == 0) {
      return null;
    }
    if (index < startIndex || index > endIndex) {
      return null;
    }
    int absoluteIndex = (int) (index - startIndex);
    return entries.get(absoluteIndex).getEntry();
  }

}
