package com.fys.easyraft.core.storage;

import com.fys.easyraft.core.protobuf.RaftProto;
import com.fys.easyraft.core.util.RaftFileUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.TreeMap;

@Slf4j
public class Segments {

  private String logDir;
  private String logDataDir;
  private int maxSegmentFileSize;
  private RaftProto.LogMetaData metaData;
  private TreeMap<Long, Segment> startLogIndexSegmentMap = new TreeMap<>();
  private volatile long totalSize;

  public Segments(String raftDataDir, int maxSegmentFileSize) {
    this.logDir = raftDataDir + File.separator + "log";
    this.logDataDir = logDir + File.separator + "data";
    this.maxSegmentFileSize = maxSegmentFileSize;
    File file = new File(logDataDir);
    if (!file.exists()) {
      file.mkdir();
    }
    loadSegments();
    for (Segment segment: startLogIndexSegmentMap.values()) {
      this.loadSegmentData(segment);
    }
    metaData = this.readMetaData();
    if (metaData == null) {
      if(startLogIndexSegmentMap.size() > 0) {
        log.error("No readable metadata file but found segments in {}", logDir);
        throw new RuntimeException("No readable metadata file but found segments");
      }
      metaData = RaftProto.LogMetaData.newBuilder().setFirstLogIndex(1).build();
    }

  }

  public void loadSegments() {
    try {
      List<String> fileNames = RaftFileUtils.getSortedFilesInDirectory(logDataDir, logDataDir);
      for(String fileName: fileNames) {
        String[] splitArray = fileName.split("-");
        if (splitArray.length != 2) {
          log.warn("Segment FileName:{} is not valid", fileName);
          continue;
        }
        Segment segment = new Segment();
        segment.setFileName(fileName);
        if (splitArray[0].endsWith("open")) {
          segment.setOpen(true);
          segment.setStartIndex(Long.valueOf(splitArray[1]));
          segment.setEndIndex(0);
        } else {
          try {
            segment.setOpen(false);
            segment.setStartIndex(Long.parseLong(splitArray[0]));
            segment.setEndIndex(Long.parseLong(splitArray[1]));
          } catch (NumberFormatException ex) {
            log.warn("Segment FileNmae:{} is not valid", fileName);
            continue;
          }
        }
        segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, fileName, "rw"));
        segment.setFileSize(segment.getRandomAccessFile().length());
        startLogIndexSegmentMap.put(segment.getStartIndex(), segment);
      }
    } catch (IOException ex) {
      log.warn("segments loaded with exception: {} ", ex);
      throw new RuntimeException("open segment file error");
    }
  }

  public void loadSegmentData(Segment segment) {
    try {
      RandomAccessFile randomAccessFile = segment.getRandomAccessFile();
      long totalLength = segment.getFileSize();
      long offset = 0;
      while(offset < totalLength) {
        RaftProto.LogEntry entry =
          RaftFileUtils.readProtoFromFile(randomAccessFile, RaftProto.LogEntry.class);
        if(entry == null) {
          throw new RuntimeException("Read Segment Log Failed");
        }
        LogRecord record = new LogRecord(offset, entry);
        segment.getEntries().add(record);
        offset = randomAccessFile.getFilePointer();
      }
      totalSize += totalLength;
    } catch (Exception ex) {
      log.error("Read Segment meet exception with msg:{}", ex.getMessage());
      throw new RuntimeException("File Not Found");
    }
    int entrySize = segment.getEntries().size();
    if (entrySize > 0) {
      segment.setStartIndex(segment.getEntries().get(0).getEntry().getIndex());
      segment.setEndIndex(segment.getEntries().get(entrySize -1).getEntry().getIndex());
    }
  }

  public RaftProto.LogMetaData readMetaData() {
    String fileName = logDir + File.separator + "metadata";
    File file = new File(fileName);
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
      RaftProto.LogMetaData metadata =
        RaftFileUtils.readProtoFromFile(randomAccessFile, RaftProto.LogMetaData.class);
      return metadata;
    } catch (IOException ex) {
      log.warn("meta file:{} is not exist", fileName);
      return null;
    }
  }

  public void updateMetaData(Long currentTerm, Integer votedFor, Long firstLogIndex, Long commitIndex) {
    RaftProto.LogMetaData.Builder builder = RaftProto.LogMetaData.newBuilder(this.metaData);
    if (currentTerm != null) {
      builder.setCurrentTerm(currentTerm);
    }
    if (votedFor != null) {
      builder.setVotedFor(votedFor);
    }
    if (firstLogIndex != null) {
      builder.setFirstLogIndex(firstLogIndex);
    }
    if (commitIndex != null) {
      builder.setCommitIndex(commitIndex);
    }
    this.metaData = builder.build();

    String fileName = logDir + File.separator + "metadata";
    File file = new File(fileName);
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
      RaftFileUtils.writeProtoToFile(randomAccessFile, metaData);
      log.info("new segment meta info, currentTerm={}, votedFor={}, firstLogIndex={}",
        metaData.getCurrentTerm(), metaData.getVotedFor(), metaData.getFirstLogIndex());
    } catch (IOException ex) {
      log.warn("Meta file:{} is not exist", fileName);
    }
  }

  public long append(List<RaftProto.LogEntry> entries) {
    long newLastLogIndex = this.getLastLogIndex();
    for (RaftProto.LogEntry entry : entries) {
      newLastLogIndex++;
      int entrySize = entry.getSerializedSize();
      int segmentSize = startLogIndexSegmentMap.size();
      boolean isNeedNewSegmentFile = false;
      try {
        if (segmentSize == 0) {
          isNeedNewSegmentFile = true;
        } else {
          Segment segment = startLogIndexSegmentMap.lastEntry().getValue();
          if (!segment.isOpen()) {
            isNeedNewSegmentFile = true;
          } else if (segment.getFileSize() + entrySize >= maxSegmentFileSize) {
            isNeedNewSegmentFile = true;
            // 最后一个segment的文件close并改名
            segment.getRandomAccessFile().close();
            segment.setOpen(false);
            String newFileName = String.format("%020d-%020d",
              segment.getStartIndex(), segment.getEndIndex());
            String newFullFileName = logDataDir + File.separator + newFileName;
            File newFile = new File(newFullFileName);
            String oldFullFileName = logDataDir + File.separator + segment.getFileName();
            File oldFile = new File(oldFullFileName);
            FileUtils.moveFile(oldFile, newFile);
            segment.setFileName(newFileName);
            segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, newFileName, "r"));
          }
        }
        Segment newSegment;
        // 新建segment文件
        if (isNeedNewSegmentFile) {
          // open new segment file
          String newSegmentFileName = String.format("open-%d", newLastLogIndex);
          String newFullFileName = logDataDir + File.separator + newSegmentFileName;
          File newSegmentFile = new File(newFullFileName);
          if (!newSegmentFile.exists()) {
            newSegmentFile.createNewFile();
          }
          Segment segment = new Segment();
          segment.setOpen(true);
          segment.setStartIndex(newLastLogIndex);
          segment.setEndIndex(0);
          segment.setFileName(newSegmentFileName);
          segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, newSegmentFileName, "rw"));
          newSegment = segment;
        } else {
          newSegment = startLogIndexSegmentMap.lastEntry().getValue();
        }
        // 写proto到segment中
        if (entry.getIndex() == 0) {
          entry = RaftProto.LogEntry.newBuilder(entry)
            .setIndex(newLastLogIndex).build();
        }
        newSegment.setEndIndex(entry.getIndex());
        newSegment.getEntries().add(new LogRecord(newSegment.getRandomAccessFile().getFilePointer(), entry));
        RaftFileUtils.writeProtoToFile(newSegment.getRandomAccessFile(), entry);
        newSegment.setFileSize(newSegment.getRandomAccessFile().length());
        if (!startLogIndexSegmentMap.containsKey(newSegment.getStartIndex())) {
          startLogIndexSegmentMap.put(newSegment.getStartIndex(), newSegment);
        }
        totalSize += entrySize;
      }  catch (IOException ex) {
        throw new RuntimeException("append raft log exception, msg=" + ex.getMessage());
      }
    }
    return newLastLogIndex;
  }

  public void truncateSuffix(long newEndIndex) {
    if (newEndIndex >= getLastLogIndex()) {
      return;
    }
    log.info("Truncating log from old end index {} to new end index {}",
      getLastLogIndex(), newEndIndex);
    while (!startLogIndexSegmentMap.isEmpty()) {
      Segment segment = startLogIndexSegmentMap.lastEntry().getValue();
      try {
        if (newEndIndex == segment.getEndIndex()) {
          break;
        } else if (newEndIndex < segment.getStartIndex()) {
          totalSize -= segment.getFileSize();
          // delete file
          segment.getRandomAccessFile().close();
          String fullFileName = logDataDir + File.separator + segment.getFileName();
          FileUtils.forceDelete(new File(fullFileName));
          startLogIndexSegmentMap.remove(segment.getStartIndex());
        } else if (newEndIndex < segment.getEndIndex()) {
          int i = (int) (newEndIndex + 1 - segment.getStartIndex());
          segment.setEndIndex(newEndIndex);
          long newFileSize = segment.getEntries().get(i).getOffset();
          totalSize -= (segment.getFileSize() - newFileSize);
          segment.setFileSize(newFileSize);
          segment.getEntries().removeAll(
            segment.getEntries().subList(i, segment.getEntries().size()));
          FileChannel fileChannel = segment.getRandomAccessFile().getChannel();
          fileChannel.truncate(segment.getFileSize());
          fileChannel.close();
          segment.getRandomAccessFile().close();
          String oldFullFileName = logDataDir + File.separator + segment.getFileName();
          String newFileName = String.format("%020d-%020d",
            segment.getStartIndex(), segment.getEndIndex());
          segment.setFileName(newFileName);
          String newFullFileName = logDataDir + File.separator + segment.getFileName();
          new File(oldFullFileName).renameTo(new File(newFullFileName));
          segment.setRandomAccessFile(RaftFileUtils.openFile(logDataDir, segment.getFileName(), "rw"));
        }
      } catch (IOException ex) {
        log.warn("io exception, msg={}", ex.getMessage());
      }
    }
  }

  public long getFirstLogIndex() {
    return metaData.getFirstLogIndex();
  }

  public long getLastLogIndex() {
    // 有两种情况segment为空
    // 1、第一次初始化，firstLogIndex = 1，lastLogIndex = 0
    // 2、snapshot刚完成，日志正好被清理掉，firstLogIndex = snapshotIndex + 1， lastLogIndex = snapshotIndex
    if (startLogIndexSegmentMap.size() == 0) {
      return getFirstLogIndex() - 1;
    }
    Segment lastSegment = startLogIndexSegmentMap.lastEntry().getValue();
    return lastSegment.getEndIndex();
  }

  public long getEntryTerm(long index) {
    RaftProto.LogEntry entry = getEntry(index);
    if (entry == null) {
      return 0;
    }
    return entry.getTerm();
  }

  public RaftProto.LogEntry getEntry(long index) {
    long firstLogIndex = getFirstLogIndex();
    long lastLogIndex = getLastLogIndex();
    if (index == 0 || index < firstLogIndex || index > lastLogIndex) {
      log.debug("index out of range, index={}, firstLogIndex={}, lastLogIndex={}",
        index, firstLogIndex, lastLogIndex);
      return null;
    }
    if (startLogIndexSegmentMap.size() == 0) {
      return null;
    }
    Segment segment = startLogIndexSegmentMap.floorEntry(index).getValue();
    return segment.getEntry(index);
  }

  public RaftProto.LogMetaData getMetaData() {
    return metaData;
  }

  public long getTotalSize() {
    return totalSize;
  }

}
