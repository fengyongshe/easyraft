package com.fys.easyraft.core.util;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;

@Slf4j
public class RaftFileUtils {

  public static List<String> getSortedFilesInDirectory(String dirName, String rootDirName)
      throws IOException  {
    List<String> fileList = new ArrayList<>();
    File rootDir = new File(rootDirName);
    File dir = new File(dirName);
    if (!rootDir.isDirectory() || !dir.isDirectory()) {
      return fileList;
    }
    String rootPath = rootDir.getCanonicalPath();
    if (!rootPath.endsWith("/")) {
      rootPath = rootPath + "/";
    }
    File[] files = dir.listFiles();
    for (File file: files) {
      if (file.isDirectory()) {
        fileList.addAll(getSortedFilesInDirectory(file.getCanonicalPath(), rootPath));
      } else {
        fileList.add(file.getCanonicalPath().substring(rootPath.length()));
      }
    }
    Collections.sort(fileList);
    return fileList;
  }

  public static RandomAccessFile openFile(String dir, String fileName, String mode) {
    try {
      String fullFileName = dir + File.separator + fileName;
      File file = new File(fullFileName);
      return new RandomAccessFile(file, mode);
    } catch (FileNotFoundException ex) {
      log.warn("file not fount, file={}", fileName);
      throw new RuntimeException("file not found, file=" + fileName);
    }
  }

  public static void closeFile(RandomAccessFile randomAccessFile) {
    try {
      if (randomAccessFile != null) {
        randomAccessFile.close();
      }
    } catch (IOException ex) {
      log.warn("close file error, msg={}", ex.getMessage());
    }
  }

  public static long getCRC32(byte[] data) {
    CRC32 crc32 = new CRC32();
    crc32.update(data);
    return crc32.getValue();
  }

  public static <T extends Message> T readProtoFromFile(RandomAccessFile raf, Class<T> clazz) {
    try {
      long crc32FromFile = raf.readLong();
      int dataLen = raf.readInt();
      int hasReadLen = (Long.SIZE + Integer.SIZE) / Byte.SIZE;
      if (raf.length() - hasReadLen < dataLen) {
        log.warn("File RemainLength < dataLen");
        return null;
      }
      byte[] data = new byte[dataLen];
      int readLen = raf.read(data);
      if (readLen != dataLen) {
        log.warn("read length != data length");
        return null;
      }
      long crc32FromData = getCRC32(data);
      if (crc32FromFile != crc32FromData) {
        log.warn("crc32 validate failed");
        return null;
      }
      Method method = clazz.getMethod("parseFrom", byte[].class);
      T message = (T) method.invoke(clazz, data);
      return message;
    } catch (Exception ex) {
      log.warn("Read Proto from file with msg: {}", ex.getMessage());
      return null;
    }
  }

  public static <T extends Message> void writeProtoToFile(RandomAccessFile raf, T message) {
    byte[] messageBytes = message.toByteArray();
    long crc32 = getCRC32(messageBytes);
    try {
      raf.writeLong(crc32);
      raf.writeInt(messageBytes.length);
      raf.write(messageBytes);
    } catch (IOException ex) {
      log.warn("Write proto message to file with error msg: {}", ex.getMessage());
      throw new RuntimeException("write proto to file error");
    }
  }

}
