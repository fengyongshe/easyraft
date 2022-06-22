package com.fys.easyraft.core.stm;

import com.fys.easyraft.core.protobuf.InvokeProto;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;

@Slf4j
public class DefaultStateMachine implements StateMachine {

  static {
    RocksDB.loadLibrary();
  }

  private RocksDB db;
  private String raftDataDir;

  public DefaultStateMachine(String raftDataDir) {
    this.raftDataDir = raftDataDir;
    String dataDir = raftDataDir + File.separator + "stm";
    Options options = new Options();
    options.setCreateIfMissing(true);
    try {
      db = RocksDB.open(options, dataDir);
    } catch (RocksDBException ex) {
      log.error("Faild to open statemachie with errr:{}", ex.getMessage());
    }
  }

  @Override
  public void apply(byte[] dataBytes) {
    try {
      InvokeProto.SetRequest request = InvokeProto.SetRequest.parseFrom(dataBytes);
      db.put(request.getKey().getBytes(), request.getValue().getBytes());
    } catch (Exception ex) {
      log.warn("Put message meet exception with err: {}", ex.getMessage());
    }
  }

  @Override
  public InvokeProto.GetResponse get(InvokeProto.GetRequest request) {
    try {
      InvokeProto.GetResponse.Builder responseBuilder = InvokeProto.GetResponse.newBuilder();
      byte[] keyBytes = request.getKey().getBytes();
      byte[] valueBytes = db.get(keyBytes);
      if (valueBytes != null) {
        String value = new String(valueBytes);
        responseBuilder.setValue(value);
      }
      InvokeProto.GetResponse response = responseBuilder.build();
      return response;
    } catch (RocksDBException ex) {
      log.warn("read rockdb error, msg={}", ex.getMessage());
      return null;
    }
  }

}
