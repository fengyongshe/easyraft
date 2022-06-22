package com.fys.easyraft.core.service;

import com.fys.easyraft.core.protobuf.InvokeProto;
import com.fys.easyraft.core.protobuf.RaftProto;

public interface RaftClientService {

  InvokeProto.SetResponse set(InvokeProto.SetRequest request);

  InvokeProto.GetResponse get(InvokeProto.GetRequest request);

  RaftProto.GetLeaderResponse getLeader(RaftProto.GetLeaderRequest request);

}
