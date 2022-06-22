package com.fys.easyraft.core.stm;

import com.fys.easyraft.core.protobuf.InvokeProto;

public interface StateMachine {

  void apply(byte[] dataBytes);

  InvokeProto.GetResponse get(InvokeProto.GetRequest request);

}
