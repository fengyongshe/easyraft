package com.fys.easyraft.client;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.fys.easyraft.core.protobuf.InvokeProto;
import com.fys.easyraft.core.protobuf.RaftProto;
import com.fys.easyraft.core.service.RaftClientService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RaftClient {

  public static void main(String[] args) {
    String bindUrl = "list://127.0.0.1:8088";
    RpcClient rpcClient = new RpcClient(bindUrl);
    RaftClientService clientService =
      BrpcProxy.getProxy(rpcClient, RaftClientService.class);

    RaftProto.GetLeaderRequest.Builder builder = RaftProto.GetLeaderRequest.newBuilder();
    RaftProto.GetLeaderResponse leader = clientService.getLeader(builder.build());

    System.out.println("Leader Info: "+leader.getLeader());

    InvokeProto.SetRequest setRequest = InvokeProto.SetRequest.newBuilder()
      .setKey("hello3")
      .setValue("msg")
      .build();

    InvokeProto.SetResponse setResponse = clientService.set(setRequest);
    System.out.println("Response after set:" + setResponse);

  }

}
