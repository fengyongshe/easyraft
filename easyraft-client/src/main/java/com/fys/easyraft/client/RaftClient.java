package com.fys.easyraft.client;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.fys.easyraft.core.protobuf.RaftProto;
import com.fys.easyraft.core.service.RaftConsensusService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RaftClient {

  public static void main(String[] args) {
    //list://127.0.0.1:8088
    String bindUrl = args[0];
    RpcClient rpcClient = new RpcClient(bindUrl);
    RaftConsensusService consensusService =
      BrpcProxy.getProxy(rpcClient, RaftConsensusService.class);

    RaftProto.VoteRequest voteRequest = RaftProto.VoteRequest.newBuilder()
      .setServerId(1)
      .setLastLogIndex(100)
      .setTerm(3)
      .setLastLogTerm(2)
      .build();

    RaftProto.VoteResponse reponse = consensusService.preVote(voteRequest);
    log.info("Response from preVote: {}", reponse);
  }

}
