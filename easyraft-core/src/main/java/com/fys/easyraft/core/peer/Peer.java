package com.fys.easyraft.core.peer;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.instance.Endpoint;
import com.fys.easyraft.core.protobuf.RaftProto;
import com.fys.easyraft.core.service.RaftConsensusServiceAsync;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Peer {

  private RaftProto.Server server;
  private RpcClient rpcClient;
  private RaftConsensusServiceAsync raftConsensusServiceAsync;

  private long nextIndex;
  private long matchIndex;

  private volatile Boolean voteGranted;
  private volatile boolean isCatchUp;

  public Peer(RaftProto.Server server) {
    this.server = server;
    this.rpcClient = new RpcClient(
      new Endpoint(
        server.getEndpoint().getHost(),
        server.getEndpoint().getPort()
      )
    );

    raftConsensusServiceAsync = BrpcProxy.getProxy(rpcClient, RaftConsensusServiceAsync.class);
    isCatchUp = false;
  }

}
