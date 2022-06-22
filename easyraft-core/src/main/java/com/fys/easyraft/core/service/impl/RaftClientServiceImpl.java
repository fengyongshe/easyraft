package com.fys.easyraft.core.service.impl;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.instance.Endpoint;
import com.fys.easyraft.core.peer.Peer;
import com.fys.easyraft.core.peer.RaftNode;
import com.fys.easyraft.core.protobuf.InvokeProto;
import com.fys.easyraft.core.protobuf.RaftProto;
import com.fys.easyraft.core.service.RaftClientService;
import com.googlecode.protobuf.format.JsonFormat;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class RaftClientServiceImpl implements RaftClientService  {

  private static final JsonFormat jsonFormat = new JsonFormat();
  private RaftNode raftNode;
  private int leaderId = -1;
  private RpcClient leaderRpcClient = null;
  private Lock leaderLock = new ReentrantLock();

  public RaftClientServiceImpl(RaftNode raftNode) {
    this.raftNode = raftNode;
  }

  @Override
  public InvokeProto.SetResponse set(InvokeProto.SetRequest request) {
    InvokeProto.SetResponse.Builder responseBuilder = InvokeProto.SetResponse.newBuilder();
    if(raftNode.getLeaderId() <= 0) {
      responseBuilder.setSuccess(false);
    } else if (raftNode.getLeaderId() != raftNode.getLocalServer().getServerId()) {
      onLeaderChangeEvent();
      RaftClientService raftClientService = BrpcProxy.getProxy(leaderRpcClient, RaftClientService.class);
      InvokeProto.SetResponse responseFromLeader = raftClientService.set(request);
      responseBuilder.mergeFrom(responseFromLeader);
    } else {
      byte[] data = request.toByteArray();
      boolean success = raftNode.replicate(data, RaftProto.EntryType.ENTRY_TYPE_DATA);
      responseBuilder.setSuccess(true);
    }
    InvokeProto.SetResponse response= responseBuilder.build();
    log.info("set request, request={}, response={}", jsonFormat.printToString(request),
      jsonFormat.printToString(response));
    return response;
  }

  @Override
  public InvokeProto.GetResponse get(InvokeProto.GetRequest request) {
    return null;
  }

  @Override
  public RaftProto.GetLeaderResponse getLeader(RaftProto.GetLeaderRequest request) {
    RaftProto.GetLeaderResponse.Builder responseBuilder = RaftProto.GetLeaderResponse.newBuilder();
    responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS);
    RaftProto.Endpoint.Builder endpointBuilder = RaftProto.Endpoint.newBuilder();

    raftNode.getLock().lock();
    try {
      int leaderId = raftNode.getLeaderId();
      if (leaderId == 0 ) {
        responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);
      } else if(leaderId == raftNode.getLocalServer().getServerId()) {
        endpointBuilder.setHost(raftNode.getLocalServer().getEndpoint().getHost());
        endpointBuilder.setPort(raftNode.getLocalServer().getEndpoint().getPort());
      } else {
        RaftProto.Configuration configuration = raftNode.getConfiguration();
        for (RaftProto.Server server: configuration.getServersList()) {
          if(server.getServerId() == leaderId) {
            endpointBuilder.setHost(server.getEndpoint().getHost());
            endpointBuilder.setPort(server.getEndpoint().getPort());
          }
        }
      }
    } finally {
      raftNode.getLock().unlock();
    }
    responseBuilder.setLeader(endpointBuilder.build());
    RaftProto.GetLeaderResponse response = responseBuilder.build();
    log.info("getLeader response={}", jsonFormat.printToString(response));
    return responseBuilder.build();
  }

  private void onLeaderChangeEvent() {
    if (raftNode.getLeaderId() != -1
      && raftNode.getLeaderId() != raftNode.getLocalServer().getServerId()
      && leaderId != raftNode.getLeaderId()) {
      leaderLock.lock();
      try {
        if (leaderId != -1 && leaderRpcClient != null) {
          leaderRpcClient.stop();
          leaderRpcClient = null;
          leaderId = -1;
        }
        leaderId = raftNode.getLeaderId();
        Peer peer = raftNode.getPeerMap().get(leaderId);
        Endpoint endpoint = new Endpoint(peer.getServer().getEndpoint().getHost(),
          peer.getServer().getEndpoint().getPort());
        RpcClientOptions rpcClientOptions = new RpcClientOptions();
        rpcClientOptions.setGlobalThreadPoolSharing(true);
        leaderRpcClient = new RpcClient(endpoint, rpcClientOptions);
        } finally {
          leaderLock.unlock();
        }
    }
  }

}
