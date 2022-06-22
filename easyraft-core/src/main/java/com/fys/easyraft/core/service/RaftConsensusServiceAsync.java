package com.fys.easyraft.core.service;

import com.baidu.brpc.client.RpcCallback;
import com.fys.easyraft.core.protobuf.RaftProto;

import java.util.concurrent.Future;

public interface RaftConsensusServiceAsync extends RaftConsensusService {

  Future<RaftProto.VoteResponse> preVote(RaftProto.VoteRequest request,
                                         RpcCallback<RaftProto.VoteResponse> callback);

  Future<RaftProto.VoteResponse> requestVote(RaftProto.VoteRequest request,
                                             RpcCallback<RaftProto.VoteResponse> callback);

}
