package com.fys.easyraft.core.service;

import com.fys.easyraft.core.protobuf.RaftProto;

public interface RaftConsensusService {

  RaftProto.VoteResponse preVote(RaftProto.VoteRequest request);

  RaftProto.VoteResponse requestVote(RaftProto.VoteRequest request);

  RaftProto.AppendEntriesResponse appendEntries(RaftProto.AppendEntriesRequest request);

}
