package com.fys.easyraft.core.service.impl;

import com.fys.easyraft.core.peer.RaftNode;
import com.fys.easyraft.core.protobuf.RaftProto;
import com.fys.easyraft.core.service.RaftConsensusService;
import com.fys.easyraft.core.util.ConfigurationUtils;
import com.googlecode.protobuf.format.JsonFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class RaftConsensusServiceImpl implements RaftConsensusService  {

  private static final JsonFormat PRINTER = new JsonFormat();
  private RaftNode raftNode;

  public RaftConsensusServiceImpl(RaftNode raftNode) {
    this.raftNode = raftNode;
  }

  @Override
  public RaftProto.VoteResponse preVote(RaftProto.VoteRequest request) {
    raftNode.getLock().lock();
    try {
      RaftProto.VoteResponse.Builder responseBuilder = RaftProto.VoteResponse.newBuilder();
      responseBuilder.setGranted(false);
      responseBuilder.setTerm(raftNode.getCurrentTerm());
      if (!ConfigurationUtils.containsServer(raftNode.getConfiguration(), request.getServerId())) {
        return responseBuilder.build();
      }
      if (request.getTerm() < raftNode.getCurrentTerm()) {
        return responseBuilder.build();
      }
      boolean isLogOk = request.getLastLogTerm() > raftNode.getLastLogTerm()
        || (request.getLastLogTerm() == raftNode.getLastLogTerm()
        && request.getLastLogIndex() >= raftNode.getRaftLog().getLastLogIndex());
      if (!isLogOk) {
        return responseBuilder.build();
      } else {
        responseBuilder.setGranted(true);
        responseBuilder.setTerm(raftNode.getCurrentTerm());
      }
      log.info("preVote request from server {} " +
          "in term {} (my term is {}), granted={}",
        request.getServerId(), request.getTerm(),
        raftNode.getCurrentTerm(), responseBuilder.getGranted());
      return responseBuilder.build();
    } finally {
      raftNode.getLock().unlock();
    }
  }

  @Override
  public RaftProto.VoteResponse requestVote(RaftProto.VoteRequest request) {
    raftNode.getLock().lock();
    try {
      RaftProto.VoteResponse.Builder responseBuilder = RaftProto.VoteResponse.newBuilder();
      responseBuilder.setGranted(false);
      responseBuilder.setTerm(raftNode.getCurrentTerm());
      if (!ConfigurationUtils.containsServer(raftNode.getConfiguration(), request.getServerId())) {
        return responseBuilder.build();
      }
      if (request.getTerm() < raftNode.getCurrentTerm()) {
        return responseBuilder.build();
      }
      if (request.getTerm() > raftNode.getCurrentTerm()) {
        raftNode.stepDown(request.getTerm());
      }
      boolean logIsOk = request.getLastLogTerm() > raftNode.getLastLogTerm()
        || (request.getLastLogTerm() == raftNode.getLastLogTerm()
        && request.getLastLogIndex() >= raftNode.getRaftLog().getLastLogIndex());
      if (raftNode.getVotedFor() == 0 && logIsOk) {
        raftNode.stepDown(request.getTerm());
        raftNode.setVotedFor(request.getServerId());
        raftNode.getRaftLog().updateMetaData(raftNode.getCurrentTerm(), raftNode.getVotedFor(), null, null);
        responseBuilder.setGranted(true);
        responseBuilder.setTerm(raftNode.getCurrentTerm());
      }
      log.info("RequestVote request from server {} " +
          "in term {} (my term is {}), granted={}",
        request.getServerId(), request.getTerm(),
        raftNode.getCurrentTerm(), responseBuilder.getGranted());
      return responseBuilder.build();
    } finally {
      raftNode.getLock().unlock();
    }
  }

  @Override
  public RaftProto.AppendEntriesResponse appendEntries(RaftProto.AppendEntriesRequest request) {
    raftNode.getLock().lock();
    try {
      RaftProto.AppendEntriesResponse.Builder responseBuilder
        = RaftProto.AppendEntriesResponse.newBuilder();
      responseBuilder.setTerm(raftNode.getCurrentTerm());
      responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);
      responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());
      if (request.getTerm() < raftNode.getCurrentTerm()) {
        return responseBuilder.build();
      }
      raftNode.stepDown(request.getTerm());
      if (raftNode.getLeaderId() == 0) {
        raftNode.setLeaderId(request.getServerId());
        log.info("new leaderId={}, conf={}",
          raftNode.getLeaderId(),
          PRINTER.printToString(raftNode.getConfiguration()));
      }
      if (raftNode.getLeaderId() != request.getServerId()) {
        log.warn("Another peer={} declares that it is the leader " +
            "at term={} which was occupied by leader={}",
          request.getServerId(), request.getTerm(), raftNode.getLeaderId());
        raftNode.stepDown(request.getTerm() + 1);
        responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);
        responseBuilder.setTerm(request.getTerm() + 1);
        return responseBuilder.build();
      }

      if (request.getPrevLogIndex() > raftNode.getRaftLog().getLastLogIndex()) {
        log.info("Rejecting AppendEntries RPC would leave gap, " +
            "request prevLogIndex={}, my lastLogIndex={}",
          request.getPrevLogIndex(), raftNode.getRaftLog().getLastLogIndex());
        return responseBuilder.build();
      }
      if (request.getPrevLogIndex() >= raftNode.getRaftLog().getFirstLogIndex()
        && raftNode.getRaftLog().getEntryTerm(request.getPrevLogIndex()) != request.getPrevLogTerm()) {
        log.info("Rejecting AppendEntries RPC: terms don't agree, " +
            "request prevLogTerm={} in prevLogIndex={}, my is {}",
          request.getPrevLogTerm(), request.getPrevLogIndex(),
          raftNode.getRaftLog().getEntryTerm(request.getPrevLogIndex()));
        Validate.isTrue(request.getPrevLogIndex() > 0);
        responseBuilder.setLastLogIndex(request.getPrevLogIndex() - 1);
        return responseBuilder.build();
      }

      if (request.getEntriesCount() == 0) {
        log.debug("heartbeat request from peer={} at term={}, my term={}",
          request.getServerId(), request.getTerm(), raftNode.getCurrentTerm());
        responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS);
        responseBuilder.setTerm(raftNode.getCurrentTerm());
        responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());
        advanceCommitIndex(request);
        return responseBuilder.build();
      }

      responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS);
      List<RaftProto.LogEntry> entries = new ArrayList<>();
      long index = request.getPrevLogIndex();
      for (RaftProto.LogEntry entry : request.getEntriesList()) {
        index++;
        if (index < raftNode.getRaftLog().getFirstLogIndex()) {
          continue;
        }
        if (raftNode.getRaftLog().getLastLogIndex() >= index) {
          if (raftNode.getRaftLog().getEntryTerm(index) == entry.getTerm()) {
            continue;
          }
          // truncate segment log from index
          long lastIndexKept = index - 1;
          raftNode.getRaftLog().truncateSuffix(lastIndexKept);
        }
        entries.add(entry);
      }
      raftNode.getRaftLog().append(entries);
      responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());

      advanceCommitIndex(request);
      log.info("AppendEntries request from server {} " +
          "in term {} (my term is {}), entryCount={} resCode={}",
        request.getServerId(), request.getTerm(), raftNode.getCurrentTerm(),
        request.getEntriesCount(), responseBuilder.getResCode());
      return responseBuilder.build();
    } finally {
      raftNode.getLock().unlock();
    }
  }

  private void advanceCommitIndex(RaftProto.AppendEntriesRequest request) {
    long newCommitIndex = Math.min(request.getCommitIndex(),
      request.getPrevLogIndex() + request.getEntriesCount());
    raftNode.setCommitIndex(newCommitIndex);
    raftNode.getRaftLog().updateMetaData(null,null, null, newCommitIndex);
    if (raftNode.getLastAppliedIndex() < raftNode.getCommitIndex()) {
      // apply state machine
      for (long index = raftNode.getLastAppliedIndex() + 1;
           index <= raftNode.getCommitIndex(); index++) {
        RaftProto.LogEntry entry = raftNode.getRaftLog().getEntry(index);
        raftNode.setLastAppliedIndex(index);
      }
    }
  }

}
