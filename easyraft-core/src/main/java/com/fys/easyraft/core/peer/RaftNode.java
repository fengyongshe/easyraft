package com.fys.easyraft.core.peer;

import com.baidu.brpc.client.RpcCallback;
import com.fys.easyraft.core.conf.RaftOptions;
import com.fys.easyraft.core.storage.Segments;
import com.fys.easyraft.core.protobuf.RaftProto;
import com.fys.easyraft.core.util.ConfigurationUtils;
import com.googlecode.protobuf.format.JsonFormat;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Getter
@Setter
public class RaftNode {

  private static final JsonFormat jsonFormat = new JsonFormat();

  private RaftOptions raftOptions;
  private RaftProto.Configuration configuration;
  private ConcurrentMap<Integer, Peer> peerMap = new ConcurrentHashMap<>();
  private RaftProto.Server localServer;
  private NodeState state = NodeState.STATE_FOLLOWER;
  private long currentTerm;
  private int votedFor;
  private int leaderId;
  private long commitIndex;
  private volatile long lastAppliedIndex;

  private Segments raftLog;

  private Lock lock = new ReentrantLock();
  private Condition commitIndexCondition = lock.newCondition();
  private Condition catchUpCondition = lock.newCondition();

  private ExecutorService executorService;
  private ScheduledExecutorService scheduledExecutorService;
  private ScheduledFuture electionScheduledFuture;
  private ScheduledFuture heartbeatScheduledFuture;

  public RaftNode(RaftOptions raftOptions,
                  List<RaftProto.Server> servers,
                  RaftProto.Server localServer) {
    this.raftOptions = raftOptions;
    RaftProto.Configuration.Builder confBuilder = RaftProto.Configuration.newBuilder();
    for (RaftProto.Server server: servers) {
      confBuilder.addServers(server);
    }
    configuration = confBuilder.build();

    this.localServer = localServer;

    raftLog = new Segments(raftOptions.getDataDir(), raftOptions.getMaxSegmentFileSize());
    currentTerm = raftLog.getMetaData().getCurrentTerm();
    votedFor = raftLog.getMetaData().getVotedFor();
    commitIndex = raftLog.getMetaData().getCommitIndex();
    lastAppliedIndex = commitIndex;

  }

  public void init() {
     for(RaftProto.Server server: configuration.getServersList()) {
       if( !peerMap.containsKey(server.getServerId())
            && server.getServerId() != localServer.getServerId()) {
         Peer peer = new Peer(server);
         peer.setNextIndex(raftLog.getLastLogIndex()  + 1);
         peerMap.put(server.getServerId(), peer);
       }

       executorService = new ThreadPoolExecutor(raftOptions.getRaftConsensusThreadNum(),
              raftOptions.getRaftConsensusThreadNum(),
              60,
              TimeUnit.SECONDS,
              new LinkedBlockingDeque<Runnable>()
         );
       scheduledExecutorService = Executors.newScheduledThreadPool(2);
       scheduledExecutorService.scheduleWithFixedDelay(
         () -> log.info("The Server is running"),
         raftOptions.getSnapshotPeriodSeconds(),
         raftOptions.getSnapshotPeriodSeconds(),
         TimeUnit.SECONDS
       );
     }
     resetElectionTimer();
  }

  private void resetElectionTimer() {
    if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
      electionScheduledFuture.cancel(true);
    }
    electionScheduledFuture = scheduledExecutorService.schedule(new Runnable() {
      @Override
      public void run() {
        log.info("Election Scheduled Future at server: {} ", localServer);
        startPreVote();
      }
    }, getElectionTimeoutMs(), TimeUnit.MILLISECONDS);
  }

  public void startPreVote() {
      lock.lock();
      try {
        if(!ConfigurationUtils.containsServer(configuration, localServer.getServerId())) {
          resetElectionTimer();
          return;
        }
        log.info("Running pre-vote in term: {}", currentTerm);
        state = NodeState.STATE_PRE_CANDIDATE;
      } finally {
        lock.unlock();
      }

      for(RaftProto.Server server: configuration.getServersList()) {
        if(server.getServerId() == localServer.getServerId()) {
          continue;
        }
        final Peer peer = peerMap.get(server.getServerId());
        executorService.submit(() -> preVote(peer));
      }
      resetElectionTimer();
  }

  public void startVote() {
    lock.lock();
    try {
      if (!ConfigurationUtils.containsServer(configuration, localServer.getServerId())) {
        resetElectionTimer();
        return;
      }
      currentTerm++;
      log.info("Running for election in term {}", currentTerm);
      state = NodeState.STATE_CANDIDATE;
      leaderId = 0;
      votedFor = localServer.getServerId();
    } finally {
      lock.unlock();
    }
    for (RaftProto.Server server : configuration.getServersList()) {
      if (server.getServerId() == localServer.getServerId()) {
        continue;
      }
      final Peer peer = peerMap.get(server.getServerId());
      executorService.submit(() -> requestVote(peer));
    }

  }

  private void preVote(Peer peer) {
    log.info("Begin pre vote , send request to {}", peer.getServer());
    RaftProto.VoteRequest.Builder requestBuilder = RaftProto.VoteRequest.newBuilder();
    lock.lock();
    try {
      peer.setVoteGranted(null);
      requestBuilder.setServerId(localServer.getServerId())
        .setTerm(currentTerm)
        .setLastLogIndex(raftLog.getLastLogIndex())
        .setLastLogTerm(getLastLogTerm());
    } finally {
      lock.unlock();
    }

    RaftProto.VoteRequest request = requestBuilder.build();
    peer.getRaftConsensusServiceAsync()
        .preVote(request, new PreVoteResponseCallback(peer, request));
  }

  private void requestVote(Peer peer) {
    log.info("Begin vote request");
    RaftProto.VoteRequest.Builder requestBuilder = RaftProto.VoteRequest.newBuilder();
    lock.lock();
    try {
      peer.setVoteGranted(null);
      requestBuilder.setServerId(localServer.getServerId())
        .setTerm(currentTerm)
        .setLastLogTerm(raftLog.getLastLogIndex())
        .setLastLogTerm(getLastLogTerm());
    } finally {
      lock.unlock();
    }
    RaftProto.VoteRequest request = requestBuilder.build();
    peer.getRaftConsensusServiceAsync().requestVote(
      request, new VoteResponseCallback(peer, request)
    );
  }

  private class PreVoteResponseCallback implements RpcCallback<RaftProto.VoteResponse> {

    private Peer peer;
    private RaftProto.VoteRequest request;

    public PreVoteResponseCallback(Peer peer, RaftProto.VoteRequest request) {
      this.peer = peer;
      this.request = request;
    }

    @Override
    public void success(RaftProto.VoteResponse response) {
      lock.lock();
      log.info("Received pre vote response from server {} " +
          "in term {} (this server's term was {})",
        peer.getServer().getServerId(),
        response.getTerm(),
        currentTerm);
      try {
        peer.setVoteGranted(response.getGranted());
        if(currentTerm != request.getTerm() || state != NodeState.STATE_PRE_CANDIDATE) {
          log.info("ingore prevote prc result");
          return;
        }
        if(response.getTerm() > currentTerm) {
          stepDown(response.getTerm());
        } else {
          if (response.getGranted()) {
            log.info("get pre vote granted from server {} for term {}",
              peer.getServer().getServerId(), currentTerm);
            int voteGrantedNum = 1;
            for (RaftProto.Server server: configuration.getServersList()) {
              if (server.getServerId() == localServer.getServerId()) {
                continue;
              }
              Peer peer1 = peerMap.get(server.getServerId());
              if (peer1.getVoteGranted() != null && peer1.getVoteGranted() == true) {
                voteGrantedNum +=1;
              }
            }
            log.info("Prevote Granted num= {}", voteGrantedNum);
            if (voteGrantedNum > configuration.getServersCount() / 2) {
              log.info("get majority pre vote, serverId={} when pre vote, start vote",
                localServer.getServerId());
              startVote();
            }
          }
        }
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void fail(Throwable throwable) {
      log.warn("pre vote with peer[{}:{}] failed",
        peer.getServer().getEndpoint().getHost(),
        peer.getServer().getEndpoint().getPort());
      peer.setVoteGranted(new Boolean(false));
    }
  }

  private class VoteResponseCallback implements RpcCallback<RaftProto.VoteResponse> {

    private Peer peer;
    private RaftProto.VoteRequest request;

    public VoteResponseCallback(Peer peer, RaftProto.VoteRequest request) {
      this.peer = peer;
      this.request = request;
    }

    @Override
    public void success(RaftProto.VoteResponse response) {
      lock.lock();
      try {
        peer.setVoteGranted(response.getGranted());
        if (currentTerm != request.getTerm() || state != NodeState.STATE_CANDIDATE) {
          log.info("Ignore requestVote Rpc Result");
          return;
        }
        if (response.getTerm() > currentTerm) {
          log.info("Received RequestVote response from server {} in term {} (this server's term was {})",
            peer.getServer().getServerId(),
            response.getTerm(),
            currentTerm);
          stepDown(response.getTerm());
        } else {
          if (response.getGranted()) {
            log.info("Got vote from server {} for term {}", peer.getServer().getServerId(), currentTerm);
            int voteGratedNum = 0;
            if (votedFor == localServer.getServerId()) {
              voteGratedNum += 1;
            }
            for (RaftProto.Server server: configuration.getServersList()) {
              if (server.getServerId() == localServer.getServerId()) {
                continue;
              }
              Peer peer1 = peerMap.get(server.getServerId());
              if (peer1.getVoteGranted() != null && peer1.getVoteGranted() == true) {
                voteGratedNum += 1;
              }
            }
            log.info("Vote Granteeed num : {}", voteGratedNum);
            if (voteGratedNum > configuration.getServersCount() /2) {
              log.info("Got Majoriy vote, serverId={} become leader", localServer.getServerId());
              becomeLeader();
            }
          } else {
            log.info("Vote denied by server {} with term {}, my term is {}",
              peer.getServer().getServerId(), response.getTerm(), currentTerm);
          }
        }
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void fail(Throwable throwable) {
      log.warn("requestVote with peer[{}:{}] failed",
        peer.getServer().getEndpoint().getHost(),
        peer.getServer().getEndpoint().getPort());
      peer.setVoteGranted(new Boolean(false));
    }
  }

  // in lock
  public void stepDown(long newTerm) {
    log.info("Step Down at newTerm:{} for server: {}", newTerm, localServer);
    if (currentTerm > newTerm) {
      log.error("can't be happened");
      return;
    }
    if (currentTerm < newTerm) {
      currentTerm = newTerm;
      leaderId = 0;
      votedFor = 0;
      raftLog.updateMetaData(currentTerm, votedFor, null, null);
    }
    state = NodeState.STATE_FOLLOWER;
    // stop heartbeat
    if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
      heartbeatScheduledFuture.cancel(true);
    }
    resetElectionTimer();
  }

  private void becomeLeader() {
    state = NodeState.STATE_LEADER;
    leaderId = localServer.getServerId();

    log.info("I become the leader for server:{}", localServer);
    // stop vote timer
    if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
      electionScheduledFuture.cancel(true);
    }

    startNewHeartbeat();
  }

  // heartbeat timer, append entries
  // in lock
  private void resetHeartbeatTimer() {
    if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
      heartbeatScheduledFuture.cancel(true);
    }
    heartbeatScheduledFuture = scheduledExecutorService.schedule(new Runnable() {
      @Override
      public void run() {
        startNewHeartbeat();
      }
    }, raftOptions.getHeartbeatPeriodMilliseconds(), TimeUnit.MILLISECONDS);
  }

  // in lock, 开始心跳，对leader有效
  private void startNewHeartbeat() {
    log.info("start new heartbeat, peers={}", peerMap.keySet());
    for (final Peer peer : peerMap.values()) {
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          appendEntries(peer);
        }
      });
    }
    resetHeartbeatTimer();
  }

  public void appendEntries(Peer peer) {
    RaftProto.AppendEntriesRequest.Builder requestBuilder = RaftProto.AppendEntriesRequest.newBuilder();

    long prevLogIndex;
    long numEntries;

    lock.lock();
    try {
      long firstLogIndex = raftLog.getFirstLogIndex();
      Validate.isTrue(peer.getNextIndex() >= firstLogIndex);
      prevLogIndex = peer.getNextIndex() - 1;
      long prevLogTerm ;
      if(prevLogIndex == 0) {
        prevLogTerm = 0;
      } else {
        prevLogTerm = raftLog.getEntryTerm(prevLogIndex);
      }
      requestBuilder.setServerId(localServer.getServerId());
      requestBuilder.setTerm(currentTerm);
      requestBuilder.setPrevLogTerm(prevLogTerm);
      requestBuilder.setPrevLogIndex(prevLogIndex);
      numEntries = packEntries(peer.getNextIndex(), requestBuilder);
      requestBuilder.setCommitIndex(Math.min(commitIndex, prevLogIndex + numEntries));
    } finally {
      lock.unlock();
    }

    RaftProto.AppendEntriesRequest request = requestBuilder.build();
    RaftProto.AppendEntriesResponse response = peer.getRaftConsensusServiceAsync().appendEntries(request);

    lock.lock();
    try {
      if (response == null) {
        log.warn("appendEntries with peer[{}:{}] failed",
          peer.getServer().getEndpoint().getHost(),
          peer.getServer().getEndpoint().getPort());
        if (!ConfigurationUtils.containsServer(configuration, peer.getServer().getServerId())) {
          peerMap.remove(peer.getServer().getServerId());
          peer.getRpcClient().stop();
        }
        return;
      }
      log.info("AppendEntries response[{}] from server {} " +
          "in term {} (my term is {})",
        response.getResCode(), peer.getServer().getServerId(),
        response.getTerm(), currentTerm);

      if (response.getTerm() > currentTerm) {
        stepDown(response.getTerm());
      } else {
        if (response.getResCode() == RaftProto.ResCode.RES_CODE_SUCCESS) {
          peer.setMatchIndex(prevLogIndex + numEntries);
          peer.setNextIndex(peer.getMatchIndex() + 1);
          if (ConfigurationUtils.containsServer(configuration, peer.getServer().getServerId())) {
            advanceCommitIndex();
          } else {
            if (raftLog.getLastLogIndex() - peer.getMatchIndex() <= raftOptions.getCatchupMargin()) {
              log.debug("peer catch up the leader");
              peer.setCatchUp(true);
              // signal the caller thread
              catchUpCondition.signalAll();
            }
          }
        } else {
          peer.setNextIndex(response.getLastLogIndex() + 1);
        }
      }

    } finally {
      lock.unlock();
    }

  }

  private void advanceCommitIndex() {
    // 获取quorum matchIndex
    int peerNum = configuration.getServersList().size();
    long[] matchIndexes = new long[peerNum];
    int i = 0;
    for (RaftProto.Server server : configuration.getServersList()) {
      if (server.getServerId() != localServer.getServerId()) {
        Peer peer = peerMap.get(server.getServerId());
        matchIndexes[i++] = peer.getMatchIndex();
      }
    }
    matchIndexes[i] = raftLog.getLastLogIndex();
    Arrays.sort(matchIndexes);
    long newCommitIndex = matchIndexes[peerNum / 2];
    log.debug("newCommitIndex={}, oldCommitIndex={}", newCommitIndex, commitIndex);
    if (raftLog.getEntryTerm(newCommitIndex) != currentTerm) {
      log.debug("newCommitIndexTerm={}, currentTerm={}",
        raftLog.getEntryTerm(newCommitIndex), currentTerm);
      return;
    }
    if (commitIndex >= newCommitIndex) {
      return;
    }
    long oldCommitIndex = commitIndex;
    commitIndex = newCommitIndex;
    raftLog.updateMetaData(currentTerm, null, raftLog.getFirstLogIndex(), commitIndex);
    lastAppliedIndex = commitIndex;
    log.debug("commitIndex={} lastAppliedIndex={}", commitIndex, lastAppliedIndex);
    commitIndexCondition.signalAll();
  }


  private long packEntries(long nextIndex, RaftProto.AppendEntriesRequest.Builder requestBuilder) {
    long lastIndex = Math.min(raftLog.getLastLogIndex(),
      nextIndex + raftOptions.getMaxLogEntriesPerRequest() - 1);
    for (long index = nextIndex; index <= lastIndex; index++) {
      RaftProto.LogEntry entry = raftLog.getEntry(index);
      requestBuilder.addEntries(entry);
    }
    return lastIndex - nextIndex + 1;
  }

  private int getElectionTimeoutMs() {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    int randomElectionTimeout = raftOptions.getElectionTimeoutMilliseconds()
      + random.nextInt(0, raftOptions.getElectionTimeoutMilliseconds());
    log.info("new election time is after {} ms", randomElectionTimeout);
    return randomElectionTimeout;
  }

  public long getLastLogTerm() {
    long lastLogIndex = raftLog.getLastLogIndex();
    if (lastLogIndex >= raftLog.getFirstLogIndex()) {
      return raftLog.getEntryTerm(lastLogIndex);
    } else {
      return -1;
    }
  }

}
