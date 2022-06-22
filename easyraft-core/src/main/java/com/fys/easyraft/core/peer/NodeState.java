package com.fys.easyraft.core.peer;

public enum NodeState {
  STATE_FOLLOWER,
  STATE_PRE_CANDIDATE,
  STATE_CANDIDATE,
  STATE_LEADER
}
