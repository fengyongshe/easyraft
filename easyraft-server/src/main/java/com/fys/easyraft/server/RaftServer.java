package com.fys.easyraft.server;

import com.baidu.brpc.server.RpcServer;
import com.fys.easyraft.core.conf.RaftOptions;
import com.fys.easyraft.core.peer.RaftNode;
import com.fys.easyraft.core.protobuf.RaftProto;
import com.fys.easyraft.core.service.RaftConsensusService;
import com.fys.easyraft.core.service.impl.RaftConsensusServiceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class RaftServer {

  public static void main(String[] args) {

    if (args.length != 3) {
      System.out.printf("Usage: ./run_server.sh DATA_PATH CLUSTER CURRENT_NODE\n");
      System.exit(-1);
    }
    // parse args
    // raft data dir
    String dataPath = args[0];
    // peers, format is "host:port:serverId,host2:port2:serverId2"
    String servers = args[1];
    String[] splitArray = servers.split(",");
    List<RaftProto.Server> serverList = new ArrayList<>();
    for (String serverString : splitArray) {
      RaftProto.Server server = parseServer(serverString);
      serverList.add(server);
    }
    // local server
    RaftProto.Server localServer = parseServer(args[2]);

    // 初始化RPCServer
    RpcServer server = new RpcServer(localServer.getEndpoint().getPort());

    RaftOptions raftOptions = new RaftOptions();
    raftOptions.setDataDir(dataPath);
    raftOptions.setSnapshotMinLogSize(10 * 1024);
    raftOptions.setSnapshotPeriodSeconds(30);
    raftOptions.setMaxSegmentFileSize(1024 * 1024);

    RaftNode raftNode = new RaftNode(raftOptions, serverList, localServer);

    RpcServer rpcServer = new RpcServer(localServer.getEndpoint().getPort());
    RaftConsensusService raftConsensusService = new RaftConsensusServiceImpl(raftNode);

    log.info("Register consensus server");
    rpcServer.registerService(raftConsensusService);

    log.info("Start the rpcServer");
    rpcServer.start();
    raftNode.init();

  }

  private static RaftProto.Server parseServer(String serverString) {
    String[] splitServer = serverString.split(":");
    String host = splitServer[0];
    Integer port = Integer.parseInt(splitServer[1]);
    Integer serverId = Integer.parseInt(splitServer[2]);
    RaftProto.Endpoint endPoint = RaftProto.Endpoint.newBuilder()
      .setHost(host).setPort(port).build();
    RaftProto.Server.Builder serverBuilder = RaftProto.Server.newBuilder();
    RaftProto.Server server = serverBuilder.setServerId(serverId).setEndpoint(endPoint).build();
    return server;
  }

}
