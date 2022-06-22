# easyraft
raft算法示例

1) 编译
$mvn clean package

2) 编译成功后进入easyraft-server/target

3）启动   
$java -jar easyraft-server-1.0-SNAPSHOT-jar-with-dependencies.jar /tmp/easyraft-server localhost:8088:1,localhost:8089:2,localhost:8090:3 localhost:8088:1
$java -jar easyraft-server-1.0-SNAPSHOT-jar-with-dependencies.jar /tmp/easyraft-server localhost:8088:1,localhost:8089:2,localhost:8090:3 localhost:8089:2
$java -jar easyraft-server-1.0-SNAPSHOT-jar-with-dependencies.jar /tmp/easyraft-server localhost:8088:1,localhost:8089:2,localhost:8090:3 localhost:8090:3

确定三个server后，可以看到选举过程及数据同步执行

