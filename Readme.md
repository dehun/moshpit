# Moshpit #
## Intro ##
Moshpit is AP P2P based service discovery. It was heavily inspired by netflix-eureka, but deliberately was made to be more lightweight.
Instances of application register themselfs through the RESTful interface and continiously heartbeating(pinging) to prove that they are still alive.
After missing heartbeat for x seconds - instance considered to be dead.
All data is stored in memory.

## Running ##
    git clone https://github.com/dehun/moshpit.git 
    sbt assembly
    cp ./src/main/resources/application.conf node_b.conf
    emacs -nw -q node_b.conf # fix seed nodes and netty.tcp listening port and interface
    java  -Dconfig.file="/home/dehun/dev/mine/moshpit/node_b.conf" -jar /home/dehun/dev/mine/moshpit/target/scala-2.12/moshpit-assembly-1.0.jar
    

## Interface ##
Moshpit provides super simple RESTful interface.

### querying existance apps ###
    curl http://localhost:8081/apps/myApplicationId/instance/myInstanceId
    {"ids":["myApp"]}

### getting intsance information ###
    curl http://localhost:8080/app/myApp/instance/myInstanceGuid
    {"appId":"myApp","instanceGuid":"myInstanceGuid","lastUpdated":"20170720T130916+0200","data":"somejsondata123"}
    
### registering/updating instance ###
    curl -XPUT -d "somejsondata123" http://localhost:8080/app/myApp/instance/myInstanceGuid
    {"note":"updated myApp::myInstanceGuid"}%
    
### pinging/renewing instance lease ###
    curl -XPATCH http://localhost:8080/app/myApp/instance/myInstanceGuid
    {"note":"pinged"}
    
### querying app instances ###
    url http://localhost:8080/app/myApp    
    {"instances":["qwe8","myInstanceGuid"]}
    
### deleting instance ###
    curl -XDELETE http://localhost:8080/app/myApp/instance/myInstanceGuid
    {"note":"instance was successfully deleted"}

### Errors ###
It will return 404 for cases when you are trying to ping/delete non existing instance.
In case if application does not exists - it considered to exist with 0 instances.

## AP and data replication ##
Plenty of companies build their service discovery solutions on CP systems. 
And in some cases that is a good choice. However in some cases CP only introduces additional risks.
In case if CP cluster experiences majority loss - you will not be able to query instances information and update instances.
On most solutions (zookeeper, consul) you can still though perform stale reads, however new instances will not be able to register themselfs.
AP of course introduces cases when 2 nodes containing conflicting information.
Conflict resolution in Moshpit is based on vector clocks and last update time for instance.
In case if we see conflicting vector clocks - we resolve conflict by picking newest by lastUpdatedTime.

## P2p ##
P2p in Moshpit is built on top of akka.remote. It uses simple gossip protocol to advertise peers.   
  

