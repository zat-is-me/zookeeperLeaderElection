package zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * @author Tatek Ahmed on 3/1/2022
 **/

public class LeaderElection implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ELECTION_NAMESPACE = "/election";

    private ZooKeeper zooKeeper;
    private String currentZnodeName;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZookeeper();
        leaderElection.volunteerForLeadership();
        leaderElection.electLeader();
        leaderElection.run();
        leaderElection.close();
        System.out.println("Disconnecting from Zookeeper, exiting application");
    }

    private void volunteerForLeadership() throws InterruptedException, KeeperException {
        String prefix = "c_";
        String znodeFullPath;

        try {
            znodeFullPath = zooKeeper.create(ELECTION_NAMESPACE + "/" + prefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        } catch (KeeperException.NoNodeException e) {
            znodeFullPath = zooKeeper.create(ELECTION_NAMESPACE, new byte[1], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        System.out.println("znode name = "+znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace("/election/","");
    }
    public void electLeader() throws InterruptedException, KeeperException {
        List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE,false);
        Collections.sort(children);
        String smallestChild = children.get(0);

        if (smallestChild.equals(currentZnodeName)){
            System.out.println("I am the leader");
            return;
        }
        System.out.println("I am not the leader " +smallestChild +" is the leader");
    }

    private void close() throws InterruptedException {
        this.zooKeeper.close();
    }

    private void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS,SESSION_TIMEOUT,this);
    }

    public void run() throws InterruptedException{
        synchronized (zooKeeper){
            zooKeeper.wait();
        }
    }
    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None:
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected){
                    System.out.println("Successfully connected to zookeeper");
                }
                else {
                    synchronized (zooKeeper){
                        System.out.println("\nDisconnecting from Zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
        }
    }
}
