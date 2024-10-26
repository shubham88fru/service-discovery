import cluster.management.LeaderElection;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

import static org.apache.zookeeper.Watcher.Event.EventType.None;

public class Application implements Watcher {


    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";

    /**
     * Zookeeper server constantly keeps track of the connected
     * clients to check if they are still alive. If the zookeeper
     * server doesn't hear from the clients in this time period,
     * it assumes that the client is dead.
     */
    private static final int SESSION_TIMEOUT = 3000;


    /**
     * The zookeeper client object.
     * Will help each node interact/connect/talk to zookeeper server
     * and listen to events from the zookeeper server as well.
     */
    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        Application application = new Application();
        ZooKeeper zooKeeper = application.connectToZookeeper();
        LeaderElection leaderElection = new LeaderElection(zooKeeper);

        leaderElection.selfElectForLeader(); //each node will put try to put forward itself to be the leader.
        leaderElection.electLeader(); //identify the leader.

        /*
         Put the main thread to wait state, otherwise,
         the app will stop as soon as main() finishes
         */
        application.run();
    }

    private void run() throws InterruptedException {
        /**
         * This trick will make the main thread
         * get into a wait state while the zookeeper
         * client libraries two threads (io and event) keep
         * running and doing their job.
         */
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }

        //If here, means main thread is woken up,
        //means received a disconnect event. so
        //shutdown gracefully.
        close();
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
        System.out.println("Disconnected from zookeeper. exiting..");

    }

    private ZooKeeper connectToZookeeper() throws IOException {
        /**
         * On initialization, the zookeeper client created two threads, out of
         * which one is the events thread. This thread receives any event sent out
         * from the zookeeper server.
         *
         * Passing the current instance as a watcher means that when an event arrives
         * from the zookeeper server, the zookeeper client library will invoke
         * the `process()` method on this instance.
         */
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
        return zooKeeper;
    }
    @Override
    public void process(WatchedEvent watchedEvent) {
        //general connection,disconnect events don't have a type.
        switch (watchedEvent.getType()) {
            case None -> {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) { //connected.
                    System.out.println("Node connected to zookeeper successfully.");
                } else { //disconnected

                    //wake up the main thread.
                    synchronized (zooKeeper) {
                        System.out.println("Node disconnected from zookeeper..");
                        zooKeeper.notifyAll();
                    }
                }
            }
        }

    }
}
