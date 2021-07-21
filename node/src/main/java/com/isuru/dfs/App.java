package com.isuru.dfs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.List;

import static com.isuru.dfs.Node.*;

@SpringBootApplication
public class App {
    public static void main(String[] args) throws IOException {

        SpringApplication.run(App.class, args);

        String bootstrapIp = "localhost";

        int bootstrapPort = 55555;

        String bootstrapAddress = System.getProperty("bootstrapAddress");

        if (checkForNotEmpty(bootstrapAddress)) {
            String[] values = bootstrapAddress.split(":");
            bootstrapIp = values[0];
            bootstrapPort = Integer.parseInt(values[1]);
        }

        String nodeIp = getLocalNodeIp();

//        Random random = new Random();
//
//        int randomNumber = random.nextInt(1000);
//
//        int nodePort = 10000 + randomNumber;
        int nodePort = 7780;

        String nodeIpAddress = System.getProperty("nodeAddress");
        if (checkForNotEmpty(nodeIpAddress)) {
            String[] values = nodeIpAddress.split(":");
            nodeIp = values[0];
            nodePort = Integer.parseInt(values[1]);
        }

        String nodeName = "node" + nodePort;

        String nodeNameSystemProp = System.getProperty("nodeName");
        if (checkForNotEmpty(nodeNameSystemProp)) {
            nodeName = nodeNameSystemProp;
        }

        int hopsCount = default_hops_count;

        String hopsCountSystemProp = System.getProperty("hopsCount");
        if (checkForNotEmpty(hopsCountSystemProp)) {
            hopsCount = Integer.parseInt(hopsCountSystemProp);
        }

        Node node = new Node(bootstrapIp, bootstrapPort, nodeName, nodeIp, nodePort, hopsCount);

        log(INFO, "This node : " + nodeIp + ":" + nodePort);

        //1. assign own file list
        node.assignFiles();

        //2. connect to bootstrap and get peers
        List<Peer> peersToConnect = node.connectToBootstrapNode();

        log(INFO, "Peers : " + peersToConnect);

        //3. connect to peers from above
        node.connectToPeers(peersToConnect, node.nodeIp, node.nodePort);

        //4. start listening
        //startListening(port);
        (new NodeThread(node)).start();

        //5. start listening to incoming search queries
        node.startListeningForQueries();
        System.exit(0);
    }
}
