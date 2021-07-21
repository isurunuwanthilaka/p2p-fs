package com.project.dfs;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;

@SpringBootApplication
public class App {
    public static int nodePort;
    @Value("${server.port}")
    public void setOwnerPort(int nodePort) {
        this.nodePort=nodePort;
    }

    @PostConstruct
    public void init() {
    }

    public static void main(String[] args) throws IOException {

        SpringApplication.run(App.class, args);

        String bootstrapIp = "localhost";

        int bootstrapPort = 55555;

        String bootstrapAddress = System.getProperty("bootstrapAddress");

        if (Node.checkForNotEmpty(bootstrapAddress)) {
            String[] values = bootstrapAddress.split(":");
            bootstrapIp = values[0];
            bootstrapPort = Integer.parseInt(values[1]);
        }

        String nodeIp = Node.getLocalNodeIp();

        String nodeIpAddress = System.getProperty("nodeAddress");
        if (Node.checkForNotEmpty(nodeIpAddress)) {
            String[] values = nodeIpAddress.split(":");
            nodeIp = values[0];
            nodePort = Integer.parseInt(values[1]);
        }

        String nodeName = "node" + nodePort;

        String nodeNameSystemProp = System.getProperty("nodeName");
        if (Node.checkForNotEmpty(nodeNameSystemProp)) {
            nodeName = nodeNameSystemProp;
        }

        int hopsCount = Node.default_hops_count;

        String hopsCountSystemProp = System.getProperty("hopsCount");
        if (Node.checkForNotEmpty(hopsCountSystemProp)) {
            hopsCount = Integer.parseInt(hopsCountSystemProp);
        }

        Node node = new Node(bootstrapIp, bootstrapPort, nodeName, nodeIp, nodePort, hopsCount);

        Node.log(Node.INFO, "This node : " + nodeIp + ":" + nodePort);

        //1. assign own file list
        node.assignFiles();

        //2. connect to bootstrap and get peers
        List<Peer> peersToConnect = node.connectToBootstrapNode();

        Node.log(Node.INFO, "Peers : " + peersToConnect);

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
