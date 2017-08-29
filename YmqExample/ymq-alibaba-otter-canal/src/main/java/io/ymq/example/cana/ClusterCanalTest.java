package io.ymq.example.cana;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import io.ymq.example.util.AbstractCanalClientTest;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.net.InetSocketAddress;

/**
 * 集群模式的测试例子
 *
 * @author jianghang 2013-4-15 下午04:19:20
 * @version 1.0.4
 */
public class ClusterCanalTest extends AbstractCanalClientTest {

    public ClusterCanalTest(String destination) {
        super(destination);
    }

    public static void main(String args[]) {
        String destination = "example";

        String ip = "192.168.252.125";

        // 基于固定canal server的地址，建立链接，其中一台server发生crash，可以支持failover
        //CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(ip, 11111), "example", "", "");


        // 基于zookeeper动态获取canal server的地址，建立链接，其中一台server发生crash，可以支持failover
        CanalConnector connector = CanalConnectors.newClusterConnector("127.0.0.1:2181", destination, "", "");

        final ClusterCanalTest clientTest = new ClusterCanalTest(destination);
        clientTest.setConnector(connector);
        clientTest.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                try {
                    logger.info("## stop the canal client");
                    clientTest.stop();
                } catch (Throwable e) {
                    logger.warn("##something goes wrong when stopping canal:\n{}", ExceptionUtils.getFullStackTrace(e));
                } finally {
                    logger.info("## canal client is down.");
                }
            }

        });

    }
}
