package io.ymq.example.cana;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import io.ymq.example.util.AbstractCanalClientTest;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.net.InetSocketAddress;

/**
 * 单机模式的测试例子
 *
 * @author jianghang 2013-4-15 下午04:19:20
 * @version 1.0.4
 */
public class SimpleCanalTest extends AbstractCanalClientTest {

    public SimpleCanalTest(String destination) {
        super(destination);
    }

    public static void main(String args[]) {

        // 根据ip，直接创建链接，无HA的功能
        String destination = "example";

        // String ip = AddressUtils.getHostIp();

        String ip = "192.168.252.125";

        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(ip, 11111), destination, "", "");

        final SimpleCanalTest clientTest = new SimpleCanalTest(destination);
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