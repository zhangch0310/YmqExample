package io.ymq.example.cana;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import java.net.InetSocketAddress;
import java.util.List;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import io.ymq.example.util.RedisUtil;
import org.jetbrains.annotations.NotNull;

/**
 * 描述: 使用 Alibaba Canal 增量订阅&消费组件,同步MySQL数据到 Redis
 * author: yanpenglei
 * Date: 2017/8/29 13:47 
 */
public class SimpleCanalSyncRedisTest {
    public static void main(String args[]) {

        // String ip = AddressUtils.getHostIp();

        String ip = "192.168.252.125";

        // 创建链接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(ip, 11111), "example", "", "");

        int batchSize = 1000;
        int emptyCount = 0;
        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            int totalEmptyCount = 120;
            while (emptyCount < totalEmptyCount) {

                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据

                long batchId = message.getId();

                int size = message.getEntries().size();

                if (batchId == -1 || size == 0) {
                    emptyCount++;
                    System.out.println("empty count : " + emptyCount);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {}
                } else {

                    emptyCount = 0;
                    // System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
                    printEntry(message.getEntries());
                }

                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }

            System.out.println("empty too many times, exit");

        } finally {
            connector.disconnect();
        }
    }
    protected static void printEntry(@NotNull List < Entry > entrys) {
        for (Entry entry: entrys) {

            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChage = null;

            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
            }

            EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s", entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(), entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), eventType));

            /**
             * 使用 Alibaba Canal 增量订阅&消费组件,同步MySQL数据到 Redis
             */
            for (RowData  redisRowData : rowChage.getRowDatasList()) {
                if (eventType == EventType.DELETE) {
                    System.out.println("-------> Redis Delete");
                    redisDelete(redisRowData.getBeforeColumnsList());
                } else if (eventType == EventType.INSERT) {
                    System.out.println("-------> Redis INSERT");
                    redisInsert(redisRowData.getAfterColumnsList());
                } else {
                    System.out.println("-------> before");
                    printColumn(redisRowData.getBeforeColumnsList());
                    System.out.println("-------> after");
                    redisUpdate(redisRowData.getAfterColumnsList());
                }
            }

        }
    }
    protected static void printColumn(List < Column > columns) {
        for (Column column: columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }

    private static void redisInsert(List < Column > columns) {
        JSONObject json = new JSONObject();
        for (Column column: columns) {
            json.put(column.getName(), column.getValue());
        }
        if (columns.size() > 0) {
            RedisUtil.stringSet("ymq-group:" + columns.get(0).getValue(), json.toJSONString());
        }
    }

    private static void redisUpdate(List < Column > columns) {
        JSONObject json = new JSONObject();
        for (Column column: columns) {
            json.put(column.getName(), column.getValue());
        }
        if (columns.size() > 0) {
            RedisUtil.stringSet("ymq-group:" + columns.get(0).getValue(), json.toJSONString());
        }
    }

    private static void redisDelete(List < Column > columns) {
        JSONObject json = new JSONObject();
        for (Column column: columns) {
            json.put(column.getName(), column.getValue());
        }
        if (columns.size() > 0) {
            RedisUtil.delKey("ymq-group:" + columns.get(0).getValue());
        }
    }
}