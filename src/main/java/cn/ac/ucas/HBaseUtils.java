package cn.ac.ucas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtils {

    private static Configuration configuration;
    private static Connection connection;

    /**
     * 初始化 HBase 配置对象和连接对象
     */
    static {
        configuration = HBaseConfiguration.create();
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建命名空间
     *
     * @param namespace 命名空间名称
     * @throws IOException
     */
    public static void createNamespace(String namespace) throws IOException {

    }

    /**
     * 删除命名空间及其中的所有表
     *
     * @param namespace 命名空间名称
     * @throws IOException
     */
    public static void deleteNamespace(String namespace) throws IOException {

    }

    /**
     * 创建表
     *
     * @param tableName 表名
     * @param families  列族
     * @throws IOException
     */
    public static void createTable(String tableName, String... families) throws IOException {

    }

    /**
     * 删除表
     *
     * @param tableName 表名
     * @throws IOException
     */
    public static void deleteTable(String tableName) throws IOException {

    }

    /**
     * 插入数据
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @param family    列族
     * @param qualifier 列标识符
     * @param value     值
     * @throws IOException
     */
    public static void putData(String tableName, String rowKey, String family, String qualifier, String value)
            throws IOException {

    }

    /**
     * 批量插入数据
     *
     * @param tableName 表名
     * @param puts      Put 对象列表
     * @throws IOException
     */
    public static void putBatchData(String tableName, List<Put> puts) throws IOException {

    }

    /**
     * 删除数据
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @param family    列族
     * @param qualifier 列标识符
     * @throws IOException
     */
    public static void deleteData(String tableName, String rowKey, String family, String qualifier) throws IOException {

    }

    /**
     * 获取单行数据
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @return Result 对象
     * @throws IOException
     */
    public static Result getRow(String tableName, String rowKey) throws IOException {

    }

    /**
     * 扫描表
     *
     * @param tableName 表名
     * @param filter    过滤器
     * @param pageSize  每页记录数
     * @return ResultScanner 对象
     * @throws IOException
     */
    public static ResultScanner scanTable(String tableName, Filter filter, int pageSize) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        if (filter != null) {
            scan.setFilter(filter);
        }
        scan.setCaching(pageSize);
        PageFilter pageFilter = new PageFilter(pageSize);
        List<Result> resultList = new ArrayList<>();
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        do {
            result = resultScanner.next();
            if (result != null) {
                resultList.add(result);
            }
        } while (result != null && resultList.size() < pageSize);
        return new ResultScanner() {

            int index = 0;

            @Override
            public Result next() throws IOException {
                if (index >= resultList.size()) {
                    return null;
                }
                return resultList.get(index++);
            }

            @Override
            public void close() {
                resultScanner.close();
            }

            @Override
            public Iterator<Result> iterator() {
                return resultList.iterator();
            }
        };
    }

    /**
     * 关闭连接
     *
     * @throws IOException
     */
    public static void close() throws IOException {

    }

    public static void main(String[] args) {

    }
}
