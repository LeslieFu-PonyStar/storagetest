package cn.ac.ucas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseUtil {

    private ThreadLocal<Connection> connHolder;

    public HBaseUtil(String hostName,String post) throws IOException {
        this.connHolder = new ThreadLocal<Connection>();
        Connection conn = connHolder.get();
        if (conn == null){
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", hostName);
            conf.set("hbase.zookeeper.property.clientPort", post);
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
    }


    /**
     * 创建命名空间
     *
     * @param namespace 命名空间名称
     * @throws IOException
     */
    public void createNamespace(String namespace) throws IOException {
        Admin admin = this.connHolder.get().getAdmin();
        NamespaceDescriptor namespaceDescriptor =
                NamespaceDescriptor.create(namespace)
                        .build();
        admin.createNamespace(namespaceDescriptor);
        admin.close();
    }
    
    /**
     * 创建命名空间
     * @param namespace
     * @param configs 可以添加多个配置选项
     * @throws IOException
     */
    public void createNamespace(String namespace, Map<String, String> configs) throws IOException {
        Admin admin = this.connHolder.get().getAdmin();
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            String configKey = entry.getKey();
            String configValue = entry.getValue();
            builder.addConfiguration(configKey, configValue);
        }
        NamespaceDescriptor namespaceDescriptor = builder.build();
        admin.createNamespace(namespaceDescriptor);
        admin.close();
    }

    /**
     * 删除命名空间及其中的所有表
     *
     * @param namespace 命名空间名称
     * @throws IOException
     */
    public void deleteNamespace(String namespace) throws IOException {
        Admin admin = this.connHolder.get().getAdmin();
        admin.getNamespaceDescriptor(namespace);
        boolean namespaceExists = false;
        try {
            admin.getNamespaceDescriptor(namespace);
            namespaceExists = true;
        } catch (NamespaceNotFoundException e) {
            System.out.println("Namespace " + namespace + " not found.");
        }
        // Delete the namespace if it exists
        if (namespaceExists) {
            admin.deleteNamespace(namespace);
            System.out.println("Namespace " + namespace + " deleted successfully.");
        } else {
            System.out.println("Namespace deletion skipped because the namespace does not exist.");
        }
        admin.close();
    }

    /**
     * 创建表
     *
     * @param tableName 表名
     * @param families  列族
     * @throws IOException
     */
    public void createTable(String tableName, String[] families) throws IOException {
        HBaseAdmin admin = (HBaseAdmin)this.connHolder.get().getAdmin();
        if (admin.tableExists(tableName)) {
            return;
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for(String family : families){
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
            tableDescriptor.addFamily(columnDescriptor);
        };
        admin.createTable(tableDescriptor);
        admin.close();
    }

    /**
     * 删除表的函数
     *
     * @param tableName 表名
     * @throws IOException
     */
    public void deleteTable(String tableName) throws IOException {
        HBaseAdmin admin = (HBaseAdmin)this.connHolder.get().getAdmin();
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println("Table " + tableName + " deleted successfully.");
        admin.close();
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
    public void putData(String tableName, String rowKey, String family, String qualifier, String value)
            throws IOException {
        Connection conn = this.connHolder.get();
        Table table = conn.getTable(TableName.valueOf(tableName));
        // 创建 Put 对象并指定行键
        Put put = new Put(Bytes.toBytes(rowKey));
        // 添加列族、列标识符、值
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        // 执行插入操作
        table.put(put);
    }

    /**
     * 批量插入数据
     *
     * @param tableName 表名
     * @param puts      Put 对象列表
     * @throws IOException
     */
    public void putBatchData(String tableName, List<Put> puts) throws IOException {
        Connection conn = this.connHolder.get();
        Table table = conn.getTable(TableName.valueOf(tableName));
        table.put(puts);
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
    public void deleteData(String tableName, String rowKey, String family, String qualifier) throws IOException {

    }

    /**
     * 获取单行数据
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @return Result 对象
     * @throws IOException
     */
    public Result getRow(String tableName, String rowKey) throws IOException {
        return null;
    }

    // /**
    //  * 扫描表
    //  *
    //  * @param tableName 表名
    //  * @param filter    过滤器
    //  * @param pageSize  每页记录数
    //  * @return ResultScanner 对象
    //  * @throws IOException
    //  */
    // public ResultScanner scanTable(String tableName, Filter filter, int pageSize) throws IOException {
    //     Table table = connection.getTable(TableName.valueOf(tableName));
    //     Scan scan = new Scan();
    //     if (filter != null) {
    //         scan.setFilter(filter);
    //     }
    //     scan.setCaching(pageSize);
    //     PageFilter pageFilter = new PageFilter(pageSize);
    //     List<Result> resultList = new ArrayList<>();
    //     ResultScanner resultScanner = table.getScanner(scan);
    //     Result result;
    //     do {
    //         result = resultScanner.next();
    //         if (result != null) {
    //             resultList.add(result);
    //         }
    //     } while (result != null && resultList.size() < pageSize);
    //     return new ResultScanner() {

    //         int index = 0;

    //         @Override
    //         public Result next() throws IOException {
    //             if (index >= resultList.size()) {
    //                 return null;
    //             }
    //             return resultList.get(index++);
    //         }

    //         @Override
    //         public void close() {
    //             resultScanner.close();
    //         }

    //         @Override
    //         public Iterator<Result> iterator() {
    //             return resultList.iterator();
    //         }
    //     };
    // }

    /**
     * 关闭连接
     *
     * @throws IOException
     */
    public void close() throws IOException {
        Connection conn = connHolder.get();
        if(conn!=null){
            conn.close();
            connHolder.remove();
        }

    }
}