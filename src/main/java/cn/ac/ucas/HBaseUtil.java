package cn.ac.ucas;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtil {

    private ThreadLocal<Connection> connHolder;

    public HBaseUtil(String hostName, String post) throws IOException {
        this.connHolder = new ThreadLocal<Connection>();
        Connection conn = connHolder.get();
        if (conn == null) {
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
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace)
                .build();
        admin.createNamespace(namespaceDescriptor);
        admin.close();
    }

    /**
     * 创建命名空间
     * 
     * @param namespace
     * @param configs   可以添加多个配置选项
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
        HBaseAdmin admin = (HBaseAdmin) this.connHolder.get().getAdmin();
        if (admin.tableExists(tableName)) {
            System.out.println("This table exists.");
            admin.close();
            return;
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String family : families) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
            tableDescriptor.addFamily(columnDescriptor);
        }
        ;
        admin.createTable(tableDescriptor);
        admin.close();
    }

    /**
     * 为table新增列族的函数
     * 
     * @param tableName
     * @param families
     * @throws IOException
     */
    public void addColumnFamliy(String tableName, String[] families) throws IOException {
        HBaseAdmin admin = (HBaseAdmin) this.connHolder.get().getAdmin();
        if (!admin.tableExists(tableName)) {
            System.out.println("This table nos exist, please create it.");
            admin.close();
            return;
        }
        HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(tableName));
        for (String family : families) {
            tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes(family)));
        }
        admin.modifyTable(Bytes.toBytes(tableName), tableDesc);
        admin.close();
    }

    /**
     * 删除表的函数
     *
     * @param tableName 表名
     * @throws IOException
     */
    public void deleteTable(String tableName) throws IOException {
        HBaseAdmin admin = (HBaseAdmin) this.connHolder.get().getAdmin();
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
     * @param isAppend  是否追加写，如果对应值不存在，则会新建；如果该值为false，则不论原来值是否存在都会覆盖写
     * @throws IOException
     */
    public void putData(String tableName, String rowKey, String family, String qualifier, String value,
            boolean isAppend)
            throws IOException {
        Connection conn = this.connHolder.get();
        Table table = conn.getTable(TableName.valueOf(tableName));
        if (!isAppend) {
            // 创建 Put 对象并指定行键
            Put put = new Put(Bytes.toBytes(rowKey));
            // 添加列族、列标识符、值
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            // 执行插入操作
            table.put(put);
        } else {
            Append append = new Append(Bytes.toBytes(rowKey));
            append.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.append(append);
        }
        table.close();

    }

    /**
     * 批量插入数据，需要提前将数据都正确地导入到List<Put>中
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
     * 批量追加数据，需要提前将数据都正确地导入到append中，具体可参照putData中的实现使用append.add()批量新建append任务
     * 
     * @param tableName 表名
     * @param puts      Put 对象列表
     * @throws IOException
     */
    public void appendBatchData(String tableName, Append append) throws IOException {
        Connection conn = this.connHolder.get();
        Table table = conn.getTable(TableName.valueOf(tableName));
        table.append(append);
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
        Connection conn = this.connHolder.get();
        // 根据表名获取 HTable 对象
        Table table = conn.getTable(TableName.valueOf(tableName));
        try {
            // 创建 Delete 对象，并指定要删除的行键、列族和列标识符
            Delete delete = new Delete(Bytes.toBytes(rowKey)).addColumn(Bytes.toBytes(family),
                    Bytes.toBytes(qualifier));
            // 调用 HTable 的 delete() 方法执行删除操作
            table.delete(delete);
        } finally {
            // 关闭 HTable 资源
            table.close();
        }

    }

    /**
     * 删除指定rowkey上整个列族的数据
     * 
     * @param tableName
     * @param rowKey
     * @param family
     * @throws IOException
     */
    public void deleteColumnFamily(String tableName, String rowKey, String family) throws IOException {
        Connection conn = this.connHolder.get();
        Table table = conn.getTable(TableName.valueOf(tableName));
        try {
            // 创建 Delete 对象，并指定要删除的行键和列族
            Delete delete = new Delete(Bytes.toBytes(rowKey)).addFamily(Bytes.toBytes(family));
            // 调用 HTable 的 delete() 方法执行删除操作
            table.delete(delete);
        } finally {
            // 关闭 HTable 资源
            table.close();
        }
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
        // 根据表名获取 HTable 对象
        Connection conn = this.connHolder.get();
        Table table = conn.getTable(TableName.valueOf(tableName));
        try {
            // 创建 Get 对象，并指定要获取的行键
            Get get = new Get(Bytes.toBytes(rowKey));
            // 调用 HTable 的 get() 方法获取该行数据
            Result result = table.get(get);
            return result;
        } finally {
            // 关闭 HTable 资源
            table.close();
        }
    }

    /**
     * 扫描表，具体的filter如何创建和自定义可以查找资料，因为这里查询需要自定义，很难写一个统一函数出来，所以在这我就不实现了
     *
     * @param tableName 表名
     * @param filter    过滤器
     * @param pageSize  每页记录数
     * @return ResultScanner 对象
     * @throws IOException
     */
    public ResultScanner scanTable(String tableName, Filter filter, int pageSize) throws IOException {
        Connection conn = this.connHolder.get();
        Table table = conn.getTable(TableName.valueOf(tableName));
        try {
            // 创建扫描器对象
            Scan scan = new Scan();
            scan.setFilter(filter);
            scan.setCaching(pageSize);
            ResultScanner resultScanner = table.getScanner(scan);
            return resultScanner;
        } finally {
            table.close();
        }
    }

    /**
     * 关闭连接
     *
     * @throws IOException
     */
    public void close() throws IOException {
        Connection conn = connHolder.get();
        if (conn != null) {
            conn.close();
            connHolder.remove();
        }
    }
    public static void main(String[] args) throws IOException {
        HBaseUtil hBaseUtil = new HBaseUtil("myhbase", "2181");
        hBaseUtil.putData("test_table", "cn.ac.ucas", "cf1", "major", "computer", false);
    }
}