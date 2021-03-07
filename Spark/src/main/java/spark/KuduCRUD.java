package spark;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

import java.util.LinkedList;
import java.util.List;

public class KuduCRUD {
    private static ColumnSchema newColumn(String name, Type type, boolean iskey) {
        ColumnSchema.ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name, type);
        column.key(iskey);
        return column.build();
    }

    public static void main(String[] args) throws KuduException {
        createTable();
    }

    private static void createTable() throws KuduException {
        KuduClient kuduClient =
                new AsyncKuduClient.AsyncKuduClientBuilder("node1:7051").build().syncClient();
        kuduClient.deleteTable("test.PERSON");
        // 设置表的schema
        List<ColumnSchema> columns = new LinkedList<ColumnSchema>();
        columns.add(newColumn("CompanyId", Type.INT32, true));
        columns.add(newColumn("WorkId", Type.INT32, false));
        columns.add(newColumn("Name", Type.STRING, false));
        columns.add(newColumn("Gender", Type.STRING, false));
        columns.add(newColumn("Photo", Type.STRING, false));
        Schema schema = new Schema(columns);
        //创建表时提供的所有选项
        CreateTableOptions options = new CreateTableOptions();
        // 设置表的replica备份和分区规则
//        List<String> parcols = new LinkedList<String>();
//        parcols.add("CompanyId");
//
//        //设置表的备份数
//        options.setNumReplicas(1);
//        //设置range分区
//        options.setRangePartitionColumns(parcols);
//        //设置hash分区和数量
//        options.addHashPartitions(parcols, 3);
        try {
            //kuduClient.deleteTable("PERSON");
            kuduClient.createTable("test.PERSON", schema, options);
            kuduClient.close();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }
}
