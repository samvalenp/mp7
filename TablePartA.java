import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;

public class TablePartA{

   public static void main(String[] args) throws IOException {

	// Instantiating configuration class
    Configuration con = HBaseConfiguration.create();

    // Instantiating HbaseAdmin class
    HBaseAdmin admin = new HBaseAdmin(con);

    // Instantiating table descriptor class
    HTableDescriptor table1 = new HTableDescriptor(TableName.valueOf("powers"));
    // Adding column families to table descriptor
    table1.addFamily(new HColumnDescriptor("personal"));
    table1.addFamily(new HColumnDescriptor("professional"));
    table1.addFamily(new HColumnDescriptor("custom"));
    // Execute the table through admin
    admin.createTable(table1);

    // Instantiating table descriptor class
    HTableDescriptor table2 = new HTableDescriptor(TableName.valueOf("food"));
    // Adding column families to table descriptor
    table2.addFamily(new HColumnDescriptor("nutrition"));
    table2.addFamily(new HColumnDescriptor("taste"));
    // Execute the table through admin
    admin.createTable(table2);
   }
}

