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

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

public class TablePartF{

   public static void main(String[] args) throws IOException {

	// Instantiating Configuration class
    Configuration config = HBaseConfiguration.create();

    Connection connection = ConnectionFactory.createConnection(config);
    // Instantiating HTable class
    Table table = connection.getTable(TableName.valueOf("powers"));
    Table table1 = connection.getTable(TableName.valueOf("powers"));

    Scan scan = new Scan();   

    scan.addColumn(Bytes.toBytes("custom"), Bytes.toBytes("color"));
    scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("power"));
    scan.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("name"));

    Scan scan2 = new Scan();   

    scan2.addColumn(Bytes.toBytes("custom"), Bytes.toBytes("color"));
    scan2.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("power"));
    scan2.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("name"));

	// DON' CHANGE THE 'System.out.println(xxx)' OUTPUT PART
	// OR YOU WON'T RECEIVE POINTS FROM THE GRADER
	ResultScanner scanner1 = table.getScanner(scan);
	ResultScanner scanner2 = table1.getScanner(scan2);

	for (Result result1 = scanner1.next(); result1 != null; result1 = scanner1.next()){
		for(Result result2 = scanner2.next(); result2 != null; result2 = scanner2.next()){

			String c1 = new String(result1.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color")));
			String c2 = new String(result2.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color")));
			String name1 = new String(result1.getValue(Bytes.toBytes("professional"),Bytes.toBytes("name")));
			String name2 = new String(result2.getValue(Bytes.toBytes("professional"),Bytes.toBytes("name")));

			if(c1.equals(c2) && !name1.equals(name2)){
				String power1 = new String(result1.getValue(Bytes.toBytes("personal"),Bytes.toBytes("power")));
				String power2 = new String(result2.getValue(Bytes.toBytes("personal"),Bytes.toBytes("power")));
				System.out.println(name1 + ", " + power1 + ", " + name2 + ", " + power2 + ", "+c1);
			}
		}
	}

	scanner1.close();
	scanner2.close();

   }
}
