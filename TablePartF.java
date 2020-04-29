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
import java.util.*;

public class TablePartF{

   public static void main(String[] args) throws IOException {

	// Instantiating Configuration class
    Configuration config = HBaseConfiguration.create();

    Connection connection = ConnectionFactory.createConnection(config);
    // Instantiating HTable class
    Table table = connection.getTable(TableName.valueOf("powers"));

    Scan scan = new Scan();   

    scan.addColumn(Bytes.toBytes("custom"), Bytes.toBytes("color"));
    scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("power"));
    scan.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("name"));

	// DON' CHANGE THE 'System.out.println(xxx)' OUTPUT PART
	// OR YOU WON'T RECEIVE POINTS FROM THE GRADER
	ResultScanner scanner1 = table.getScanner(scan);

	List<List<String>> list = new ArrayList<List<String>>();

	for (Result result1 = scanner1.next(); result1 != null; result1 = scanner1.next()){
		List<String> aux = new ArrayList<String>();

		String col = new String(result1.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color")));
		String nam = new String(result1.getValue(Bytes.toBytes("professional"),Bytes.toBytes("name")));
		String pow = new String(result1.getValue(Bytes.toBytes("personal"),Bytes.toBytes("power")));

		aux.add(col);
		aux.add(nam);
		aux.add(pow);
		list.add(aux);
	}


	for(int i = 0; i < list.size(); i++){
		for(int j = 0; j < list.size(); j++){
			if(list.get(i).get(0).equals(list.get(j).get(0)) && !list.get(i).get(1).equals(list.get(j).get(1))){
				System.out.println(list.get(i).get(1) + ", " + list.get(i).get(2) + ", " + list.get(j).get(1) + ", " + list.get(j).get(2) + ", "+list.get(i).get(0));
			}
		}
	}

	scanner1.close();

   }
}
