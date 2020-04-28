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
import org.apache.hadoop.hbase.client.Get;


import org.apache.hadoop.hbase.util.Bytes;


import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

public class TablePartD{

   public static void main(String[] args) throws IOException {

   	// Instantiating Configuration class
    Configuration config = HBaseConfiguration.create();

    Connection connection = ConnectionFactory.createConnection(config);
    // Instantiating HTable class
    Table table = connection.getTable(TableName.valueOf("powers"));

    Get getr1 = new Get(Bytes.toBytes("row1"));
    Result r1 = table.get(getr1);

    Get getr19 = new Get(Bytes.toBytes("row19"));
    Result r19 = table.get(getr1);

	// DON' CHANGE THE 'System.out.println(xxx)' OUTPUT PART
	// OR YOU WON'T RECEIVE POINTS FROM THE GRADER 
	
	String hero = r1.getValue(Bytes.toBytes("personal"),Bytes.toBytes("hero"));
	String power = r1.getValue(Bytes.toBytes("personal"),Bytes.toBytes("power"));
	String name = r1.getValue(Bytes.toBytes("professional"),Bytes.toBytes("name"));
	String xp = r1.getValue(Bytes.toBytes("professional"),Bytes.toBytes("xp"));
	String color = r1.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color"));
	System.out.println("hero: "+hero+", power: "+power+", name: "+name+", xp: "+xp+", color: "+color);

	hero = r19.getValue(Bytes.toBytes("personal"),Bytes.toBytes("hero"));
	color = r19.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color"));
	System.out.println("hero: "+hero+", color: "+color);

	hero = r1.getValue(Bytes.toBytes("personal"),Bytes.toBytes("hero"));
	name = r1.getValue(Bytes.toBytes("professional"),Bytes.toBytes("name"));
	color = r1.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color"));
	System.out.println("hero: "+hero+", name: "+name+", color: "+color); 
   }
}

