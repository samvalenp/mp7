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
    Result r19 = table.get(getr19);

	// DON' CHANGE THE 'System.out.println(xxx)' OUTPUT PART
	// OR YOU WON'T RECEIVE POINTS FROM THE GRADER 
	
	String hero = new String(r1.getValue(Bytes.toBytes("personal"),Bytes.toBytes("hero")));
	String power = new String(r1.getValue(Bytes.toBytes("personal"),Bytes.toBytes("power")));
	String name = new String(r1.getValue(Bytes.toBytes("professional"),Bytes.toBytes("name")));
	String xp = new String(r1.getValue(Bytes.toBytes("professional"),Bytes.toBytes("xp")));
	String color = new String(r1.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color")));
	System.out.println("hero: "+hero+", power: "+power+", name: "+name+", xp: "+xp+", color: "+color);

	hero = new String(r19.getValue(Bytes.toBytes("personal"),Bytes.toBytes("hero")));
	color = new String(r19.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color")));
	System.out.println("hero: "+hero+", color: "+color);

	hero = new String(r1.getValue(Bytes.toBytes("personal"),Bytes.toBytes("hero")));
	name = new String(r1.getValue(Bytes.toBytes("professional"),Bytes.toBytes("name")));
	color = new String(r1.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color")));
	System.out.println("hero: "+hero+", name: "+name+", color: "+color); 
   }
}

