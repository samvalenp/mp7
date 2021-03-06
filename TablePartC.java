import java.io.IOException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.BufferedReader;

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
import java.io.File;

import org.apache.hadoop.hbase.client.Table;
import java.util.Scanner;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class TablePartC{

   public static void main(String[] args) throws IOException {

   	Configuration config = HBaseConfiguration.create();

   	Connection connection = ConnectionFactory.createConnection(config);
    // Instantiating HTable class
    Table table = connection.getTable(TableName.valueOf("powers"));


	Scanner sc = new Scanner(new File("input.csv"));
    while (sc.hasNextLine()) {
        String line = sc.nextLine();
        String[] splitt = line.split(",");
        Put p = new Put(Bytes.toBytes(splitt[0]));

        p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes(splitt[1]));
        p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("power"), Bytes.toBytes(splitt[2]));

        p.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes(splitt[3]));
        p.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes(splitt[4]));

        p.addColumn(Bytes.toBytes("custom"), Bytes.toBytes("color"), Bytes.toBytes(splitt[5]));

        table.put(p);
    }    

    table.close();
   }
}

