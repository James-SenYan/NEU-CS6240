package MajorTask2;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Where join happens
 */
public class Hcompute {


  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    //for aws connection
    //String hbaseconf = "/etc/hbase/conf/hbase-site.xml";
    //conf.addResource(new File(hbaseconf).toURI().toURL());
    Connection conn = ConnectionFactory.createConnection(conf);
    /*String[] otherArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();
    if (otherArgs.length != 1) {
      System.err.println("Usage: H-COMPUTE <out-file-path>");
      System.exit(2);
    }*/
    HTable bikeTable = (HTable) conn.getTable(TableName.valueOf("BikeData"));
    HTable weatherTable = (HTable) conn.getTable(TableName.valueOf("WeatherData"));
    Scan scanBike = new Scan();
    Scan scanWeather = new Scan();
    ResultScanner resultScannerBike = bikeTable.getScanner(scanBike);
    ResultScanner resultScannerWeather = weatherTable.getScanner(scanWeather);
    System.out.println("first record in bike: "+resultScannerBike.next());
    for(Result bike : resultScannerBike){
      byte[] rowKey = bike.getRow();
      String valueBikeId = Arrays.toString(bike.getValue("Bike".getBytes(), "bikeId".getBytes()));
      String valueUserType = Arrays.toString(bike.getValue("Bike".getBytes(), "userType".getBytes()));
      String valueBirthYear = Arrays.toString(bike.getValue("Bike".getBytes(), "birthYear".getBytes()));
      String valueGender = Arrays.toString(bike.getValue("Bike".getBytes(), "gender".getBytes()));
      Get get = new Get(rowKey);
      Result weathers = weatherTable.get(get);
      if(!weathers.isEmpty()){
        String valueDate = Arrays.toString(weathers.getValue("Weather".getBytes(), "date".getBytes()));
        System.out.println(valueBikeId + " " + valueUserType + " " + valueBirthYear+" "+valueGender+" " + valueDate);
      }
    }
    bikeTable.close();
    weatherTable.close();
  }
}
