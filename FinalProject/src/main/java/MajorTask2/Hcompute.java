package MajorTask2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
    System.out.println("BikeId\tUserType\tBirthYear\tGender\tDate\tprecipitation\tmaxTemp\tminTemp");
    for(Result bike : resultScannerBike){
      String rowKey = new String(bike.getRow(), StandardCharsets.UTF_8).trim();
      byte[] s1 = bike.getValue("Bike".getBytes(), "bikeID".getBytes());
      String valueBikeId = new String(s1, StandardCharsets.UTF_8);
      byte[] s2 = bike.getValue("Bike".getBytes(), "userType".getBytes());
      String valueUserType = new String(s2, StandardCharsets.UTF_8);
      byte[] s3 = bike.getValue("Bike".getBytes(), "birthYear".getBytes());
      String valueBirthYear = new String(s3, StandardCharsets.UTF_8);
      byte[] s4 = bike.getValue("Bike".getBytes(), "gender".getBytes());
      String valueGender = new String(s4, StandardCharsets.UTF_8);
      Get get = new Get(rowKey.split("#")[0].getBytes());
      Result weathers = weatherTable.get(get);
      if(!weathers.isEmpty()){
        String valueDate = new String(weathers.getValue("Weather".getBytes(), "date".getBytes()), StandardCharsets.UTF_8);
        String precipitation = new String(weathers.getValue("Weather".getBytes(), "precipitation".getBytes()), StandardCharsets.UTF_8);
        String maxTemperature = new String(weathers.getValue("Weather".getBytes(), "maxTemperature".getBytes()), StandardCharsets.UTF_8);
        String minTemperature = new String(weathers.getValue("Weather".getBytes(), "minTemperature".getBytes()), StandardCharsets.UTF_8);
        System.out.println(valueBikeId+"\t"+valueUserType+"\t"+valueBirthYear+"\t"
            +valueGender+"\t"+valueDate+"\t"+precipitation+"\t"+maxTemperature+"\t"+minTemperature);
      }
    }
    bikeTable.close();
    weatherTable.close();
  }
}
