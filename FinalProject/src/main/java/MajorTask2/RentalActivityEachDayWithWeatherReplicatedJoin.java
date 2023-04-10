package MajorTask2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *Replicated join
 */
public class RentalActivityEachDayWithWeatherReplicatedJoin {
  private static final String HBASE_TABLE_NAME = "BikeData";

  public static class RentalActivityMapper extends Mapper<Object, Result, Text, IntWritable> {
    private HashMap<String, Integer> counter;
    private static HashMap<String, String> weatherMap;
    private Text date = new Text();
    private HTable weatherTable;

    public static HashMap<String, String> getWeatherMap() {
      return weatherMap;
    }

    @Override
    public void setup(Context context) throws IOException {
      counter = new HashMap<>();
      weatherMap = new HashMap<>();
      //load weather data to weatherMap
      Configuration conf = HBaseConfiguration.create();
      //for aws connection
      //String hbaseconf = "/etc/hbase/conf/hbase-site.xml";
      //conf.addResource(new File(hbaseconf).toURI().toURL());
      Connection conn = ConnectionFactory.createConnection(conf);
      weatherTable = (HTable) conn.getTable(TableName.valueOf("WeatherData"));
      Scan scanWeather = new Scan();
      ResultScanner resultScannerWeather = weatherTable.getScanner(scanWeather);
      for(Result weather : resultScannerWeather){
        String valueDate = new String(weather.getValue("Weather".getBytes(), "date".getBytes()), StandardCharsets.UTF_8);
        String precipitation = new String(weather.getValue("Weather".getBytes(), "precipitation".getBytes()), StandardCharsets.UTF_8);
        String maxTemperature = new String(weather.getValue("Weather".getBytes(), "maxTemperature".getBytes()), StandardCharsets.UTF_8);
        String minTemperature = new String(weather.getValue("Weather".getBytes(), "minTemperature".getBytes()), StandardCharsets.UTF_8);
        weatherMap.put(valueDate, String.join(",", precipitation, maxTemperature, minTemperature));
      }
    }
    @Override
    public void map(Object key, Result value, Context context){
      byte[] s = value.getValue("Bike".getBytes(), "startTime".getBytes());
      String str = new String(s, StandardCharsets.UTF_8);
      StringTokenizer itr = new StringTokenizer(str);
      while(itr.hasMoreTokens()){
        String token = itr.nextToken().toLowerCase();
        token = token.split("#")[0];
        counter.put(token, counter.getOrDefault(token, 0) + 1);
      }
    }

    public void cleanup(Context context
    ) throws IOException, InterruptedException {
      // called at end of the task
      for(String token : counter.keySet()) {
        date.set(token);
        context.write(date, new IntWritable(counter.get(token)));
      }
    }
  }
  //join with weather table
  public static class RentalActivityReducer extends Reducer<Text,IntWritable,Text,Text> {
    private HashMap<String, String> weatherMap;
    @Override
    public void setup(Context context) throws IOException {
      weatherMap = RentalActivityMapper.getWeatherMap();
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
        Context context
    ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      String[] info = weatherMap.get(key.toString()).split(",");

      String out = String.join("\t", String.valueOf(sum), info[0], info[1], info[2]);
      context.write(key, new Text(out));
    }
  }

  public static void main(String[] args)
      throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = HBaseConfiguration.create();
    //for aws connection
    //String hbaseconf = "/etc/hbase/conf/hbase-site.xml";
    //conf.addResource(new File(hbaseconf).toURI().toURL());
    Connection conn = ConnectionFactory.createConnection(conf);
    String[] otherArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();
    if (otherArgs.length != 1) {
      System.err.println("Usage: COMPUTE <out-file-path>");
      System.exit(2);
    }
    Scan scan = new Scan();
    scan.setCaching(500);
    scan.setCacheBlocks(false);
    Job job = new Job(conf, "Rental Activity");
    job.setInputFormatClass(TableInputFormat.class);
    job.getConfiguration().set(TableInputFormat.INPUT_TABLE, HBASE_TABLE_NAME);
    job.getConfiguration().set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan));
    job.setJarByClass(RentalActivityEachDayWithWeatherReplicatedJoin.class);
    job.setMapperClass(RentalActivityMapper.class);
    job.setReducerClass(RentalActivityReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
