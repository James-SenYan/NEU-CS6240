package MajorTask2;

import com.opencsv.CSVParser;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 */
public class HpopulateWeather {
  public static final String HBASE_TABLE_NAME = "WeatherData";
  public static final String COLUMN_FAMILY = "Weather";
  public static final String COLUMN_DATE = "date";
  public static final String COLUMN_PRCP = "precipitation";//in inches
  public static final String COLUMN_SNOWFALL = "snowFall";//in inches
  public static final String COLUMN_SNWDEPTH = "snowDepth";//in inches
  public static final String COLUMN_TMIN = "minTemperature";//in Fahrenheit
  public static final String COLUMN_TMAX = "maxTemperature";//in Fahrenheit


  public static void main(String[] args)
      throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();
    // Arguments length check
    if (otherArgs.length != 1) {
      System.err.println("Usage: HPopulate <weather_input-file-path>");
      System.exit(2);
    }
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();
    // Check if the table already exists
    if (admin.tableExists(TableName.valueOf(HBASE_TABLE_NAME))) {
      admin.disableTable(TableName.valueOf(HBASE_TABLE_NAME));
      admin.deleteTable(TableName.valueOf(HBASE_TABLE_NAME));
      System.out.println("HTable " + HBASE_TABLE_NAME
          + " already exists! Deleting...");
    }
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(HBASE_TABLE_NAME));
    HColumnDescriptor hcd = new HColumnDescriptor(COLUMN_FAMILY);
    htd.addFamily(hcd);
    admin.createTable(htd);
    System.out.println("HBase table " + HBASE_TABLE_NAME
        + " created.");
    Job job = new Job(conf, "HBase table populate");
    job.setJarByClass(HpopulateWeather.class);
    job.setMapperClass(HPopulateMapper.class);
    job.setOutputFormatClass(TableOutputFormat.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Put.class);
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, HBASE_TABLE_NAME);
    job.setNumReduceTasks(0);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    admin.close();
  }

  //mapper class
  public static class HPopulateMapper extends
      Mapper<Object, Text, ImmutableBytesWritable, Writable> {
    private CSVParser csvParser = null;
    private HTable table = null;

    protected void setup(Context context) throws IOException {
      this.csvParser = new CSVParser();
      Configuration config = HBaseConfiguration.create();
      Connection connection = ConnectionFactory.createConnection(config);
      table = (HTable) connection.getTable(TableName.valueOf(HBASE_TABLE_NAME));
    }

    public void map(Object offset, Text value, Context context)
        throws IOException {
      // Parse the input line
      String[] line = this.csvParser.parseLine(value.toString());
      for(String item : line){
        if(item == null || item.length() == 0) return;
      }
      StringBuilder rowKey = new StringBuilder();
      String dateStr = line[0];//in seconds
      String precipitation = line[1];
      String snowFall = line[2];
      String snowDepth = line[3];
      String minTemperature = line[4];
      String maxTemperature = line[5];
      LocalDate date = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
      rowKey.append(date.toString());
      Put put = new Put(Bytes.toBytes(rowKey.toString()));
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_DATE.getBytes(), dateStr.getBytes());
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_PRCP.getBytes(),
          precipitation.getBytes());
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_SNOWFALL.getBytes(), snowFall.getBytes());
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_SNWDEPTH.getBytes(), snowDepth.getBytes());
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_TMIN.getBytes(), minTemperature.getBytes());
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_TMAX.getBytes(), maxTemperature.getBytes());
      table.put(put);
    }

    protected void cleanup(Context context) throws IOException {
      table.close();
    }
  }
}
