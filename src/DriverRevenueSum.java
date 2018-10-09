import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DriverRevenueSum extends Configured implements Tool {

  static int printUsage() {
    System.out.println("wordcount [-m <maps>] [-r <reduces>] <input> <output>");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  public static class DriverRevenueMapper
       extends Mapper<Object, Text, Text, DoubleWritable> {

    // so we don't have to do reallocations
    private Text word = new Text();

    // to chec for only alphanumeric
    /* Order of entries in text file should be:
    * medallion, hack_license, vendor_id, pickup_datetime, payment, fare_amount,
    * surcharge, mta_tax, tip_amount, tolls_amount, total_amount
    * */

    String license_medallionRegex = "[A-Z|\\d]{32}";
    String DTRegex = "(?=\\d)(?:(?:(?:(?:(?:0?[13578]|1[02])(\\/|-|\\.)31)\\1|(?:(?:0?[1,3-9]|1[0-2])(\\/|-|\\.)(?:29|30)\\2))(?:(?:1[6-9]|[2-9]\\d)?\\d{2})|(?:0?2(\\/|-|\\.)29\\3(?:(?:(?:1[6-9]|[2-9]\\d)?(?:0[48]|[2468][048]|[13579][26])|(?:(?:16|[2468][048]|[3579][26])00))))|(?:(?:0?[1-9])|(?:1[0-2]))(\\/|-|\\.)(?:0?[1-9]|1\\d|2[0-8])\\4(?:(?:1[6-9]|[2-9]\\d)?\\d{2}))($|\\ (?=\\d)))?(((0?[1-9]|1[012])(:[0-5]\\d){0,2}(\\ [AP]M))|([01]\\d|2[0-3])(:[0-5]\\d){1,2})?([0-9]|0[0-9]|1[0-9]|2[0-3]):[0-5][0-9]";
    String singleDollarRegex = "^\\$[0-9]+(\\.[0-9][0-9])?";

//    String sixDollarRegex = "(,([0-9]+(\\.[0-9][0-9])?)){6}";
// regex for capturing exactly 6 dollar amounts with commas as we expect in the input file

    String sixDecimalRegex = "(,\\d*\\.?\\d*){6}";
    // TODO: this regex isn't completely right but I just couldn't get the above one to work

    String vendorID_paymentRegex = "[A-Z]{3}";

    String expression = license_medallionRegex + "," + license_medallionRegex + "," + vendorID_paymentRegex
        + DTRegex + "," + vendorID_paymentRegex + sixDecimalRegex + "$";

    Pattern pattern = Pattern.compile(expression);

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {

        // check for all alphabetical
        String nextToken = itr.nextToken ();
        Matcher matcher = pattern.matcher(nextToken);

        // if not, don't write
        if (!matcher.matches ())
          continue;


        String[] columns = nextToken.split(",");

        // parse out the amount
        double amt = Double.parseDouble(columns[10]);
        // amount is in the 11th column

        DoubleWritable totalAmt = new DoubleWritable(amt);

        // parse out the license
        String license = columns[1];

        word.set(license);
        context.write(word, totalAmt);
      }
    }
  }

  public static class DriverRevenueReducer
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      MinMaxPriority
      double sum = 0;
      for (DoubleWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }


  public int run(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(DriverRevenueSum.class);
    job.setMapperClass(DriverRevenueMapper.class);
    job.setCombinerClass(DriverRevenueReducer.class);
    job.setReducerClass(DriverRevenueReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    
    List<String> other_args = new ArrayList<String>();
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-r".equals(args[i])) {
          job.setNumReduceTasks(Integer.parseInt(args[++i]));
        } else {
          other_args.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
                           args[i-1]);
        return printUsage();
      }
    }
    // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      System.out.println("ERROR: Wrong number of parameters: " +
                         other_args.size() + " instead of 2.");
      return printUsage();
    }
    FileInputFormat.setInputPaths(job, other_args.get(0));
    FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
    return (job.waitForCompletion(true) ? 0 : 1);
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new DriverRevenueSum(), args);
    System.exit(res);
  }

}
