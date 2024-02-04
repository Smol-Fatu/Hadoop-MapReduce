import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CoAuthorshipGraph {
    public static class CoAuthorshipMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text author = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Skip the first line as it contains column names
            if (!value.toString().contains("Title")) {
                // Extract the authors' names from the input line enclosed in brackets []
                String authors = value.toString().replaceAll("^.*\\[|\\].*", "");
                StringTokenizer tokenizer = new StringTokenizer(authors, ",");
                List<String> authorList = new ArrayList<String>();
                while (tokenizer.hasMoreTokens()) {
                    authorList.add(tokenizer.nextToken().trim());
                }
                // Generate pairs of authors
                for (int i = 0; i < authorList.size() - 1; i++) {
                    for (int j = i + 1; j < authorList.size(); j++) {
                        String authorPair = authorList.get(i) + " & " + authorList.get(j);
                        author.set(authorPair);
                        context.write(author, one);
                    }
                }
            }
        }
    }
    
    public static class CoAuthorshipReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Co-Authorship Graph");
        job.setJarByClass(CoAuthorshipGraph.class);
        job.setMapperClass(CoAuthorshipMapper.class);
        job.setCombinerClass(CoAuthorshipReducer.class);
        job.setReducerClass(CoAuthorshipReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

