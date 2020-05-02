package it.unipi.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MatrixMultiplication {

    public static class MatrixMapper extends Mapper<Object, Text, Text, Text> {

        private int i;
        private int k;

        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        public void setup(Context context) {
            this.i = context.getConfiguration().getInt("matrix.multiplication.i", 2);
            this.k = context.getConfiguration().getInt("matrix.multiplication.k", 2);
        }

        public void map(Object key, Text value, Context context)
          throws IOException, InterruptedException {
/*          
**          Input example:
**          (i,j,m_ij)
*/
                String tuple = value.toString();
                String[] split = tuple.trim().split(",");

                int limit = 0;
                if (split[0].equals("M")) {
                    limit = this.k;
                } else if (split[0].equals("N")) {
                    limit = this.i;
                }

                for (int kappa = 0; kappa < limit; kappa++) {
                    // k = 3
                    // input map
                    // M, 0, 0, 10.0
                    // output for
                    // final (0,0), (M,0,10.0)
                    // (0,1), (M,0,10.0)
                    // (0,2), (M,0,10.0)

                    if (split[0].equals("M")) {
                        outputKey.set(split[1] + ", " + kappa);
                    } else if (split[0].equals("N")) {
                        outputKey.set(kappa + ", " + split[1]);
                    }
                    
                    outputValue.set(split[0] + ", " + split[2] + ", " + split[3]); // M,j,m_ij
                    context.write(outputKey, outputValue);
                }
        }
    }

    public static class MatrixReducer extends Reducer<Text, Text, Text, Text>  {

        private int j;

        public void setup(Context context) {
            this.j = context.getConfiguration().getInt("matrix.multiplication.j", 2);
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
          throws IOException, InterruptedException {
              //k= (i,k),  v= [(M,j,m_ij), ...., (N,j,n_jk)]
              //map key= j, value= m_ij
            Map<Integer, Float> mapM = new HashMap<Integer, Float>();
            Map<Integer, Float> mapN = new HashMap<Integer, Float>();

            while(values.iterator().hasNext()){
                String elem = values.iterator().next().toString();
                String[] split = elem.trim().split(",");

                if(split[0].equals("M")) {
                    mapM.put(Integer.parseInt(split[1].trim()), Float.parseFloat(split[2].trim()));
                } else if(split[0].equals("N")) {
                    mapN.put(Integer.parseInt(split[1].trim()), Float.parseFloat(split[2].trim()));
                }
            }

            float sum = 0.0f;
            for (int i = 0; i < this.j; i++) {
                float m_ij = mapM.containsKey(i) ? mapM.get(i) : 0.0f;
                float n_jk = mapN.containsKey(i) ? mapN.get(i) : 0.0f;

                sum += m_ij*n_jk;
            }

            context.write(key, new Text(Double.toString(sum)));
            /* if (result != 0.0f)
            **    context.write(null, new Text(key.toString() + "," + Float.toString(result))); */
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 6){
            System.err.println("Usage: matrixmultiplication <i> <j> <k> <in> <in> <out>");
            System.exit(1);
        }

        Job job = Job.getInstance(conf, "matrix multiplication");
        job.setJarByClass(MatrixMultiplication.class);

        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().setInt("matrix.multiplication.i", Integer.parseInt(otherArgs[0]));
        job.getConfiguration().setInt("matrix.multiplication.j", Integer.parseInt(otherArgs[1]));
        job.getConfiguration().setInt("matrix.multiplication.k", Integer.parseInt(otherArgs[2]));
        
        for (int i = 3; i < otherArgs.length - 1; ++i) { //per leggere tutti i file di input (dal terzo parametro in poi)
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        } 
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}