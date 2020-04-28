package it.unipi.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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

    public static class MatrixMapper extends Mapper<Object, Text, Text, List<Text>> {

        private int i;
        private int j;
        private int k;
        
        private final Map<Text, List<Text>> output = new HashMap<Text, List<Text>>();

        private final Text key = new Text();
        private final List<Text> values = new ArrayList<Text>();

        public void setup(Context context) {
            this.i = context.getConfiguration().getInt("matrix.multiplication.i", 2);
            this.j = context.getConfiguration().getInt("matrix.multiplication.j", 2);
            this.k = context.getConfiguration().getInt("matrix.multiplication.k", 2);
        }

        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
            //(i,j,m_ij)
            //M, 0, 0, 10.0   = split   
            //N, 0, 0, 1.0

            String tuple = value.toString();
            String[] split = tuple.trim().split(",");
            
            if (split[0].equals("M")) {
                for (int col = 0; col<this.k ; col++) {
                    this.key.set(split[1] + ", " + col);
                    this.values.add(new Text(split[0] + ", " + split[2] + ", " + split[3]));  //M,j,m_ij
                    //  k = 3
                    //  input map
                    //  M, 0, 0, 10.0
                    //  output for
                    //  (0,0), (M,0,10.0) 
                    //  (0,1), (M,0,10.0)
                    //  (0,2), (M,0,10.0)
                }

            } else if (split[0].equals("N") {
                
            }          
            
            //for every element of M
            //produce (k,v)-> (i,k),(M,j,m_ij)  for k=0 k < columns_N


            //for every element of N
            //produce (k,v)-> (i,k),(N,j,n_jk) for i=0 i < romws_M


            //return 
            //set of (k,v)pairs->
            //key=(i,k) 
            //value=list with  (M,j,m_ij) e (N,j,n_jk) for all possible values of j
        }
    }
}