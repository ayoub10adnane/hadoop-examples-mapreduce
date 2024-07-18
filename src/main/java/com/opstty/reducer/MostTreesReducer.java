package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MostTreesReducer extends Reducer<NullWritable, Text, Text, IntWritable> {
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String district = "";
        int maxTrees = 0;

        for (Text val : values) {
            String[] fields = val.toString().split("\t");
            String currentDistrict = fields[0];
            int treeCount = Integer.parseInt(fields[1]);

            if (treeCount > maxTrees) {
                maxTrees = treeCount;
                district = currentDistrict;
            }
        }
        context.write(new Text(district), new IntWritable(maxTrees));
    }
}
