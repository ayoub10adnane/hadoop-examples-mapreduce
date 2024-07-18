package com.opstty.mapper;

import com.opstty.writable.AgeDistrictWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeMapper extends Mapper<LongWritable, Text, IntWritable, AgeDistrictWritable> {
    private boolean isFirstRow = true;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (isFirstRow) {
            isFirstRow = false;
            return; // Skip the first row (header)
        }

        String[] fields = value.toString().split(";");
        if (fields.length > 5) {
            try {
                int age = Integer.parseInt(fields[5]);
                String district = fields[1];
                context.write(new IntWritable(age), new AgeDistrictWritable(new IntWritable(age), new Text(district)));
            } catch (NumberFormatException e) {
                // Skip rows with invalid age value
            }
        }
    }
}
