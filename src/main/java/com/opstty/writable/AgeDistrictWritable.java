package com.opstty.writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AgeDistrictWritable implements Writable {
    private IntWritable age;
    private Text district;

    public AgeDistrictWritable() {
        this.age = new IntWritable();
        this.district = new Text();
    }

    public AgeDistrictWritable(IntWritable age, Text district) {
        this.age = age;
        this.district = district;
    }

    public IntWritable getAge() {
        return age;
    }

    public Text getDistrict() {
        return district;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        age.write(out);
        district.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        age.readFields(in);
        district.readFields(in);
    }
}
