package it.unipi.hadoop;

import java.security.Timestamp;
import java.io.*;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class TimeSeriesData implements WritableComparable {
    private long timestamp;
    private double average;

    public TimeSeriesData() {}

    public TimeSeriesData(long t, double a) {
        setTimestamp(t);
        setAverage(a);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getAverage() {
        return average;
    }

    public void setTimestamp(long t) {
        timestamp = t;
    }

    public void setAverage(double a) {
        average = a;
    }

    public void readFields(DataInput in) throws IOException {
        timestamp = in.readLong();
        average = in.readDouble();
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeDouble(average);
    }

    public boolean equals(Object o) {
        if(!(o instanceof TimeSeriesData))
            return false;

        TimeSeriesData other = (TimeSeriesData)o;
        if(this.timestamp == other.timestamp && this.average == other.average)
            return true;
        
        return false;
    }

    public int hashCode() {
        LongWritable ts = new LongWritable(timestamp);
        DoubleWritable a = new DoubleWritable(average);

        return ts.hashCode() + a.hashCode();
    }

    public int compareTo(Object o) {
        long thisTime = this.timestamp;
        long thatTime = ((TimeSeriesData)o).timestamp;
        return (thisTime<thatTime ? -1 : (thisTime==thatTime ? 0 : 1));
    }

    public String toString() {
        return Long.toString(timestamp) + ", " + Double.toString(average);
    }
}