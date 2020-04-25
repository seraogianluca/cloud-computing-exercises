package it.unipi.hadoop;

import java.io.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class TimeSeriesData implements Writable, Comparable<TimeSeriesData> {
    private long timestamp;
    private double average;

    public static TimeSeriesData copy(final TimeSeriesData tsd) {
        return new TimeSeriesData(tsd.timestamp, tsd.average);
    }

    public TimeSeriesData() {}

    public TimeSeriesData(final long t, final double a) {
        this.set(t,a);
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public double getAverage() {
        return this.average;
    }

    public void set(final long t, final double a) {
        this.timestamp = t;
        this.average = a;
    }

    public void readFields(final DataInput in) throws IOException {
        this.timestamp = in.readLong();
        this.average = in.readDouble();
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeDouble(average);
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof TimeSeriesData))
            return false;

        final TimeSeriesData other = (TimeSeriesData) o;
        if (this.timestamp == other.timestamp && this.average == other.average)
            return true;

        return false;
    }

    @Override
    public int hashCode() {
        final LongWritable ts = new LongWritable(timestamp);
        final DoubleWritable a = new DoubleWritable(average);

        return ts.hashCode() + a.hashCode();
    }

    @Override
    public int compareTo(final TimeSeriesData o) {
        final long thisTime = this.timestamp;
        final long thatTime = o.timestamp;
        return (thisTime < thatTime ? -1 : (thisTime == thatTime ? 0 : 1));
    }

    @Override
    public String toString() {
        return Long.toString(timestamp) + ", " + Double.toString(average);
    }
}