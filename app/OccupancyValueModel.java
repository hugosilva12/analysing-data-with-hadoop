package tead;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OccupancyValueModel implements Writable {

    private IntWritable totalNights;
    private IntWritable totalRooms;

    public OccupancyValueModel() {
        this.totalNights = new IntWritable();
        this.totalRooms = new IntWritable();
    }

    public OccupancyValueModel(IntWritable totalNights, IntWritable totalRooms) {
        this.totalNights = totalNights;
        this.totalRooms = totalRooms;
    }

    public void set(OccupancyValueModel occupancyValueModel) {
        this.totalNights.set(occupancyValueModel.getTotalNights().get());
        this.totalRooms.set(occupancyValueModel.getTotalRooms().get());
    }

    public IntWritable getTotalNights() {
        return totalNights;
    }

    public IntWritable getTotalRooms() {
        return totalRooms;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        totalNights.readFields(in);
        totalRooms.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        totalNights.write(out);
        totalRooms.write(out);
    }

    public void setTotalNights(IntWritable totalNights) {
        this.totalNights = totalNights;
    }

    public void setTotalRooms(IntWritable totalRooms) {
        this.totalRooms = totalRooms;
    }

    @Override
    public String toString() {
        return "OccupancyValueModel{" +
                "totalNights=" + totalNights +
                ", totalRooms=" + totalRooms +
                '}';
    }
}