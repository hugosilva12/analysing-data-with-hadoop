package tead;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OccupancyKeyModel implements WritableComparable<OccupancyKeyModel> {

    private Text hotelCity;
    private IntWritable year;
    private IntWritable month;
    private Text hotelID;

    public OccupancyKeyModel() {
        this.hotelCity = new Text();
        this.hotelID = new Text();
        this.year = new IntWritable();
        this.month = new IntWritable();
    }

    public OccupancyKeyModel(Text hotelCity, Text hotelID, IntWritable year, IntWritable month) {
        this.hotelCity = hotelCity;
        this.year = year;
        this.month = month;
        this.hotelID = hotelID;
    }


    public void set(OccupancyKeyModel occupancyKeyModel) {
        this.hotelCity.set(occupancyKeyModel.getHotelCity().toString());
        this.hotelID.set(occupancyKeyModel.getHotelID().toString());
        this.year.set(occupancyKeyModel.getYear().get());
        this.month.set(occupancyKeyModel.getMonth().get());
    }



    @Override
    public void write(DataOutput out) throws IOException {
        hotelCity.write(out);
        year.write(out);
        month.write(out);
        hotelID.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        hotelCity.readFields(in);
        year.readFields(in);
        month.readFields(in);
        hotelID.readFields(in);
    }

    @Override
    public int compareTo(OccupancyKeyModel o) {
        int cmp = hotelCity.compareTo(o.hotelCity);
        if (cmp != 0) {
            return cmp;
        }
        cmp = year.compareTo(o.year);
        if (cmp != 0) {
            return cmp;
        }
        return month.compareTo(o.month);
    }

    public Text getHotelCity() {
        return hotelCity;
    }

    public void setHotelCity(Text hotelCity) {
        this.hotelCity = hotelCity;
    }

    public IntWritable getYear() {
        return year;
    }

    public void setYear(IntWritable year) {
        this.year = year;
    }

    public IntWritable getMonth() {
        return month;
    }

    public void setMonth(IntWritable month) {
        this.month = month;
    }

    public Text getHotelID() {
        return hotelID;
    }

    public void setHotelID(Text hotelID) {
        this.hotelID = hotelID;
    }

    @Override
    public String toString() {
        return "OccupancyKeyModel{" +
                "hotelCity=" + hotelCity +
                ", year=" + year +
                ", month=" + month +
                ", hotelID=" + hotelID +
                '}';
    }
}