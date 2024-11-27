package tead;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReservationModel implements WritableComparable<ReservationModel> {

    private Text hotelCity;

    private Text hotelID;

    private Text country;

    private IntWritable hotelStars;
    private FloatWritable price;

    private Text reservationId;

    private Text reservationStatus;

    public ReservationModel(){
        this.reservationId = new Text();
        this.hotelCity = new Text();
        this.hotelStars = new IntWritable();
        this.price = new FloatWritable();
        this.hotelID = new Text();
        this.country = new Text();
        this.reservationStatus= new Text();
    }

    public ReservationModel(Text reservationId,Text hotelCity,Text hotelID,Text country,Text reservationStatus, IntWritable hotelStars, FloatWritable price) {
        this.hotelCity = hotelCity;
        this.hotelStars = hotelStars;
        this.price = price;
        this.reservationId= reservationId;
        this.hotelID = hotelID;
        this.country = country;
        this.reservationStatus= reservationStatus;

    }


    public void set(ReservationModel reservationModel) {
        this.hotelCity.set(reservationModel.getHotelCity().toString());
        this.hotelStars.set(reservationModel.getHotelStars().get());
        this.price.set(reservationModel.getPrice().get());
        this.reservationId.set(reservationModel.getReservationId().toString());
        this.hotelID.set(reservationModel.getReservationId().toString());
        this.country.set(reservationModel.getCountry().toString());
        this.reservationStatus.set(reservationModel.getReservationStatus().toString());
    }


    @Override
    public int compareTo(ReservationModel o) {
        return this.reservationId.compareTo(o.reservationId);

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        hotelCity.write(dataOutput);
        hotelStars.write(dataOutput);
        price.write(dataOutput);
        reservationId.write(dataOutput);
        hotelID.write(dataOutput);
        reservationStatus.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        hotelCity.readFields(dataInput);
        hotelStars.readFields(dataInput);
        price.readFields(dataInput);
        reservationId.readFields(dataInput);
        hotelID.readFields(dataInput);
        reservationStatus.readFields(dataInput);
    }

    public Text getHotelCity() {
        return hotelCity;
    }

    public void setHotelCity(Text hotelCity) {
        this.hotelCity = hotelCity;
    }

    public IntWritable getHotelStars() {
        return hotelStars;
    }

    public void setHotelStars(IntWritable hotelStars) {
        this.hotelStars = hotelStars;
    }

    public FloatWritable getPrice() {
        return price;
    }

    public void setPrice(FloatWritable price) {
        this.price = price;
    }

    public Text getReservationId() {
        return reservationId;
    }

    public void setReservationId(Text reservationId) {
        this.reservationId = reservationId;
    }

    public Text getHotelID() {
        return hotelID;
    }

    public void setHotelID(Text hotelID) {
        this.hotelID = hotelID;
    }

    public Text getCountry() {
        return country;
    }

    public void setCountry(Text country) {
        this.country = country;
    }

    public Text getReservationStatus() {
        return reservationStatus;
    }

    public void setReservationStatus(Text reservationStatus) {
        this.reservationStatus = reservationStatus;
    }
}
