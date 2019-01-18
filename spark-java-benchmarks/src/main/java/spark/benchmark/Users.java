package spark.benchmark;

import java.io.Serializable;
import java.sql.Timestamp;

public class Users implements Serializable {
    private int userID = 0;
    private int gemPackID = 0;
    private int price = 0;
    private Timestamp timeStamp;
    public Users(int userID, int gemPackID, int price, Timestamp timeStamp) {
        this.userID = userID;
        this.gemPackID = gemPackID;
        this.price = price;
        this.timeStamp = timeStamp;
    }

    public int getUserID() {
        return userID;
    }

    public void setUserID(int userID) {
        this.userID = userID;
    }

    public int getGemPackID() {
        return gemPackID;
    }

    public void setGemPackID(int gemPackID) {
        this.gemPackID = gemPackID;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public Timestamp getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Timestamp timeStamp) {
        this.timeStamp = timeStamp;
    }
}
