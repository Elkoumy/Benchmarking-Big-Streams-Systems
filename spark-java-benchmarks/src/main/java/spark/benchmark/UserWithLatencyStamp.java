package spark.benchmark;

import java.io.Serializable;

public class UserWithLatencyStamp implements Serializable {
    private int userID = 0;
    private int gemPackID = 0;
    private int price = 0;
    private long timeStamp = 0;
    String ltcID="";

    public UserWithLatencyStamp(int userID, int gemPackID, int price, long timeStamp, String ltcID) {
        this.userID = userID;
        this.gemPackID = gemPackID;
        this.price = price;
        this.timeStamp = timeStamp;
        this.ltcID = ltcID;
    }

    public String getLtcID() {
        return ltcID;
    }

    public void setUidWithTS(String ltcID) {
        this.ltcID = ltcID;
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

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

}
