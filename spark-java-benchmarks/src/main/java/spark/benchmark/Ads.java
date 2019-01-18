package spark.benchmark;

import java.io.Serializable;
import java.sql.Timestamp;

public class Ads implements Serializable {
    private int userID = 0;
    private int gemPackID = 0;
    private Timestamp timeStamp;

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

    public Timestamp getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Timestamp timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Ads(int userID, int gemPackID, Timestamp timeStamp) {
        this.userID = userID;
        this.gemPackID = gemPackID;
        this.timeStamp = timeStamp;
    }


}
