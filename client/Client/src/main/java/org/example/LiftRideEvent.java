package org.example;

public class LiftRideEvent {
    private int skierID;
    private int resortID;
    private int liftID;
    private String seasonID;
    private int dayID;
    private int time;

    public LiftRideEvent(int skierID, int resortID, int liftID,
                         String seasonID, int dayID, int time) {
        this.skierID = skierID;
        this.resortID = resortID;
        this.liftID = liftID;
        this.seasonID = seasonID;
        this.dayID = dayID;
        this.time = time;
    }

    // Default constructor for JSON deserialization
    public LiftRideEvent() {}

    public int getSkierID() {
        return skierID;
    }

    public void setSkierID(int skierID) {
        this.skierID = skierID;
    }

    public int getResortID() {
        return resortID;
    }

    public void setResortID(int resortID) {
        this.resortID = resortID;
    }

    public int getLiftID() {
        return liftID;
    }

    public void setLiftID(int liftID) {
        this.liftID = liftID;
    }

    public String getSeasonID() {
        return seasonID;
    }

    public void setSeasonID(String seasonID) {
        this.seasonID = seasonID;
    }

    public int getDayID() {
        return dayID;
    }

    public void setDayID(int dayID) {
        this.dayID = dayID;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public boolean isValid() {
        return skierID >= 1 && skierID <= 100000 &&
                resortID >= 1 && resortID <= 10 &&
                liftID >= 1 && liftID <= 40 &&
                "2025".equals(seasonID) &&
                dayID == 1 &&
                time >= 1 && time <= 360;
    }
}
