package cs6650;

import com.google.gson.annotations.SerializedName;

public class LiftRideEvent {
    @SerializedName("skierID")
    private int skierId;

    @SerializedName("resortID")
    private int resortId;

    @SerializedName("liftID")
    private int liftId;

    @SerializedName("seasonID")
    private String seasonId; // Note: This should be String based on the JSON

    @SerializedName("dayID")
    private int dayId;

    private int time; // This one matches already

    // Default constructor for Gson deserialization
    public LiftRideEvent() {
    }

    // Getters and setters remain the same
    public int getSkierId() {
        return skierId;
    }

    public void setSkierId(int skierId) {
        this.skierId = skierId;
    }

    public int getResortId() {
        return resortId;
    }

    public void setResortId(int resortId) {
        this.resortId = resortId;
    }

    public int getLiftId() {
        return liftId;
    }

    public void setLiftId(int liftId) {
        this.liftId = liftId;
    }

    public String getSeasonId() { // Changed to String
        return seasonId;
    }

    public void setSeasonId(String seasonId) { // Changed to String
        this.seasonId = seasonId;
    }

    public int getDayId() {
        return dayId;
    }

    public void setDayId(int dayId) {
        this.dayId = dayId;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "LiftRideEvent{" +
                "skierId=" + skierId +
                ", resortId=" + resortId +
                ", liftId=" + liftId +
                ", seasonId=" + seasonId +
                ", dayId=" + dayId +
                ", time=" + time +
                '}';
    }
}