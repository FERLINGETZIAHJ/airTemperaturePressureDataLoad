package com.weather.common;

import java.io.Serializable;

public class FieldNames implements Serializable{
    private int startYear;
    private int endYear;
    private long yearCount;

    public long getYearCount() {
        return yearCount;
    }

    public void setYearCount(long yearCount) {
        this.yearCount = yearCount;
    }

    public int getStartYear() {
        return startYear;
    }

    public void setStartYear(int startYear) {
        this.startYear = startYear;
    }

    public int getEndYear() {
        return endYear;
    }

    public void setEndYear(int endYear) {
        this.endYear = endYear;
    }


}
