package com.phone.common;
/*
 * 时间枚举
 * */


public enum DateEnum {

    YEAR("year"),
    SEASON("season"),
    MONTH("month"),
    WEEK("week"),
    DAY("day"),
    HOUR("hour")
    ;

    public String dateType;

    DateEnum(String dateType) {

        this.dateType = dateType;

    }

    //根据type获取枚举

    public static DateEnum valueOfType(String type) {
        for (DateEnum date : values()) {
            if (type.equals(date.dateType)) {
                return date;
            }
        }
        return null;
    }

}
