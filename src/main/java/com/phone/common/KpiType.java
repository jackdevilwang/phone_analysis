package com.phone.common;

public enum KpiType {
    NEW_USER("new_user"),
    BROWSER_NEW_USER("browser_new_user");

    public String kpiName;

    KpiType(String kpiName) {

        this.kpiName = kpiName;

    }

    //根据type获取枚举

    public static KpiType valueOfKpiType(String type) {
        for (KpiType kpi : values()) {
            if (type.equals(kpi.kpiName)) {
                return kpi;
            }
        }
        return null;
    }

}
