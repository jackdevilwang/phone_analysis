package com.phone.common;

public enum KpiType {
    NEW_USER("new_user"),
    BROWSER_NEW_USER("browser_new_user"),
    ACTIVE_USER("active_user"),
    BROWSER_ACTIVE_USER("browser_active_user"),
    ACTIVE_MEMBER("active_member"),
    BROWSER_ACTIVE_MEMBER("browser_active_member"),
    NEW_MEMBER("new_member"),
    BROWSER_NEW_MEMBER("browser_new_member"),
    MEMBER_INFO("member_info"),
    SESSION("session"),
    BROWSER_SESSION("browser_session"),
    HOURLY_ACTIVE_USER("hourly_active_user"),
    PAGEVIEW("pageview"),
    BROWSER_PAGEVIEW("browser_pageview"),
    LOCAL("local"),
    ;

    public String kpiName; //

    KpiType(String kpiName) {
        this.kpiName = kpiName;
    }

    public static KpiType valueOfKpiName(String name){
        for (KpiType kpi : values()){
            if(name.equals(kpi.kpiName)){
                return kpi;
            }
        }
        return null;
    }

}
