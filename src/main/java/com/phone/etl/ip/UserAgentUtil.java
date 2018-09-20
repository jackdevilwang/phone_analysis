package com.phone.etl.ip;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import org.apache.log4j.Logger;
import org.datanucleus.util.StringUtils;

import java.io.IOException;

public class UserAgentUtil {
    public static final Logger logger = Logger.getLogger(UserAgentUtil.class);

    private static UASparser ua = null;


    static {

        try {
            ua = new UASparser(OnlineUpdater.getVendoredInputStream());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static UserAgentInfo parserUserAgent(String agent){
        UserAgentInfo uainfo = new UserAgentInfo();
        if (StringUtils.isEmpty(agent)) {
            logger.warn("agent为空");
            return null;
        }

        try {
            cz.mallat.uasparser.UserAgentInfo info = ua.parse(agent);

            uainfo.setBrowserName(info.getUaFamily());
            uainfo.setBrowserVersion(info.getBrowserVersionInfo());
            uainfo.setOsName(info.getOsFamily());
            uainfo.setOsVersion(info.getOsName());
        } catch (IOException e) {
            logger.warn("解析useragent异常", e);
        }

        return uainfo;
    }



    /**
     * 用于封装useragent解析后的信息
     */
    public static class UserAgentInfo{
        private String browserName;
        private String browserVersion;
        private String osName;
        private String osVersion;

        public String getBrowserName() {
            return browserName;
        }

        public void setBrowserName(String browserName) {
            this.browserName = browserName;
        }

        public String getBrowserVersion() {
            return browserVersion;
        }

        public void setBrowserVersion(String browserVersion) {
            this.browserVersion = browserVersion;
        }

        public String getOsName() {
            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }

        @Override
        public String toString() {
            return "UserAgentInfo{" +
                    "browserName='" + browserName + '\'' +
                    ", browserVersion='" + browserVersion + '\'' +
                    ", osName='" + osName + '\'' +
                    ", osVersion='" + osVersion + '\'' +
                    '}';
        }
    }
}
