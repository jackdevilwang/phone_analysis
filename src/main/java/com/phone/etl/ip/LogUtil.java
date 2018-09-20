package com.phone.etl.ip;

import com.phone.common.EventLogConstants;
import com.phone.etl.IpUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


//日志解析工具
//@param log 192.168.216.1^A1534231880.767^A192.168.216.111^A/qf.png?uuid=1289-dfsf-erw2&name=zs

public class LogUtil {

    private static final Logger logger = Logger.getLogger(LogUtil.class);

    //单行日志解析


    public static Map<String, String> parserLog(String log) {

        Map<String, String> info = new ConcurrentHashMap<String, String>();

        if (StringUtils.isNotEmpty(log.trim())) {
            //拆分单行日志
            String[] fields = log.split(EventLogConstants.LOG_SEPARTOR);

            if (fields.length == 4) {
                //存储数据到info中
                info.put(EventLogConstants.EVENT_COLUMN_NAME_IP, fields[0]);
                info.put(EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME, fields[1]
                        .replaceAll("\\.",""));

                //处理参数列表
                String params = fields[3];
                handleParams(info,params);
                handleUserAgent(info);
                handleIp(info);




            }
        }

        return info;
    }
    //处理agent

    private static void handleUserAgent(Map<String, String> info) {
        if (info.containsKey(EventLogConstants.EVENT_COLUMN_NAME_USERAGENT)) {
            UserAgentUtil.UserAgentInfo ua = UserAgentUtil
                    .parserUserAgent(info.get(EventLogConstants.EVENT_COLUMN_NAME_USERAGENT));

            if (ua != null) {
                info.put(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_NAME, ua.getBrowserName());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_VERSION, ua.getBrowserVersion());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_OS_NAME, ua.getOsName());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_OS_VERSION, ua.getOsVersion());
            }
        }
    }

    //处理ip

    private static void handleIp(Map<String, String> info) {
        if (info.containsKey(EventLogConstants.EVENT_COLUMN_NAME_IP)) {
            IpUtil.RegionInfo ri = new IpUtil()
                    .getRegionInfoByIp(info.get(EventLogConstants.EVENT_COLUMN_NAME_IP));
            if (ri != null) {
                info.put(EventLogConstants.EVENT_COLUMN_NAME_COUNTRY, ri.getCountry());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_PROVINCE, ri.getProvince());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_CITY, ri.getCity());
            }
        }
    }

    //处理参数

    private static void handleParams(Map<String, String> info, String field) {
        if (StringUtils.isNotEmpty(field)) {
            int index = field.indexOf("?");
            if (index > 0) {
                String fields = field.substring(index + 1);
                String[] params = fields.split("&");
                for (int i = 0; i < params.length; i++) {
                    String[] kvs = params[i].split("=");

                    try {
                        String k = kvs[0];
                        String v = URLDecoder.decode(kvs[1], "utf-8");
                        if (StringUtils.isNotEmpty(k)) {
                            //存储数据到info
                            info.put(k, v);
                        }
                    } catch (UnsupportedEncodingException e) {
                        logger.warn("url的解码异常。",e);
                    }

                }
            }
        }
    }
}
