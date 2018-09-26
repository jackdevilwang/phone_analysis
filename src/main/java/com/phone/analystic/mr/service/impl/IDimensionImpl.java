package com.phone.analystic.mr.service.impl;

import com.phone.analystic.modle.base.*;
import com.phone.analystic.mr.service.IDimension;
import com.phone.util.JdbcUtil;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

public class IDimensionImpl implements IDimension {
    private static final Logger logger = Logger.getLogger(IDimensionImpl.class);
    private Map<String, Integer> cache = new LinkedHashMap<String, Integer>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
            return this.size() > 5000;
        }
    };

    @Override
    public int getDimensionIdByObject(BaseDimension dimension) {
        Connection conn = null;
        try {

            String cachekey = bulidCacheKey(dimension);
            if (this.cache.containsKey(cachekey)) {
                return this.cache.get(cachekey);
            }

            String sqls[] = null;
            if (dimension instanceof KpiDimension) {
                sqls = buidKpiSqls(dimension);
            }else if (dimension instanceof PlatformDimension) {
                sqls = buidPlatformSqls(dimension);
            }else if (dimension instanceof DateDimension) {
                sqls = buidDateSqls(dimension);
            }else if (dimension instanceof BrowserDimension) {
                sqls = buidBrowserSqls(dimension);
            }

            conn = JdbcUtil.getConn();
            int id = -1;
            synchronized (this) {
                id = this.executsql(conn, sqls, dimension);
            }

            this.cache.put(cachekey, id);
            return id;
        } catch (Exception e) {
           logger.warn("获取维度id异常",e);
        }finally {
            JdbcUtil.close(null, null, null);
        }
        throw new RuntimeException("获取维度id运行异常");
    }

    private int executsql(Connection conn, String[] sqls, BaseDimension dimension) {
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = conn.prepareStatement(sqls[1]);
            this.setArgs(dimension, ps);
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
            ps = conn.prepareStatement(sqls[0], Statement.RETURN_GENERATED_KEYS);
            this.setArgs(dimension, ps);
            ps.executeUpdate();
            rs = ps.getGeneratedKeys();
            if (rs.next()) {
                return rs.getInt(1);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(null, ps, rs);
        }
        return -1;
    }

    private void setArgs(BaseDimension dimension, PreparedStatement ps) {
        try {
            int i = 0;
            if(dimension instanceof KpiDimension){
                KpiDimension kpi = (KpiDimension)dimension;
                ps.setString(++i,kpi.getKpiName());
            } else if(dimension instanceof DateDimension){
                DateDimension date = (DateDimension)dimension;
                ps.setInt(++i,date.getYear());
                ps.setInt(++i,date.getSeason());
                ps.setInt(++i,date.getMonth());
                ps.setInt(++i,date.getWeek());
                ps.setInt(++i,date.getDay());
                ps.setString(++i,date.getType());
                ps.setDate(++i,new Date(date.getCalendar().getTime()));
            }  else if(dimension instanceof PlatformDimension){
                PlatformDimension platform = (PlatformDimension)dimension;
                ps.setString(++i,platform.getPlatformName());
            }  else if(dimension instanceof BrowserDimension){
                BrowserDimension browser = (BrowserDimension)dimension;
                ps.setString(++i,browser.getBrowserName());
                ps.setString(++i,browser.getBrowserVersion());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

    private String[] buidKpiSqls(BaseDimension dimension) {
        String inserSql = "insert into `dimension_kpi`(`kpi_name`) values(?)";
        String selectSql = "select `id` from `dimension_kpi` where kpi_name=?";
        return new String[]{inserSql, selectSql};

    }
    private String[] buidPlatformSqls(BaseDimension dimension) {
        String inserSql = "insert into `dimension_platform`(`platform_name`) values(?)";
        String selectSql = "select `id` from `dimension_platform` where platform_name=?";
        return new String[]{inserSql, selectSql};

    }
    private String[] buidDateSqls(BaseDimension dimension) {
        String inserSql = "insert into `dimension_date`(`year`, `season`, `month`, `week`, `day`, `type`, `calendar`) VALUES(?, ?, ?, ?, ?, ?, ?)";
        String selectSql = "select `id` from `dimension_date` WHERE `year` = ? AND `season` = ? AND `month` = ? AND `week` = ? AND `day` = ? AND `type` = ? AND `calendar` = ?";
        return new String[]{inserSql, selectSql};

    }
    private String[] buidBrowserSqls(BaseDimension dimension) {
        String inserSql = "insert into `dimension_browser`(`browser_name`, `browser_version`) VALUES(?,?)";
        String selectSql = "select `id` from `dimension_browser` WHERE `browser_name` = ? AND `browser_version` = ?";
        return new String[]{inserSql, selectSql};

    }


    private String bulidCacheKey(BaseDimension dimension) {
        StringBuffer sb = new StringBuffer();
        if (dimension instanceof BrowserDimension) {
            sb.append("browser_");
            BrowserDimension bd = (BrowserDimension) dimension;
            sb.append(bd.getBrowserName());
            sb.append(bd.getBrowserVersion());
        } else if (dimension instanceof PlatformDimension) {
            sb.append("platform_");
            PlatformDimension pd = (PlatformDimension) dimension;
            sb.append(pd.getPlatformName());
        } else if (dimension instanceof KpiDimension) {
            sb.append("kpi_");
            KpiDimension kd = (KpiDimension) dimension;
            sb.append(kd.getKpiName());
        } else if (dimension instanceof DateDimension) {
            sb.append("date_");
            DateDimension dd = (DateDimension) dimension;
            sb.append(dd.getYear());
            sb.append(dd.getSeason());
            sb.append(dd.getMonth());
            sb.append(dd.getWeek());
            sb.append(dd.getDay());
            sb.append(dd.getType());
        }
        return sb.toString();
    }
}
