package com.phone.analystic.mr.nu;

import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.base.DateDimension;
import com.phone.analystic.modle.value.map.TimeOutputValue;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.analystic.mr.OutputToMySqlFormat;
import com.phone.analystic.mr.service.IDimension;
import com.phone.analystic.mr.service.impl.IDimensionImpl;
import com.phone.common.DateEnum;
import com.phone.common.GlobalConstants;
import com.phone.util.JdbcUtil;
import com.phone.util.TimeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName NewUserRunner
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 新增用户的runner
truncate dimension_browser;
truncate dimension_currency_type;
truncate dimension_date;
truncate dimension_event;
truncate dimension_inbound;
truncate dimension_kpi;
truncate dimension_location;
truncate dimension_os;
truncate dimension_payment_type;
truncate dimension_platform;
truncate event_info;
truncate order_info;
truncate stats_device_browser;
truncate stats_device_location;
truncate stats_event;
truncate stats_hourly;
truncate stats_inbound;
truncate stats_order;
truncate stats_user;
truncate stats_view_depth;

 **/
public class NewUserRunner implements Tool {
    private static final Logger logger = Logger.getLogger(NewUserRunner.class);

    private Configuration conf = new Configuration();

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new NewUserRunner(), args);
        } catch (Exception e) {
            logger.warn("运行异常", e);
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

//        conf.set("mapreduce.framework.name","yarn");
//        conf.set("yarn.resourcemanager.address","hadoop01:8032");
//        conf.setBoolean("mapreduce.app-submission.cross-platform",true);
//
//        DBConfiguration.configureDB(conf,
//                GlobalConstants.DRIVER,
//                GlobalConstants.URL,
//                GlobalConstants.USERNAME,
//                GlobalConstants.PASSWORD);

        this.setArgs(args, conf);

        Job job = Job.getInstance(conf,"new_user");

        job.setJarByClass(NewUserRunner.class);

        //设置map端的属性

        job.setMapperClass(NewUserMapper.class);

        job.setMapOutputKeyClass(StatsUserDimension.class);
        job.setMapOutputValueClass(TimeOutputValue.class);

        //设置reducer类
        job.setReducerClass(NewUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(OutputWritable.class);

        //设置redcuce的输出格式类
        job.setOutputFormatClass(OutputToMySqlFormat.class);



        setInputPath(job);

//        return job.waitForCompletion(true) ? 0 : 1;
        if(job.waitForCompletion(true)){
            this.computeTotalNewUser(job);
            return 0;
        } else {
            return 1;
        }
    }
    /**
     * 计算新增的总用户
     * 1、获取运行日期当天和前一天的时间维度，并获取其对应的时间维度id，判断id是否大于0。
     * 2、根据时间维度的id获取前天的总用户和当天的新增用户。
     * 3、更新新增总用户
     * @param job
     */
    private void computeTotalNewUser(Job job) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            //获取运行当天的日期
            long nowDate = TimeUtil.parseString2Long(job.getConfiguration().get(GlobalConstants.RUNNING_DATE));
            long yesterdayDate = nowDate = GlobalConstants.DAY_OF_MILLSECONDS;

            //构建时间维度对象
            DateDimension nowDateDimension = DateDimension.buildDate(nowDate, DateEnum.DAY);
            DateDimension yesterdayDimension = DateDimension.buildDate(yesterdayDate, DateEnum.DAY);

            //获取对应时间维度的id
            int nowDimensionId = -1;
            int yesterdayDimensionId = -1;
            IDimension convert = new IDimensionImpl();
            //没理解
            nowDimensionId = convert.getDimensionIdByObject(nowDateDimension);
            yesterdayDimensionId = convert.getDimensionIdByObject(yesterdayDimension);


            //===========================用户模块下==================================
            //获取昨天的新增总用户
            conn = JdbcUtil.getConn();
            Map<String, Integer> map = new HashMap<String, Integer>();

            //没理解
            if (yesterdayDimensionId > 0) {
                ps = conn.prepareStatement(conf.get(GlobalConstants.TOTAL_PREFIX + "new_total_user"));
                //赋值
                ps.setInt(1,yesterdayDimensionId);
                rs = ps.executeQuery();
                while (rs.next()) {
                    int platformId = rs.getInt("platform_dimension_id");
                    int totalUser = rs.getInt("total_install_users");
                    //存储
                    map.put(platformId + "", totalUser);
                }
            }
            //获取今天的新增用户
            if (nowDimensionId > 0) {
                ps = conn.prepareStatement(conf.get(GlobalConstants.TOTAL_PREFIX + "new_user"));
                //赋值
                ps.setInt(1, nowDimensionId);
                rs = ps.executeQuery();
                while (rs.next()) {
                    int platformId = rs.getInt("platform_dimension_id");
                    int newUsers = rs.getInt("new_install_users");
                    //存储
                    if (map.containsKey(platformId + "")) {
                        newUsers += map.get(platformId + "");
                    }
                    map.put(platformId + "", newUsers);
                }
            }

            //将map中数据进行更新
            ps = conn.prepareStatement(conf.get(GlobalConstants.TOTAL_PREFIX + "new_update_user"));
            for (Map.Entry<String, Integer> en : map.entrySet()) {
                //赋值
                ps.setInt(1, nowDimensionId);
                ps.setInt(2, Integer.parseInt(en.getKey()));
                ps.setInt(3, en.getValue());
                ps.setString(4, conf.get(GlobalConstants.RUNNING_DATE));
                ps.setInt(5, en.getValue());
                //执行
                ps.addBatch();
            }
            ps.executeBatch(); //批量执行
            //=========================用户模块下end============================================


        } catch (Exception e) {
            logger.warn("计算新增总用户失败.",e);
        } finally {
            JdbcUtil.close(conn,ps,rs);
        }


    }

    private void setInputPath(Job job) {
        String date = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
        String fields[] = date.split("-");
        String month = fields[1];
        String day = fields[2];

        System.out.println(date+month+day);

        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            Path inputPath = new Path("/ods/"+month+"/"+day);
            if (fs.exists(inputPath)) {
                FileInputFormat.addInputPath(job, inputPath);
            } else {
                throw new RuntimeException("数据数据路径不存在.");
            }
        } catch (IOException e) {
            logger.warn("获取fs对象异常.", e);
        }
    }


    private void setArgs(String[] args, Configuration conf) {
        String date = null;

        if(args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("-d")) {
                    if (i + 1 < args.length) {
                        date = args[i + 1];
                        break;
                    }
                }
            }
            //代码到这儿，date还是null，默认用昨天的时间
            if (StringUtils.isEmpty(date) || !TimeUtil.isValidateDate(date)) {
                date = TimeUtil.getYesterday();
            }
            //然后将date设置到时间conf中
            conf.set(GlobalConstants.RUNNING_DATE, date);
        }
    }

    @Override
    public void setConf(Configuration conf) {
        conf.addResource("output_mapping.xml");
        conf.addResource("output_writter.xml");
        conf.addResource("total_mapping.xml");
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
}
