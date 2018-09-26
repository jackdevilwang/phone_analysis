package com.phone.analystic.mr;

import com.phone.analystic.modle.StatsBaseDimension;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.analystic.mr.service.IDimension;
import com.phone.analystic.mr.service.impl.IDimensionImpl;
import com.phone.common.GlobalConstants;
import com.phone.common.KpiType;
import com.phone.util.JdbcUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class OutputToMySqlFormat extends OutputFormat<StatsBaseDimension, OutputWritable> {

    private static final Logger logger = Logger.getLogger(OutputToMySqlFormat.class);
    @Override
    public RecordWriter<StatsBaseDimension, OutputWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Connection conn = JdbcUtil.getConn();
        Configuration conf = taskAttemptContext.getConfiguration();
//        conf.addResource("output_mapping.xml");
//        conf.addResource("output_writter.xml");

        IDimension iDimension = new IDimensionImpl();
        return new OutputToMySqlRecordWritter(conf, conn, iDimension);

    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new FileOutputCommitter(null, taskAttemptContext);
    }

    public static class OutputToMySqlRecordWritter extends RecordWriter<StatsBaseDimension, OutputWritable> {
        Configuration conf = null;
        Connection conn = null;
        IDimension iDimension = null;



        private Map<KpiType, PreparedStatement> map = new HashMap<KpiType, PreparedStatement>();

        private Map<KpiType, Integer> batch = new HashMap<KpiType, Integer>();

        public OutputToMySqlRecordWritter(Configuration conf, Connection conn, IDimension iDimension) {
            this.conf = conf;
            this.conn = conn;
            this.iDimension = iDimension;
        }

        @Override
        public void write(StatsBaseDimension key, OutputWritable value) throws IOException, InterruptedException {
            if(key == null || value == null){
                return;
            }


            KpiType kpi = value.getKpi();
            PreparedStatement ps = null;
            int count = 1;

            try {
                if (map.containsKey(kpi)) {
                    ps = map.get(kpi);
                    count = this.batch.get(kpi);
                    count++;
                } else {
                    ps = conn.prepareStatement(conf.get(kpi.kpiName));
                    map.put(kpi, ps);
                }


                this.batch.put(kpi, count);

                String className = conf.get(GlobalConstants.WRITTER_PREFIX + kpi.kpiName);

                Class<?> classz = Class.forName(className);

                IOutputWritter writter = (IOutputWritter) classz.newInstance();

                writter.output(conf, key, value, ps, iDimension);

                if (count % GlobalConstants.BATCH_NUMBER == 0) {
                    ps.executeUpdate();
//                    this.conn.commit();
                    this.batch.remove(kpi);
                }


            } catch (Exception e) {
                logger.warn("执行写recordWriter的write方法失败",e);
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            try {
                for (Map.Entry<KpiType,PreparedStatement> en:map.entrySet()){
                    en.getValue().executeBatch(); //将剩余的ps进行执行
//                    this.conn.commit();
                }
            } catch (SQLException e) {
                logger.error("在close时，执行剩余的ps错误.",e);
            } finally {
                for (Map.Entry<KpiType,PreparedStatement> en:map.entrySet()){
                    JdbcUtil.close(conn,en.getValue(),null); //关闭所有能关闭的资源
                }
            }
        }
    }
}
