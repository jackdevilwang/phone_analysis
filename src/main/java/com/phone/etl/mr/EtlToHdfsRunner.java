package com.phone.etl.mr;

/*
*  源数据： /log/09/18
   清洗后的存储目录/ods/09/19
* */

import com.phone.common.GlobalConstants;
import com.phone.util.TimeUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import java.io.IOException;


public class EtlToHdfsRunner implements Tool {
    private static final Logger logger = Logger.getLogger(EtlToHdfsRunner.class);
    Configuration conf = new Configuration();

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new EtlToHdfsRunner(), args);
        } catch (Exception e) {
            logger.warn("运行etl to hdfs 异常", e);
        }
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public void setConf(Configuration conf) {
//        this.conf = HBaseConfiguration.createClusterConf();
        this.conf = conf;
    }

    @Override
    public int run(String[] args) throws Exception{

        Configuration conf = getConf();
        //设置处理的参数

        this.setArgs(args, conf);

        Job job = Job.getInstance(conf, "etl to hdfs");
        job.setJarByClass(EtlToHdfsRunner.class);

        //设置map端的属性

        job.setMapperClass(EtlToHdfsMapper.class);
        job.setMapOutputKeyClass(LogWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);
        //设置map阶段的输入路径
        this.setInputOutputPath(job);
        return job.waitForCompletion(true) ? 0 : 1;
    }

//    @Override
//    public Set<SourceVersion> getSourceVersions() {
//        return null;
//    }


    /**
     * 参数处理  ,将接收到的日期存储在conf中，以供后续使用
     *
     * @param args 如果没有传递日期，则默认使用昨天的日期
     * @param conf
     */
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
            if (date == null) {
                date = TimeUtil.getYesterday();
            }
            //然后将date设置到时间conf中
            conf.set(GlobalConstants.RUNNING_DATE, date);
        }
    }

    /**
     * 设置输入输出的路径
     *
     * @param job
     */
    private void setInputOutputPath(Job job) {
        String date = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
        String fields[] = date.split("-");
        String month = fields[1];
        String day = fields[2];

        System.out.println(date+month+day);

        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            Path inputPath = new Path("/logs/"+month+"/"+day);
            Path outputPath = new Path("/ods/"+month+"/"+day);
            if (fs.exists(inputPath)) {
                FileInputFormat.addInputPath(job, inputPath);
            } else {
                throw new RuntimeException("数据数据路径不存在.");
            }
            //设置输出
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }
            //设置输出
            FileOutputFormat.setOutputPath(job, outputPath);
        } catch (IOException e) {
            logger.warn("获取fs对象异常.", e);
        }
    }


}
