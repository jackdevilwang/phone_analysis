package com.phone.analystic.mr.nu;

import com.phone.analystic.modle.StatsCommonDimension;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.base.BrowserDimension;
import com.phone.analystic.modle.base.DateDimension;
import com.phone.analystic.modle.base.KpiDimension;
import com.phone.analystic.modle.base.PlatformDimension;
import com.phone.analystic.modle.value.map.TimeOutputValue;
import com.phone.common.DateEnum;
import com.phone.common.EventLogConstants;
import com.phone.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @ClassName NewUserMapper
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description //TODO $
 **/
public class NewUserMapper extends Mapper<LongWritable,Text, StatsUserDimension, TimeOutputValue> {
    private Logger logger = Logger.getLogger(NewUserMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();
    private KpiDimension newUserKpi = new KpiDimension(KpiType.NEW_USER.kpiName);
    private KpiDimension browserNewUserKpi = new KpiDimension(KpiType.BROWSER_NEW_USER.kpiName);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (StringUtils.isEmpty(line)) {
            return;
        }

        String[] fields = line.split("\u0001");

        String en = fields[2];

        if (StringUtils.isNotEmpty(en) && en.equals(EventLogConstants.EventEnum.LAUNCH.alias)) {
            //获取想要的字段
            String serverTime = fields[1];
            String platform = fields[13];
            String uuid = fields[3];
            String browserName = fields[24];
            String browserVersion = fields[25];


            if(StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(uuid)){
                logger.info("serverTime & uuid is null.serverTime:"+serverTime+". uuid:"+uuid);
                return;
            }

            //构造输出的key
            long stime = Long.valueOf(serverTime);
            PlatformDimension platformDimension = PlatformDimension.getInstance(platform);
            DateDimension dateDimension = DateDimension.buildDate(stime, DateEnum.DAY);
            BrowserDimension browserDimension = BrowserDimension.getInstance(browserName,browserVersion);

            StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();

            //为statsCommonDimension设置
            statsCommonDimension.setDateDimension(dateDimension);
            statsCommonDimension.setPlatformDimension(platformDimension);
            statsCommonDimension.setKpiDimension(newUserKpi);

            //设置默认的浏览器对象
            BrowserDimension defaultBrowserDimension = new BrowserDimension("","");
            this.k.setBrowserDimension(defaultBrowserDimension);
            this.k.setStatsCommonDimension(statsCommonDimension);



            //构建输出的value
            this.v.setId(uuid);
            //输出
            context.write(this.k,this.v);

            //以下输出的数据用于计算浏览器模块下的用户
            statsCommonDimension.setKpiDimension(browserNewUserKpi);
            this.k.setStatsCommonDimension(statsCommonDimension);
            this.k.setBrowserDimension(browserDimension);
            context.write(this.k,this.v);

        }




    }
}