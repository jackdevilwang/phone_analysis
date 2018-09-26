package com.phone.analystic.mr;

import com.phone.analystic.modle.StatsBaseDimension;
import com.phone.analystic.modle.value.StatsOutputValue;
import com.phone.analystic.mr.service.IDimension;
import org.apache.hadoop.conf.Configuration;
import java.sql.PreparedStatement;

public interface IOutputWritter {
    void output(Configuration conf, StatsBaseDimension key,
                StatsOutputValue value, PreparedStatement ps, IDimension iDimension);


}
