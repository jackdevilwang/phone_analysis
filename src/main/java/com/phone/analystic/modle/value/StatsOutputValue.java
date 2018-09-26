package com.phone.analystic.modle.value;

import com.phone.common.KpiType;
import org.apache.hadoop.io.Writable;

public abstract class StatsOutputValue implements Writable {
    //获取kpi的抽象方法
    public abstract KpiType getKpi ();
}
