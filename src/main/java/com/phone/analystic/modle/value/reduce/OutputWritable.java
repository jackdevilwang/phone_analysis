package com.phone.analystic.modle.value.reduce;

import com.phone.analystic.modle.value.StatsOutputValue;
import com.phone.common.KpiType;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OutputWritable extends StatsOutputValue {
    private KpiType kpi;
    private MapWritable newUserStatsvalue = new MapWritable();

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeEnum(dataOutput,kpi);
        this.newUserStatsvalue.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        WritableUtils.readEnum(dataInput,KpiType.class);
        this.newUserStatsvalue.readFields(dataInput);
    }

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    public MapWritable getnewUserStatsvalue() {
        return newUserStatsvalue;
    }

    public void setnewUserStatsvalue(MapWritable value) {
        this.newUserStatsvalue = value;
    }
}
