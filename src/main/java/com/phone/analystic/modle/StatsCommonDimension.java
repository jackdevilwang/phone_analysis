package com.phone.analystic.modle;

import com.phone.analystic.modle.base.BaseDimension;
import com.phone.analystic.modle.base.DateDimension;
import com.phone.analystic.modle.base.KpiDimension;
import com.phone.analystic.modle.base.PlatformDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class StatsCommonDimension extends StatsBaseDimension {

    public DateDimension dateDimension = new DateDimension();
    public PlatformDimension platformDimension = new PlatformDimension();
    public KpiDimension kpiDimension = new KpiDimension();

    public StatsCommonDimension() {
    }

    public StatsCommonDimension(DateDimension dateDimension, PlatformDimension platformDimension, KpiDimension kpiDimension) {
        this.dateDimension = dateDimension;
        this.platformDimension = platformDimension;
        this.kpiDimension = kpiDimension;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.dateDimension.write(out);
        this.platformDimension.write(out);
        this.kpiDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.dateDimension.readFields(in);
        this.platformDimension.readFields(in);
        this.kpiDimension.readFields(in);

    }

    public static  StatsCommonDimension clone(StatsCommonDimension dimension){
        PlatformDimension platformDimension = new PlatformDimension(dimension.
                platformDimension.getPlatformName());
        KpiDimension kpiDiemension = new KpiDimension(dimension.kpiDimension.getKpiName());
        DateDimension dateDimension = new DateDimension(dimension.dateDimension.getYear(),
                dimension.dateDimension.getSeason(),dimension.dateDimension.getMonth(),
                dimension.dateDimension.getWeek(),dimension.dateDimension.getDay(),
                dimension.dateDimension.getType(),dimension.dateDimension.getCalendar());
        return new StatsCommonDimension(dateDimension,platformDimension,kpiDiemension);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dateDimension, platformDimension, kpiDimension);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        StatsCommonDimension that = (StatsCommonDimension) obj;
        return Objects.equals(dateDimension, that.dateDimension) &&
                Objects.equals(platformDimension, that.platformDimension) &&
                Objects.equals(kpiDimension, that.kpiDimension);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(o == this){
            return 0;
        }
        StatsCommonDimension other = (StatsCommonDimension) o;
        int tmp = this.dateDimension.compareTo(other.dateDimension);
        if(tmp != 0){
            return  tmp;
        }
        tmp = this.platformDimension.compareTo(other.platformDimension);
        if(tmp != 0){
            return  tmp;
        }
        return this.kpiDimension.compareTo(other.kpiDimension);
    }


    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    public PlatformDimension getPlatformDimension() {
        return platformDimension;
    }

    public void setPlatformDimension(PlatformDimension platformDimension) {
        this.platformDimension = platformDimension;
    }

    public KpiDimension getKpiDimension() {
        return kpiDimension;
    }

    public void setKpiDimension(KpiDimension kpiDimension) {
        this.kpiDimension = kpiDimension;
    }
}
