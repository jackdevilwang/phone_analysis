package com.phone.analystic.modle;

import com.phone.analystic.modle.base.BaseDimension;
import com.phone.analystic.modle.base.BrowserDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class StatsUserDimension extends StatsBaseDimension{
    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();

    private BrowserDimension browserDimension = new BrowserDimension();

    public StatsUserDimension() {
    }

    public StatsUserDimension(StatsCommonDimension statsCommonDimension, BrowserDimension browserDimension) {
        this.statsCommonDimension = statsCommonDimension;
        this.browserDimension = browserDimension;
    }

    @Override
    public int hashCode() {
        return Objects.hash(browserDimension, statsCommonDimension);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        StatsUserDimension that = (StatsUserDimension) obj;
        return Objects.equals(browserDimension, that.browserDimension) &&
                Objects.equals(statsCommonDimension, that.statsCommonDimension);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(o == this){
            return 0;
        }
        StatsUserDimension other = (StatsUserDimension) o;
        int tmp = this.browserDimension.compareTo(other.browserDimension);
        if(tmp != 0){
            return  tmp;
        }
        return this.statsCommonDimension.compareTo(other.statsCommonDimension);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.browserDimension.write(out);
        this.statsCommonDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.browserDimension.readFields(in);
        this.statsCommonDimension.readFields(in);
    }

//    /**
//     * 克隆当前对象的实例
//     * @param dimension
//     * @return
//     */
//    public static StatsUserDimension clone(StatsUserDimension dimension){
//        BrowserDimension browserDimension = new BrowserDimension(dimension.browserDimension.getBrowserName(),
//                dimension.browserDimension.getBrowserVersion());
//        StatsCommonDimension statsCommonDimension = StatsCommonDimension.clone(dimension.statsCommonDimension);
//        return new StatsUserDimension(statsCommonDimension,browserDimension);
//    }


    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public BrowserDimension getBrowserDimension() {
        return browserDimension;
    }

    public void setBrowserDimension(BrowserDimension browserDimension) {
        this.browserDimension = browserDimension;
    }
}
