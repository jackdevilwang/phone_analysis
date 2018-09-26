package com.phone;

import com.phone.etl.IpUtil;
import com.phone.etl.ip.IPSeeker;
import com.phone.etl.ip.UserAgentUtil;

import java.awt.*;
import java.sql.SQLOutput;
import java.util.Date;

public class IpTest {

    public static void main(String[] args) {


        System.out.println(IPSeeker.getInstance().getCountry("112.111.11.12"));

        System.out.println(new IpUtil().getRegionInfoByIp("112.111.11.12"));

        System.out.println(UserAgentUtil.parserUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:62.0) Gecko/20100101 Firefox/62.0"));




    }
}
