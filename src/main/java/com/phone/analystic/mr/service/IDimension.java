package com.phone.analystic.mr.service;

import com.phone.analystic.modle.base.BaseDimension;

import java.io.IOException;
import java.sql.SQLException;

public interface IDimension {
    int getDimensionIdByObject(BaseDimension dimension) throws IOException, SQLException;
}
