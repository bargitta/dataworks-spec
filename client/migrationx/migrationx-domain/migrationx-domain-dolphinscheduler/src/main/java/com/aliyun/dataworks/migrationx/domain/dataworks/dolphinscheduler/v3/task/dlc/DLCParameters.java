package com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.dlc;

import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.parameters.AbstractParameters;

public class DLCParameters extends AbstractParameters {
    //DLC
    private String type;
    private int datasource;
    private String sql;
    private String sqlType;
    private int dlcTaskType;
    private String jobName;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getDatasource() {
        return datasource;
    }

    public void setDatasource(int datasource) {
        this.datasource = datasource;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getSqlType() {
        return sqlType;
    }

    public void setSqlType(String sqlType) {
        this.sqlType = sqlType;
    }

    public int getDlcTaskType() {
        return dlcTaskType;
    }

    public void setDlcTaskType(int dlcTaskType) {
        this.dlcTaskType = dlcTaskType;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public boolean checkParameters() {
        return false;
    }
}
