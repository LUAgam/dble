/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.helper;

import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.plan.common.ptr.BoolPtr;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.OneTimeConnJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class CreateDelayDetectTableTask extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateDelayDetectTableTask.class);
    private static final String LOGIC_TIMESTAMP_COLUMN = "logic_timestamp";
    private static final long LOGIC_TIMESTAMP_UNSET = -1L;
    private final PhysicalDbInstance ds;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition finishCond = lock.newCondition();
    private boolean isFinish = false;
    private final BoolPtr successFlag;
    private volatile long logicTimestamp = LOGIC_TIMESTAMP_UNSET;

    public CreateDelayDetectTableTask(PhysicalDbInstance ds, BoolPtr successFlag) {
        this.ds = ds;
        this.successFlag = successFlag;
    }

    @Override
    public void run() {
        String table = ds.getDbGroupConfig().getDelayDatabase() + ".u_delay";

        createDelayDetectTable(table);
        if (!successFlag.get()) {
            return;
        }
        Long initLogicTimestamp = queryLogicTimestamp(table);
        if (initLogicTimestamp != null) {
            ds.getDbGroup().getLogicTimestamp().updateAndGet(current -> Math.max(current, initLogicTimestamp));
        }
    }

    private void createDelayDetectTable(String table) {
        OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(new String[0], new DelayDetectionListener());
        String createTableSQL = "create table if not exists " + table +
                " (source VARCHAR(256) primary key,real_timestamp varchar(26) NOT NULL,logic_timestamp BIGINT default 0)";
        OneTimeConnJob sqlJob = new OneTimeConnJob(createTableSQL, null, resultHandler, ds);
        runAndWait(sqlJob);
    }

    private Long queryLogicTimestamp(String table) {
        logicTimestamp = LOGIC_TIMESTAMP_UNSET;
        String sourceName = buildSourceName();
        String selectSQL = "select " + LOGIC_TIMESTAMP_COLUMN + " from " + table + " where source = '" + sourceName + "'";
        OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(new String[]{LOGIC_TIMESTAMP_COLUMN}, new LogicTimestampListener());
        OneTimeConnJob sqlJob = new OneTimeConnJob(selectSQL, null, resultHandler, ds);
        runAndWait(sqlJob);
        if (logicTimestamp == LOGIC_TIMESTAMP_UNSET) {
            return null;
        }
        return logicTimestamp;
    }

    private void runAndWait(OneTimeConnJob sqlJob) {
        lock.lock();
        try {
            isFinish = false;
        } finally {
            lock.unlock();
        }
        sqlJob.run();
        waitForFinish();
    }

    private void waitForFinish() {
        lock.lock();
        try {
            while (!isFinish) {
                finishCond.await();
            }
        } catch (InterruptedException e) {
            LOGGER.warn("test conn Interrupted:", e);
        } finally {
            lock.unlock();
        }
    }

    private String buildSourceName() {
        return "dble_" + ds.getDbGroupConfig().getName() + "_" + SystemConfig.getInstance().getInstanceName();
    }

    private void handleFinished() {
        lock.lock();
        try {
            isFinish = true;
            finishCond.signal();
        } finally {
            lock.unlock();
        }
    }

    private class DelayDetectionListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
        @Override
        public void onResult(SQLQueryResult<Map<String, String>> result) {
            successFlag.set(result.isSuccess());
            handleFinished();
        }
    }

    private class LogicTimestampListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
        @Override
        public void onResult(SQLQueryResult<Map<String, String>> result) {
            if (result.isSuccess()) {
                String value = result.getResult().get(LOGIC_TIMESTAMP_COLUMN);
                if (value != null) {
                    try {
                        logicTimestamp = Long.parseLong(value);
                    } catch (NumberFormatException e) {
                        LOGGER.warn("invalid logic_timestamp value [{}] in delay table for {}", value, ds.getName(), e);
                    }
                }
            }
            handleFinished();
        }
    }
}
