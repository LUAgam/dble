package com.actiontech.dble.rwsplit;

import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.services.rwsplit.Callback;
import com.actiontech.dble.services.rwsplit.RWSplitHandler;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.actiontech.dble.singleton.RouteService;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

public class RWSplitNonBlockingSession {

    public static final Logger LOGGER = LoggerFactory.getLogger(RWSplitNonBlockingSession.class);

    private volatile BackendConnection conn;
    private final RWSplitService rwSplitService;
    private PhysicalDbGroup rwGroup;

    public RWSplitNonBlockingSession(RWSplitService service) {
        this.rwSplitService = service;
    }

    public void execute(boolean master, Callback callback) throws IOException {
        execute(master, null, callback);
    }

    public void execute(boolean master, byte[] originPacket, Callback callback) throws IOException {
        RWSplitHandler handler = new RWSplitHandler(rwSplitService, originPacket, callback);
        if (conn != null && !conn.isClosed()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("select bind conn[id={}]", conn.getId());
            }
            handler.execute(conn);
            return;
        }
        PhysicalDbInstance instance = rwGroup.select(master);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("route dbInstance{}", instance);
        }
        instance.getConnection(rwSplitService.getSchema(), handler, null, false);
    }

    public void executeHint(int sqlType, String sql, Callback callback) throws IOException {
        RWSplitHandler handler = new RWSplitHandler(rwSplitService, null, callback);
        if (conn != null && !conn.isClosed()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("select bind conn[id={}]", conn.getId());
            }
            handler.execute(conn);
            return;
        }
        try {
            PhysicalDbInstance dbInstance = RouteService.getInstance().routeRwSplit(sqlType, sql, rwSplitService);
            if (dbInstance == null) {
                return;
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("route sql {} to {}", sql, dbInstance);
            }
            dbInstance.getConnection(rwSplitService.getSchema(), handler, null, false);
        } catch (Exception e) {
            executeException(e, sql);
            return;
        }
    }

    private void executeException(Exception e, String sql) {
        sql = sql.length() > 1024 ? sql.substring(0, 1024) + "..." : sql;
        if (e instanceof SQLException) {
            SQLException sqlException = (SQLException) e;
            String msg = sqlException.getMessage();
            StringBuilder s = new StringBuilder();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(s.append(this).append(sql).toString() + " err:" + msg);
            }
            int vendorCode = sqlException.getErrorCode() == 0 ? ErrorCode.ER_PARSE_ERROR : sqlException.getErrorCode();
            String sqlState = StringUtil.isEmpty(sqlException.getSQLState()) ? "HY000" : sqlException.getSQLState();
            String errorMsg = msg == null ? sqlException.getClass().getSimpleName() : msg;
            rwSplitService.writeErrMessage(sqlState, errorMsg, vendorCode);
        } else {
            StringBuilder s = new StringBuilder();
            LOGGER.info(s.append(this).append(sql).toString() + " err:" + e.toString(), e);
            String msg = e.getMessage();
            rwSplitService.writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
        }
    }

    public void setRwGroup(PhysicalDbGroup rwGroup) {
        this.rwGroup = rwGroup;
    }

    public void bind(BackendConnection bindConn) {
        if (conn != null) {
            LOGGER.warn("last conn is remaining");
        }
        this.conn = bindConn;
    }

    public void unbindIfSafe() {
        if (rwSplitService.isAutocommit() && !rwSplitService.isLocked() &&
                !rwSplitService.isTxStart() &&
                !rwSplitService.isInLoadData() &&
                !rwSplitService.isInPrepare()) {
            this.conn.release();
            this.conn = null;
        }
    }

    public void unbind() {
        this.conn = null;
    }

    public void close(String reason) {
        if (conn != null) {
            conn.close(reason);
        }
    }

    public RWSplitService getService() {
        return rwSplitService;
    }
}
