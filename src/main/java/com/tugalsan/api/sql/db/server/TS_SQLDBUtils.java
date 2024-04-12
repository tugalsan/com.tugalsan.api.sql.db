package com.tugalsan.api.sql.db.server;

import java.sql.*;
import com.tugalsan.api.runnable.client.*;
import com.tugalsan.api.sql.conn.server.*;
import com.tugalsan.api.sql.resultset.server.*;
import com.tugalsan.api.sql.sanitize.server.*;
import com.tugalsan.api.sql.update.server.*;
import com.tugalsan.api.string.client.*;
import com.tugalsan.api.union.client.TGS_UnionExcuse;
import com.tugalsan.api.union.client.TGS_UnionExcuseVoid;

public class TS_SQLDBUtils {

    public static TGS_UnionExcuseVoid exists(TS_SQLConnAnchor anchor) {
        var u = loc(anchor);
        if (u.isExcuse()) {
            return u.toExcuseVoid();
        }
        if (TGS_StringUtils.isEmpty(u.value())) {
            TGS_UnionExcuseVoid.ofExcuse(TS_SQLDBUtils.class.getSimpleName(), "exists", "TGS_StringUtils.isEmpty(u.value())");
        }
        return TGS_UnionExcuseVoid.ofVoid();
    }

    public static TGS_UnionExcuse<TS_SQLConnStmtUpdateResult> createIfNotExists(TS_SQLConnAnchor anchor) {
        var dbName = anchor.config.dbName;
        TS_SQLSanitizeUtils.sanitize(dbName);
        var sql = "CREATE DATABASE IF NOT EXISTS ".concat(dbName);
        return TS_SQLUpdateStmtUtils.update(anchor.cloneItAs("mysql"), sql);
    }

    public static TGS_UnionExcuse<String> loc(TS_SQLConnAnchor achor) {
        var wrap = new Object() {
            TGS_UnionExcuse<String> u_rs_str_get = null;
            TGS_UnionExcuse<Boolean> u_rs_row_isEmpty = null;
        };
        var u = catalog(achor, rs -> {
            wrap.u_rs_row_isEmpty = rs.row.isEmpty();
            if (wrap.u_rs_row_isEmpty.isExcuse() || wrap.u_rs_row_isEmpty.value()) {
                return;
            }
            wrap.u_rs_str_get = rs.str.get(0, 0);
        });
        if (wrap.u_rs_row_isEmpty != null && wrap.u_rs_row_isEmpty.isExcuse()) {
            return wrap.u_rs_row_isEmpty.toExcuse();
        }
        if (wrap.u_rs_str_get != null && wrap.u_rs_str_get.isExcuse()) {
            return wrap.u_rs_str_get;
        }
        if (u.isExcuse()) {
            return u.toExcuse();
        }
        return wrap.u_rs_str_get;
    }

    public static TGS_UnionExcuseVoid meta(TS_SQLConnAnchor anchor, TGS_RunnableType1<DatabaseMetaData> executor) {
        var wrap = new Object() {
            TGS_UnionExcuseVoid result = null;
        };
        var u = TS_SQLConnWalkUtils.con(anchor, con -> {
            try {
                executor.run(con.getMetaData());
                wrap.result = TGS_UnionExcuseVoid.ofVoid();
            } catch (SQLException ex) {
                wrap.result = TGS_UnionExcuseVoid.ofExcuse(ex);
            }
        });
        if (wrap.result != null && wrap.result.isExcuse()) {
            return wrap.result;
        }
        return u;
    }

    public static TGS_UnionExcuseVoid catalog(TS_SQLConnAnchor anchor, TGS_RunnableType1<TS_SQLResultSet> executor) {
        var wrap = new Object() {
            TGS_UnionExcuseVoid result = null;
        };
        var u = meta(anchor, meta -> {
            try (var rs = meta.getCatalogs();) {
                executor.run(new TS_SQLResultSet(rs));
                wrap.result = TGS_UnionExcuseVoid.ofVoid();
            } catch (SQLException ex) {
                wrap.result = TGS_UnionExcuseVoid.ofExcuse(ex);
            }
        });
        if (wrap.result != null && wrap.result.isExcuse()) {
            return wrap.result;
        }
        return u;
    }

    public static TGS_UnionExcuseVoid typeInfo(TS_SQLConnAnchor anchor, TGS_RunnableType1<TS_SQLResultSet> executor) {
        var wrap = new Object() {
            TGS_UnionExcuseVoid result = null;
        };
        var u = meta(anchor, meta -> {
            try (var rs = meta.getTypeInfo();) {
                executor.run(new TS_SQLResultSet(rs));
                wrap.result = TGS_UnionExcuseVoid.ofVoid();
            } catch (SQLException ex) {
                wrap.result = TGS_UnionExcuseVoid.ofExcuse(ex);
            }
        });
        if (wrap.result != null && wrap.result.isExcuse()) {
            return wrap.result;
        }
        return u;
    }

    public static TGS_UnionExcuse<TS_SQLDBTypeInfo> typeInfo(TS_SQLConnAnchor anchor) {
        var r = new TS_SQLDBTypeInfo();
        var wrap = new Object() {
            TGS_UnionExcuse<Boolean> u_empty = null;
            TGS_UnionExcuseVoid u_scroll = null;
            TGS_UnionExcuse<String> u_TYPE_NAME = null;
            TGS_UnionExcuseVoid u_DATA_TYPE = null;
            TGS_UnionExcuse<String> u_CREATE_PARAMS = null;
            TGS_UnionExcuseVoid u_NULLABLE = null;
            TGS_UnionExcuseVoid u_CASE_SENSITIVE = null;
        };
        var u = typeInfo(anchor, tInfo -> {
            wrap.u_empty = tInfo.row.isEmpty();
            if (wrap.u_empty.isExcuse() || wrap.u_empty.value()) {
                return;
            }

            wrap.u_scroll = tInfo.row.scrll(0);
            if (wrap.u_scroll.isExcuse()) {
                return;
            }

            wrap.u_TYPE_NAME = tInfo.str.get("TYPE_NAME");
            if (wrap.u_TYPE_NAME.isExcuse()) {
                return;
            }
            r.TYPE_NAME = wrap.u_TYPE_NAME.value();

            try {
                r.DATA_TYPE = tInfo.resultSet.getShort("DATA_TYPE");
                wrap.u_DATA_TYPE = TGS_UnionExcuseVoid.ofVoid();
            } catch (SQLException ex) {
                wrap.u_NULLABLE = TGS_UnionExcuseVoid.ofExcuse(ex);
                return;
            }

            wrap.u_CREATE_PARAMS = tInfo.str.get("CREATE_PARAMS");
            if (wrap.u_CREATE_PARAMS.isExcuse()) {
                return;
            }
            r.CREATE_PARAMS = wrap.u_CREATE_PARAMS.value();

            try {
                r.NULLABLE = tInfo.resultSet.getInt("NULLABLE");
                wrap.u_NULLABLE = TGS_UnionExcuseVoid.ofVoid();
            } catch (SQLException ex) {
                wrap.u_NULLABLE = TGS_UnionExcuseVoid.ofExcuse(ex);
                return;
            }

            try {
                r.CASE_SENSITIVE = tInfo.resultSet.getBoolean("CASE_SENSITIVE");
                wrap.u_CASE_SENSITIVE = TGS_UnionExcuseVoid.ofVoid();
            } catch (SQLException ex) {
                wrap.u_CASE_SENSITIVE = TGS_UnionExcuseVoid.ofExcuse(ex);
                return;
            }
        });
        if (wrap.u_empty.isExcuse()) {
            return wrap.u_empty.toExcuse();
        }
        if (wrap.u_scroll.isExcuse()) {
            return wrap.u_scroll.toExcuse();
        }
        if (wrap.u_TYPE_NAME.isExcuse()) {
            return wrap.u_TYPE_NAME.toExcuse();
        }
        if (wrap.u_DATA_TYPE.isExcuse()) {
            return wrap.u_DATA_TYPE.toExcuse();
        }
        if (wrap.u_CREATE_PARAMS.isExcuse()) {
            return wrap.u_CREATE_PARAMS.toExcuse();
        }
        if (wrap.u_NULLABLE.isExcuse()) {
            return wrap.u_NULLABLE.toExcuse();
        }
        if (wrap.u_CASE_SENSITIVE.isExcuse()) {
            return wrap.u_CASE_SENSITIVE.toExcuse();
        }
        if (u.isExcuse()) {
            return u.toExcuse();
        }
        return TGS_UnionExcuse.of(r);
    }
}
