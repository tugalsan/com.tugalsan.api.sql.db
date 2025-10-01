package com.tugalsan.api.sql.db.server;

import module com.tugalsan.api.tuple;
import module com.tugalsan.api.sql.conn;
import module com.tugalsan.api.sql.resultset;
import module com.tugalsan.api.sql.sanitize;
import module com.tugalsan.api.sql.update;
import module com.tugalsan.api.string;
import module com.tugalsan.api.function;
import module java.sql;

public class TS_SQLDBUtils {

    private TS_SQLDBUtils() {

    }

    public static boolean exists(TS_SQLConnAnchor anchor) {
        return TGS_StringUtils.cmn().isPresent(loc(anchor));
    }

    public static TS_SQLConnStmtUpdateResult createIfNotExists(TS_SQLConnAnchor anchor) {
        var dbName = anchor.config.dbName;
        TS_SQLSanitizeUtils.sanitize(dbName);
        var sql = "CREATE DATABASE IF NOT EXISTS ".concat(dbName);
        return TS_SQLUpdateStmtUtils.update(anchor.cloneItAs("mysql"), sql);
    }

    public static String loc(TS_SQLConnAnchor achor) {
        TGS_Tuple1<String> r = new TGS_Tuple1();
        catalog(achor, rs -> {
            if (rs.row.isEmpty()) {
                return;
            }
            TGS_FuncMTCUtils.run(() -> r.value0 = rs.str.get(0, 0));
        });
        return r.value0;
    }

    public static void meta(TS_SQLConnAnchor anchor, TGS_FuncMTU_In1<DatabaseMetaData> executor) {
        anchor.use(con -> {
            TGS_FuncMTCUtils.run(() -> {
                executor.run(con.getMetaData());
            });
        });
    }

    public static void catalog(TS_SQLConnAnchor anchor, TGS_FuncMTU_In1<TS_SQLResultSet> executor) {
        meta(anchor, meta -> {
            TGS_FuncMTCUtils.run(() -> {
                try (var rs = meta.getCatalogs();) {
                    executor.run(new TS_SQLResultSet(rs));
                }
            });
        });
    }

    public static void typeInfo(TS_SQLConnAnchor anchor, TGS_FuncMTU_In1<TS_SQLResultSet> executor) {
        meta(anchor, meta -> {
            TGS_FuncMTCUtils.run(() -> {
                try (var rs = meta.getTypeInfo();) {
                    executor.run(new TS_SQLResultSet(rs));
                }
            });
        });
    }

    public static TS_SQLDBTypeInfo typeInfo(TS_SQLConnAnchor anchor) {
        var r = new TS_SQLDBTypeInfo();
        typeInfo(anchor, tInfo -> {
            if (tInfo.row.isEmpty()) {
                return;
            }
            TGS_FuncMTCUtils.run(() -> {
                tInfo.row.scrll(0);
                r.TYPE_NAME = tInfo.str.get("TYPE_NAME");
                r.DATA_TYPE = tInfo.resultSet.getShort("DATA_TYPE");
                r.CREATE_PARAMS = tInfo.str.get("CREATE_PARAMS");
                r.NULLABLE = tInfo.resultSet.getInt("NULLABLE");
                r.CASE_SENSITIVE = tInfo.resultSet.getBoolean("CASE_SENSITIVE");
            });
        });
        return r;
    }
}
