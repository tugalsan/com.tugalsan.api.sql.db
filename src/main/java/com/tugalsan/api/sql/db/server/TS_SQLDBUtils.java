package com.tugalsan.api.sql.db.server;

import java.sql.*;
import com.tugalsan.api.executable.client.*;
import com.tugalsan.api.pack.client.*;
import com.tugalsan.api.sql.conn.server.*;
import com.tugalsan.api.sql.resultset.server.*;
import com.tugalsan.api.sql.sanitize.server.*;
import com.tugalsan.api.sql.update.server.*;
import com.tugalsan.api.string.client.*;
import com.tugalsan.api.unsafe.client.*;

public class TS_SQLDBUtils {

    public static boolean exists(TS_SQLConnAnchor anchor) {
        return TGS_StringUtils.isPresent(loc(anchor));
    }

    public static TS_SQLConnStmtUpdateResult createIfNotExists(TS_SQLConnAnchor anchor) {
        var dbName = anchor.config.dbName;
        TS_SQLSanitizeUtils.sanitize(dbName);
        var sql = "CREATE DATABASE IF NOT EXISTS ".concat(dbName);
        return TS_SQLUpdateStmtUtils.update(anchor.cloneItAs("mysql"), sql);
    }

    public static String loc(TS_SQLConnAnchor achor) {
        TGS_Pack1<String> r = new TGS_Pack1();
        catalog(achor, rs -> {
            if (rs.row.isEmpty()) {
                return;
            }
            TGS_UnSafe.execute(() -> r.value0 = rs.str.get(0, 0));
        });
        return r.value0;
    }

    public static void meta(TS_SQLConnAnchor anchor, TGS_ExecutableType1<DatabaseMetaData> executor) {
        TS_SQLConnWalkUtils.con(anchor, con -> {
            TGS_UnSafe.execute(() -> {
                executor.execute(con.getMetaData());
            });
        });
    }

    public static void catalog(TS_SQLConnAnchor anchor, TGS_ExecutableType1<TS_SQLResultSet> executor) {
        meta(anchor, meta -> {
            TGS_UnSafe.execute(() -> {
                try ( var rs = meta.getCatalogs();) {
                    executor.execute(new TS_SQLResultSet(rs));
                }
            });
        });
    }

    public static void typeInfo(TS_SQLConnAnchor anchor, TGS_ExecutableType1<TS_SQLResultSet> executor) {
        meta(anchor, meta -> {
            TGS_UnSafe.execute(() -> {
                try ( var rs = meta.getTypeInfo();) {
                    executor.execute(new TS_SQLResultSet(rs));
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
            TGS_UnSafe.execute(() -> {
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
