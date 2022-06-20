package com.tugalsan.api.sql.db.server;

import java.sql.*;
import com.tugalsan.api.executable.client.*;
import com.tugalsan.api.pack.client.*;
import com.tugalsan.api.sql.conn.server.*;
import com.tugalsan.api.sql.resultset.server.*;
import com.tugalsan.api.sql.sanitize.server.*;
import com.tugalsan.api.sql.update.server.*;
import com.tugalsan.api.string.client.*;

public class TS_SQLDBUtils {

    public static boolean exists(TS_SQLConnAnchor anchor) {
        return TGS_StringUtils.isPresent(loc(anchor));
    }

    public static int createIfNotExists(TS_SQLConnAnchor anchor) {
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
            try {
                r.value0 = rs.str.get(0, 0);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return r.value0;
    }

    public static void meta(TS_SQLConnAnchor anchor, TGS_ExecutableType1<DatabaseMetaData> executor) {
        TS_SQLConnWalkUtils.con(anchor, con -> {
            try {
                executor.execute(con.getMetaData());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void catalog(TS_SQLConnAnchor anchor, TGS_ExecutableType1<TS_SQLResultSet> executor) {
        meta(anchor, meta -> {
            try ( var rs = meta.getCatalogs();) {
                executor.execute(new TS_SQLResultSet(rs));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void typeInfo(TS_SQLConnAnchor anchor, TGS_ExecutableType1<TS_SQLResultSet> executor) {
        meta(anchor, meta -> {
            try ( var rs = meta.getTypeInfo();) {
                executor.execute(new TS_SQLResultSet(rs));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static TS_SQLDBTypeInfo typeInfo(TS_SQLConnAnchor anchor) {
        TS_SQLDBTypeInfo r = new TS_SQLDBTypeInfo();
        TS_SQLDBUtils.typeInfo(anchor, tInfo -> {
            if (tInfo.row.isEmpty()) {
                return;
            }
            try {
                tInfo.row.scrll(0);
                r.TYPE_NAME = tInfo.str.get("TYPE_NAME");
                r.DATA_TYPE = tInfo.resultSet.getShort("DATA_TYPE");
                r.CREATE_PARAMS = tInfo.str.get("CREATE_PARAMS");
                r.NULLABLE = tInfo.resultSet.getInt("NULLABLE");
                r.CASE_SENSITIVE = tInfo.resultSet.getBoolean("CASE_SENSITIVE");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return r;
    }
}
