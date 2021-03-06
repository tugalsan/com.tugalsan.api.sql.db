module com.tugalsan.api.sql.db {
    requires java.sql;
    requires com.tugalsan.api.executable;
    requires com.tugalsan.api.unsafe;
    requires com.tugalsan.api.pack;
    requires com.tugalsan.api.string;
    requires com.tugalsan.api.sql.sanitize;
    requires com.tugalsan.api.sql.resultset;
    requires com.tugalsan.api.sql.update;
    requires com.tugalsan.api.sql.conn;
    exports com.tugalsan.api.sql.db.server;
}
