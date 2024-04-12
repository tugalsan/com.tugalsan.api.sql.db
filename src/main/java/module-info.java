module com.tugalsan.api.sql.db {
    requires java.sql;
    requires com.tugalsan.api.runnable;
    requires com.tugalsan.api.string;
    requires com.tugalsan.api.sql.sanitize;
    requires com.tugalsan.api.sql.resultset;
    requires com.tugalsan.api.sql.update;
    requires com.tugalsan.api.union;
    requires com.tugalsan.api.sql.conn;
    exports com.tugalsan.api.sql.db.server;
}
