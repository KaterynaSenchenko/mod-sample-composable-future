package org.folio.rest.utils;

import io.vertx.core.AsyncResult;
import io.vertx.ext.sql.SQLConnection;
import org.folio.rest.tools.utils.OutStream;

public class PgTransaction<T> {
    public T entity;
    public AsyncResult<SQLConnection> sqlConnection;
    public OutStream stream;
    public AsyncResult<String> location;

    public PgTransaction(T entity) {
        this.entity = entity;
    }
}
