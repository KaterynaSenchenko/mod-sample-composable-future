package org.folio.rest.utils;

import io.vertx.core.Future;

public abstract class TransactionExecutor<T> {

  public Future<PgTransaction<T>> executeTransaction(PgTransaction<T> tx) {
    Future<PgTransaction<T>> future = Future.future();
    Future.succeededFuture(tx)
      .compose(this::startTx)
      .compose(this::runInTransaction)
      .compose(this::endTx)
      .setHandler(res -> {
        if (res.failed()) {
          tx.pgClient.rollbackTx(tx.sqlConnection, done -> future.fail(res.cause()));
        } else {
          future.complete(tx);
        }
      });
    return future;
  }

  public abstract Future<PgTransaction<T>> runInTransaction(PgTransaction<T> tx);

  private Future<PgTransaction<T>> startTx(PgTransaction<T> tx) {
    Future<PgTransaction<T>> future = Future.future();
    tx.pgClient.startTx(sqlConnection -> {
      tx.sqlConnection = sqlConnection;
      future.complete(tx);
    });
    return future;
  }

  private Future<PgTransaction<T>> endTx(PgTransaction<T> tx) {
    Future<PgTransaction<T>> future = Future.future();
    tx.pgClient.endTx(tx.sqlConnection, v -> {
      future.complete(tx);
    });
    return future;
  }

}
