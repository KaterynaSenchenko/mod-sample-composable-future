package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.sql.UpdateResult;
import org.folio.rest.jaxrs.model.Pet;
import org.folio.rest.jaxrs.model.PetsCollection;
import org.folio.rest.jaxrs.resource.Pets;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.interfaces.Results;
import org.folio.rest.utils.PgQuery;
import org.folio.rest.utils.PgTransaction;

import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PetsImpl implements Pets {

  private static final String HOMELESS_PETS_TABLE_NAME = "homeless_pets";
  private static final String ADOPTED_PETS_TABLE_NAME = "adopted_pets";
  private static final String[] ALL_FIELDS = {"*"};

  private final PostgresClient pgClient;

  public PetsImpl(Vertx vertx, String tenantId) {
    this.pgClient = PostgresClient.getInstance(vertx, tenantId);
  }

  @Override
  public void postPets(String lang, Pet entity, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      vertxContext.runOnContext(v -> {
        String id = UUID.randomUUID().toString();
        entity.setId(id);
        savePet(entity)
          .setHandler(res -> {
            if (res.succeeded()) {
              asyncResultHandler.handle(Future.succeededFuture(PostPetsResponse.respond201WithApplicationJson(entity, PostPetsResponse.headersFor201())));
            } else {
              asyncResultHandler.handle(Future.succeededFuture(PostPetsResponse.respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
            }
          });
      });
    } catch (Exception e) {
      asyncResultHandler.handle(Future.succeededFuture(PostPetsResponse.respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
    }
  }

  @Override
  public void getPets(String query, int offset, int limit, String lang, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      vertxContext.runOnContext(v -> {
        PgQuery.PgQueryBuilder queryBuilder = new PgQuery.PgQueryBuilder(ALL_FIELDS, HOMELESS_PETS_TABLE_NAME).query(query).offset(offset).limit(limit);
        Future.succeededFuture(queryBuilder)
          .compose(this::runGetQuery)
          .compose(this::parseGetResults)
          .setHandler(res -> {
            if (res.succeeded()) {
              asyncResultHandler.handle(Future.succeededFuture(GetPetsResponse.respond200WithApplicationJson(res.result())));
            } else {
              asyncResultHandler.handle(Future.succeededFuture(GetPetsResponse.respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
            }
          });
      });
    } catch (Exception e) {
      asyncResultHandler.handle(Future.succeededFuture(GetPetsResponse.respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
    }
  }

  private Future<Results<Pet>> runGetQuery(PgQuery.PgQueryBuilder queryBuilder) {
    Future<Results<Pet>> future = Future.future();
    try {
      PgQuery query = queryBuilder.build();
      pgClient.get(query.getTable(), Pet.class, query.getFields(), query.getCql(), true, false, future.completer());
    } catch (Exception e) {
      future.fail(e);
    }
    return future;
  }

  private Future<PetsCollection> parseGetResults(Results<Pet> resultSet) {
    List<Pet> petsList = resultSet.getResults();
    int totalRecords = petsList.size();
    PetsCollection petsCollection = new PetsCollection();
    petsCollection.setPets(petsList);
    petsCollection.setTotalRecords(totalRecords);
    return Future.succeededFuture(petsCollection);
  }

  @Override
  public void putPetsById(String id, String lang, Pet entity, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      vertxContext.runOnContext(v -> {
        entity.setId(id);
        updatePet(entity)
          .setHandler(res -> {
            if (res.failed()) {
              asyncResultHandler.handle(Future.succeededFuture(PutPetsByIdResponse.respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
            } else if (res.result().getUpdated() == 0) {
              asyncResultHandler.handle(Future.succeededFuture(PutPetsByIdResponse.respond404WithTextPlain(Response.Status.NOT_FOUND.getReasonPhrase())));
            } else {
              asyncResultHandler.handle(Future.succeededFuture(PutPetsByIdResponse.respond204()));
            }
          });
      });
    } catch (Exception e) {
      asyncResultHandler.handle(Future.succeededFuture(PutPetsByIdResponse.respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
    }
  }

  @Override
  public void getPetsById(String id, String lang, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      vertxContext.runOnContext(v -> {
        getPetById(HOMELESS_PETS_TABLE_NAME, id)
          .setHandler(res -> {
            if (res.failed()) {
              asyncResultHandler.handle(Future.succeededFuture(GetPetsByIdResponse.respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
            } else if (res.result().getResults().isEmpty()) {
              asyncResultHandler.handle(Future.succeededFuture(GetPetsByIdResponse.respond404WithTextPlain(Response.Status.NOT_FOUND.getReasonPhrase())));
            } else {
              asyncResultHandler.handle(Future.succeededFuture(GetPetsByIdResponse.respond200WithApplicationJson(res.result().getResults().get(0))));
            }
          });
      });
    } catch (Exception e) {
      asyncResultHandler.handle(Future.succeededFuture(GetPetsByIdResponse.respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
    }
  }

  @Override
  public void deletePetsById(String id, String lang, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      vertxContext.runOnContext(v -> {
        Future.succeededFuture(id)
          .compose(this::deletePetById)
          .setHandler(res -> {
            if (res.failed()) {
              asyncResultHandler.handle(Future.succeededFuture(DeletePetsByIdResponse.respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
            } else if (res.result().getUpdated() == 0) {
              asyncResultHandler.handle(Future.succeededFuture(DeletePetsByIdResponse.respond404WithTextPlain(Response.Status.NOT_FOUND.getReasonPhrase())));
            } else {
              asyncResultHandler.handle(Future.succeededFuture(DeletePetsByIdResponse.respond204()));
            }
          });
      });
    } catch (Exception e) {
      asyncResultHandler.handle(Future.succeededFuture(DeletePetsByIdResponse.respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
    }
  }

  @Override
  public void getPetsAdoptById(String id, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      vertxContext.runOnContext(v -> {
        getPetById(ADOPTED_PETS_TABLE_NAME, id)
          .setHandler(res -> {
            if (res.failed()) {
              asyncResultHandler.handle(Future.succeededFuture(GetPetsAdoptByIdResponse.respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
            } else if (res.result().getResults().isEmpty()) {
              asyncResultHandler.handle(Future.succeededFuture(GetPetsAdoptByIdResponse.respond404WithTextPlain(Response.Status.NOT_FOUND.getReasonPhrase())));
            } else {
              asyncResultHandler.handle(Future.succeededFuture(GetPetsAdoptByIdResponse.respond200WithApplicationJson(res.result().getResults().get(0))));
            }
          });
      });
    } catch (Exception e) {
      asyncResultHandler.handle(Future.succeededFuture(GetPetsAdoptByIdResponse.respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
    }
  }

  @Override
  public void postPetsAdoptById(String id, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      vertxContext.runOnContext(v -> {
        Pet entity = new Pet();
        entity.setId(id);
        PgTransaction<Pet> pgTransaction = new PgTransaction<>(entity);
        Future.succeededFuture(pgTransaction)
          .compose(this::startTx)
          .compose(this::readPetFields)
          .compose(this::deleteHomelessPet)
          .compose(this::adoptPet)
          .compose(this::endTx)
          .setHandler(res -> {
            if (res.failed()) {
              asyncResultHandler.handle(Future.succeededFuture(PostPetsAdoptByIdResponse.respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
            } else if (res.result().entity == null) {
              asyncResultHandler.handle(Future.succeededFuture(PostPetsAdoptByIdResponse.respond404WithTextPlain(Response.Status.NOT_FOUND.getReasonPhrase())));
            } else {
              asyncResultHandler.handle(Future.succeededFuture(PostPetsAdoptByIdResponse.respond201WithApplicationJson(res.result().entity)));
            }
          });
      });
    } catch (Exception e) {
      asyncResultHandler.handle(Future.succeededFuture(PostPetsAdoptByIdResponse.respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
    }
  }

  private Future<PgTransaction<Pet>> startTx(PgTransaction<Pet> tx) {
    Future<PgTransaction<Pet>> future = Future.future();
    pgClient.startTx(sqlConnection -> {
      tx.sqlConnection = sqlConnection;
      future.complete(tx);
    });
    return future;
  }

  private Future<PgTransaction<Pet>> readPetFields(PgTransaction<Pet> tx) {
    Future<PgTransaction<Pet>> future = Future.future();
    try {
      Criteria idCrit = constructCriteria("'id'", tx.entity.getId());
      pgClient.get(tx.sqlConnection, HOMELESS_PETS_TABLE_NAME, Pet.class, new Criterion(idCrit), true, false, reply -> {
        if (reply.failed()) {
          future.fail(reply.cause());
        } else if (reply.result().getResults().isEmpty()) {
          tx.entity = null;
          future.complete(tx);
        } else {
          List<Pet> pets = reply.result().getResults();
          tx.entity.setGenus(pets.get(0).getGenus());
          tx.entity.setQuantity(pets.get(0).getQuantity());
          future.complete(tx);
        }
      });
    } catch (Exception e) {
      future.fail(e);
    }
    return future;
  }

  private Future<PgTransaction<Pet>> deleteHomelessPet(PgTransaction<Pet> tx) {
    Future<PgTransaction<Pet>> future = Future.future();
    try {
      if (tx.entity != null) {
        Criteria idCrit = constructCriteria("'id'", tx.entity.getId());
        pgClient.delete(tx.sqlConnection, HOMELESS_PETS_TABLE_NAME, new Criterion(idCrit), reply -> {
          if (reply.succeeded()) {
            future.complete(tx);
          } else {
            future.fail(reply.cause());
          }
        });
      } else {
        future.complete(tx);
      }
    } catch (Exception e) {
      future.fail(e);
    }
    return future;
  }

  private Future<PgTransaction<Pet>> adoptPet(PgTransaction<Pet> tx) {
    Future<PgTransaction<Pet>> future = Future.future();
    try {
      if(tx.entity != null) {
        Pet entity = new Pet();
        entity.setGenus(tx.entity.getGenus());
        entity.setQuantity(tx.entity.getQuantity());
        pgClient.save(tx.sqlConnection, ADOPTED_PETS_TABLE_NAME, entity, postReply -> {
          if (postReply.succeeded()) {
            entity.setId(postReply.result());
          }
          tx.entity = entity;
          future.complete(tx);
        });
      } else {
        future.complete(tx);
      }
    } catch (Exception e) {
      pgClient.rollbackTx(tx.sqlConnection, reply -> future.fail(e));
    }
    return future;
  }

  private Future<PgTransaction<Pet>> endTx(PgTransaction<Pet> tx) {
    Future<PgTransaction<Pet>> future = Future.future();
    pgClient.endTx(tx.sqlConnection, v -> {
      future.complete(tx);
    });
    return future;
  }

  private Future<Pet> savePet(Pet pet) {
    Future<Pet> future = Future.future();
    try {
      pgClient.save(HOMELESS_PETS_TABLE_NAME, pet.getId(), pet, postReply -> {
        future.complete(pet);
      });
    } catch (Exception e) {
      future.fail(e);
    }
    return future;
  }

  private Future<UpdateResult> updatePet(Pet pet) {
    Future<UpdateResult> future = Future.future();
    try {
      Criteria idCrit = constructCriteria("'id'", pet.getId());
      pgClient.update(HOMELESS_PETS_TABLE_NAME, pet, new Criterion(idCrit), true, future.completer());
    } catch (Exception e) {
      future.fail(e);
    }
    return future;
  }

  private Future<Results<Pet>> getPetById(String table, String id) {
    Future<Results<Pet>> future = Future.future();
    try {
      Criteria idCrit = constructCriteria("'id'", id);
      pgClient.get(table, Pet.class, new Criterion(idCrit), true, false, future.completer());
    } catch (Exception e) {
      future.fail(e);
    }
    return future;
  }

  private Future<UpdateResult> deletePetById(String id) {
    Future<UpdateResult> future = Future.future();
    try {
      pgClient.delete(HOMELESS_PETS_TABLE_NAME, id, future.completer());
    } catch (Exception e) {
      future.fail(e);
    }
    return future;
  }

  /**
   * Builds criteria by which db result is filtered
   *
   * @param jsonbField - json key name
   * @param value      - value corresponding to the key
   * @return - Criteria object
   */
  private Criteria constructCriteria(String jsonbField, String value) {
    Criteria criteria = new Criteria();
    criteria.addField(jsonbField);
    criteria.setOperation("=");
    criteria.setValue(value);
    return criteria;
  }

}
