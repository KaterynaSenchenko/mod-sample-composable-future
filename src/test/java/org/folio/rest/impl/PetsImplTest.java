package org.folio.rest.impl;

import io.restassured.RestAssured;
import io.restassured.http.Header;
import io.restassured.response.Response;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.Pet;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import javax.ws.rs.core.MediaType;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class PetsImplTest {

  private static final String PETS_PATH = "/pets";
  private static final String HTTP_PORT = "http.port";
  private static final String TENANT = "diku";
  private static final String TABLE_NAME = "pets";
  private static final Header TENANT_HEADER = new Header(RestVerticle.OKAPI_HEADER_TENANT, TENANT);

  private static Vertx vertx;
  private static int port;

  private static final JsonObject PET1 = new JsonObject()
    .put("genus", "Canis")
    .put("quantity", 30);

  private static final JsonObject PET2 = new JsonObject()
    .put("genus", "Panthera")
    .put("quantity", 50);

  private static final JsonObject PET3 = new JsonObject()
    .put("genus", "Boas")
    .put("quantity", 20);

  @org.junit.Rule
  public Timeout timeout = Timeout.seconds(180);

  @BeforeClass
  public static void setUpClass(final TestContext context) throws Exception {
    Async async = context.async();
    vertx = Vertx.vertx();
    port = NetworkUtils.nextFreePort();
    PostgresClient.setIsEmbedded(true);
    PostgresClient.getInstance(vertx).startEmbeddedPostgres();
    TenantClient tenantClient = new TenantClient("localhost", port, TENANT, "diku");
    final DeploymentOptions options = new DeploymentOptions().setConfig(new JsonObject().put(HTTP_PORT, port));
    vertx.deployVerticle(RestVerticle.class.getName(), options, res -> {
      try {
        tenantClient.postTenant(null, res2 -> {
          async.complete();
        });
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }

  @AfterClass
  public static void tearDownClass(final TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> {
      PostgresClient.stopEmbeddedPostgres();
      async.complete();
    }));
  }

  @Before
  public void clearPets(TestContext context) throws Exception {
    PostgresClient.getInstance(vertx, TENANT).delete(TABLE_NAME, new Criterion(), event -> {
      if (event.failed()) {
        context.fail(event.cause());
      }
    });
  }

  @Test
  public void shouldReturnEmptyListIfNoPetsExist(final TestContext context) {
    RestAssured.given()
      .port(port)
      .header(TENANT_HEADER)
      .when()
      .get(PETS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(0))
      .body("pets", empty());
  }

  @Test
  public void shouldReturnAllPets(final TestContext context) {
    List<JsonObject> petsToPost = Arrays.asList(PET1, PET2, PET3);
    for (JsonObject pet : petsToPost) {
      RestAssured.given()
        .port(port)
        .contentType(MediaType.APPLICATION_JSON)
        .header(TENANT_HEADER)
        .body(pet.toString())
        .when()
        .post(PETS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }

    Object[] petsGenuses = petsToPost.stream().map(r -> r.getString("genus")).toArray();
    RestAssured.given()
      .port(port)
      .header(TENANT_HEADER)
      .when()
      .get(PETS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(petsToPost.size()))
      .body("pets*.genus", contains(petsGenuses));
  }

  @Test
  public void shouldReturnBadRequestOnPostWhenNoPetPassedInBody(final TestContext context) {
    RestAssured.given()
      .port(port)
      .contentType(MediaType.APPLICATION_JSON)
      .header(TENANT_HEADER)
      .body(new JsonObject().toString())
      .when()
      .post(PETS_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldCreatePet(final TestContext context) {
    RestAssured.given()
      .port(port)
      .contentType(MediaType.APPLICATION_JSON)
      .header(TENANT_HEADER)
      .body(PET1.toString())
      .when()
      .post(PETS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("genus", is(PET1.getString("genus")))
      .body("quantity", is(PET1.getInteger("quantity")));
  }

  @Test
  public void shouldReturnBadRequestOnPutWhenNoPetPassedInBody(final TestContext context) {
    RestAssured.given()
      .port(port)
      .contentType(MediaType.APPLICATION_JSON)
      .header(TENANT_HEADER)
      .pathParam("id", "nonexistent_pet_id")
      .body(new JsonObject().toString())
      .when()
      .put(PETS_PATH + "/{id}")
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldReturnNotFoundWhenPetDoesNotExist(final TestContext context) {
    RestAssured.given()
      .port(port)
      .contentType(MediaType.APPLICATION_JSON)
      .header(TENANT_HEADER)
      .pathParam("id", "nonexistent_pet_id")
      .body(PET2.toString())
      .when()
      .put(PETS_PATH + "/{id}")
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldUpdateExistingPet(final TestContext context) {
    Response response = RestAssured.given()
      .port(port)
      .contentType(MediaType.APPLICATION_JSON)
      .header(TENANT_HEADER)
      .body(PET3.toString())
      .when()
      .post(PETS_PATH);

    Assert.assertThat(response.statusCode(), is(HttpStatus.SC_CREATED));
    Pet createdPet = response.body().as(Pet.class);
    JsonObject petToUpdate = new JsonObject()
      .put("id", createdPet.getId())
      .put("genus", "Panthera")
      .put("quantity", 10);

    RestAssured.given()
      .port(port)
      .contentType(MediaType.APPLICATION_JSON)
      .header(TENANT_HEADER)
      .pathParam("id", createdPet.getId())
      .body(petToUpdate.toString())
      .when()
      .put(PETS_PATH + "/{id}")
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);

    RestAssured.given()
      .port(port)
      .contentType(MediaType.APPLICATION_JSON)
      .header(TENANT_HEADER)
      .pathParam("id", createdPet.getId())
      .when()
      .get(PETS_PATH + "/{id}")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(createdPet.getId()))
      .body("genus", is("Panthera"))
      .body("quantity", is(10));
  }

  @Test
  public void shouldReturnNotFoundOnGetPetByIdWhenPetDoesNotExist(final TestContext context) {
    RestAssured.given()
      .port(port)
      .contentType(MediaType.APPLICATION_JSON)
      .header(TENANT_HEADER)
      .pathParam("id", "nonexistent_pet_id")
      .when()
      .get(PETS_PATH + "/{id}")
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnPetById(final TestContext context) {

    Response response = RestAssured.given()
      .port(port)
      .contentType(MediaType.APPLICATION_JSON)
      .header(TENANT_HEADER)
      .body(PET1.toString())
      .when()
      .post(PETS_PATH);
    Assert.assertThat(response.statusCode(), is(HttpStatus.SC_CREATED));
    Pet createdPet = response.body().as(Pet.class);

    RestAssured.given()
      .port(port)
      .contentType(MediaType.APPLICATION_JSON)
      .header(TENANT_HEADER)
      .pathParam("id", createdPet.getId())
      .when()
      .get(PETS_PATH + "/{id}")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(createdPet.getId()));

  }
}
