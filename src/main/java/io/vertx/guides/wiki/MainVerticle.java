package io.vertx.guides.wiki;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.guides.wiki.database.WikiDatabaseVerticle;

public class MainVerticle extends AbstractVerticle {

  @Override
  public void start(Future<Void> startFuture) throws Exception {
    Future<String> dbVerticleDeployment = Future.future();
    // Async deploy. Verticle gets an ID when successfully deployed.
    vertx.deployVerticle(new WikiDatabaseVerticle(), dbVerticleDeployment.completer());

    dbVerticleDeployment.compose(id -> { // Sequential composition.
      Future<String> httpVerticleDeployment = Future.future();
      vertx.deployVerticle("HttpServerVerticle", // A class name as a string to specify a verticle.
        new DeploymentOptions().setInstances(2), // Multiple verticles can share the same TCP ports.
        httpVerticleDeployment.completer());
      return httpVerticleDeployment;
    }).setHandler(ar -> {
      if (ar.succeeded()) {
        startFuture.complete();
      } else {
        startFuture.fail(ar.cause());
      }
    });
  }
}
