package io.vertx.starter;

import com.github.rjeschke.txtmark.Processor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class MainVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class);

  private static final String SQL_CREATE_PAGES_TABLE = "create table if not exists Pages (Id integer identity primary key, Name varchar(255) unique, Content clob)";
  private static final String SQL_GET_PAGE = "select Id, Content from Pages where Name = ?";
  private static final String SQL_CREATE_PAGE = "insert into Pages values (NULL, ?, ?)";
  private static final String SQL_SAVE_PAGE = "update Pages set Content = ? where Id = ?";
  private static final String SQL_ALL_PAGES = "select Name from Pages";
  private static final String SQL_DELETE_PAGE = "delete from Pages where Id = ?";

  private static final String EMPTY_PAGE_MARKDOWN = "# A new page\n" + "\n" + "Feel free to write in Markdown!\n";

  private final FreeMarkerTemplateEngine templateEngine = FreeMarkerTemplateEngine.create();
  private JDBCClient dbclient;

  @Override
  public void start(Future<Void> startFuture) throws Exception {
    Future<Void> steps = prepareDatabase().compose(v -> startHttpServer());
    steps.setHandler(ar -> {
      if (ar.succeeded()) {
        startFuture.complete();
      } else {
        startFuture.fail(ar.cause());
      }
    });
  }

  private Future<Void> prepareDatabase() {
    Future<Void> future = Future.future();

    // Shared connection among verticles known to the vertx instance.
    dbclient = JDBCClient.createShared(vertx, new JsonObject()
      .put("url", "jdbc:hsqldb:file:db/wiki") // JDBC url.
      .put("driver_class", "org.hsqldb.jdbcDriver") // Specific to the JDBC driver being used and points to the river class.
      .put("max_pool_size", 30));

    // Async getting connection.
    dbclient.getConnection(ar -> {
      if (ar.failed()) {
        LOGGER.error("Could not open a database connection", ar.cause());
        future.fail(ar.cause());
      } else {
        SQLConnection conn = ar.result();
        conn.execute(SQL_CREATE_PAGES_TABLE, create -> {
          conn.close(); // Release the connection. Otherwise the JDBC connection pool can eventually drain.
          if (create.failed()) {
            LOGGER.error("Database preparation error", create.cause());
            future.fail(create.cause());
          } else {
            future.complete();
          }
        });
      }
    });

    return future;
  }

  private Future<Void> startHttpServer() {
    Future<Void> future = Future.future();

    HttpServer server = vertx.createHttpServer();

    Router router = Router.router(vertx);
    router.get("/").handler(this::indexHandler);
    router.get("/wiki/:page").handler(this::pageRenderingHandler); // Url can be parametric.
    router.post().handler(BodyHandler.create()); // Makes all HTTP POST requests go through a first handler which automatically decodes the body from the requests, which can then be a Vert.x buffer objects.
    router.post("/save").handler(this::pageUpdateHandler);
    router.post("/create").handler(this::pageCreateHandler);
    router.post("/delete").handler(this::pageDeletionHandler);

    server.requestHandler(router::accept)
      .listen(8080, ar -> { // Async start server.
        if (ar.succeeded()) {
          LOGGER.info("HTTP server running on port 8080");
          future.complete();
        } else {
          LOGGER.error("Could not start a HTTP server", ar.cause());
          future.fail(ar.cause());
        }
      });

    return future;
  }

  private void indexHandler(RoutingContext context) {
    dbclient.getConnection(car -> {
      if (car.succeeded()) {
        SQLConnection conn = car.result();
        conn.query(SQL_ALL_PAGES, res -> {
          conn.close();
          if (res.succeeded()) {
            List<String> pages = res.result().getResults().stream() // Query result is JsonArray.
              .map(json -> json.getString(0))
              .sorted()
              .collect(Collectors.toList());
            context.put("title", "Wiki home"); // Put arbitrary key/value data that is then available fro templates, or chained router handlers.
            context.put("pages", pages);
            templateEngine.render(context, "templates", "/index.ftl", ar -> {
              if (ar.succeeded()) {
                context.response().putHeader("Content-Type", "text/html");
                context.response().end(ar.result());
              } else {
                context.fail(ar.cause()); // Returns a 500 error to the client.
              }
            });
          } else {
            context.fail(res.cause());
          }
        });
      } else {
        context.fail(car.cause());
      }
    });
  }

  private void pageRenderingHandler(RoutingContext context) {
    String page = context.request().getParam("page");

    dbclient.getConnection(car -> {
      if (car.succeeded()) {
        SQLConnection conn = car.result();
        conn.queryWithParams(SQL_GET_PAGE, new JsonArray().add(page), fetch -> { // Passing values to SQL query.
          conn.close();
          if (fetch.succeeded()) {
            JsonArray row = fetch.result().getResults().stream()
              .findFirst()
              .orElseGet(() -> new JsonArray().add(-1).add(EMPTY_PAGE_MARKDOWN));
            Integer id = row.getInteger(0);
            String rawContent = row.getString(1);

            context.put("title", page);
            context.put("id", id);
            context.put("newPage", fetch.result().getResults().size() == 0 ? "yes" : "no");
            context.put("rawContent", rawContent);
            context.put("content", Processor.process(rawContent));
            context.put("timestamp", new Date().toString());

            templateEngine.render(context, "templates", "/page.ftl", ar -> {
              if (ar.succeeded()) {
                context.response().putHeader("Content-Type", "text/html");
                context.response().end(ar.result());
              } else {
                context.fail(ar.cause());
              }
            });
          } else {
            context.fail(fetch.cause());
          }
        });
      } else {
        context.fail(car.cause());
      }
    });
  }

  private void pageCreateHandler(RoutingContext context) {
    String pageName = context.request().getParam("name");
    String location = "/wiki/" + pageName;
    if (pageName == null || pageName.isEmpty()) {
      location = "/";
    }
    context.response().setStatusCode(303); // Redirection.
    context.response().putHeader("Location", location);
    context.response().end();
  }

  private void pageUpdateHandler(RoutingContext context) {
    // Without a body handler within the router config chain these values won't be available.
    String id = context.request().getParam("id");
    String title = context.request().getParam("title");
    String markdown = context.request().getParam("markdown");
    boolean newPage = "yes".equals(context.request().getParam("newPage"));

    dbclient.getConnection(car -> {
      if (car.succeeded()) {
        SQLConnection conn = car.result();
        String sql = newPage ? SQL_CREATE_PAGE : SQL_SAVE_PAGE;
        JsonArray params = new JsonArray();
        if (newPage) {
          params.add(title).add(markdown);
        } else {
          params.add(markdown).add(id);
        }
        conn.updateWithParams(sql, params, res -> { // For insert, update, delete SQL queries.
          conn.close();
          if (res.succeeded()) {
            LOGGER.info("Update succeeded!");
            context.response().setStatusCode(303); // Redirect to the page when succeeded.
            context.response().putHeader("Location", "/wiki/" + title);
            context.response().end();
          } else {
            LOGGER.info("Update failed");
            context.fail(res.cause());
          }
        });
      } else {
        context.fail(car.cause());
      }
    });
  }

  private void pageDeletionHandler(RoutingContext context) {
    String id = context.request().getParam("id");
    dbclient.getConnection(car -> {
      if (car.succeeded()) {
        SQLConnection conn = car.result();
        conn.updateWithParams(SQL_DELETE_PAGE, new JsonArray().add(id), res -> {
          conn.close();
          if (res.succeeded()) {
            context.response()
              .setStatusCode(303) // Redirects to wiki index page.
              .putHeader("Location", "/")
              .end();
          } else {
            context.fail(res.cause());
          }
        });
      } else {
        context.fail(car.cause());
      }
    });
  }
}
