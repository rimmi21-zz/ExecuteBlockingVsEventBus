package aexp.sirius.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

public class MainVerticle extends AbstractVerticle {

    private WorkerExecutor executor=null ;
    @Override
    public void start(Future<Void> fut) {
        executor= vertx.createSharedWorkerExecutor("worker",20);
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        router.route("/").handler(routingContext -> {
            HttpServerResponse response = routingContext.response(); //created a PI for the server side response
            response
                    .putHeader("content-type", "text/html")
                    .end("Local host is listening at 8080</h1>");
        });
        //Trigerred through requests posted by jmeter for worker verticle (THROUGH EVENT BUS)
        router.post("/api/eventbus").handler(this::EmployeeData);
        ////Trigerred through requests posted by jmeter for EXECUTE BLOCKING CODE BLOCK
        router.post("/api/executeblocking").handler(this::ExecuteBlockingH);

        vertx.createHttpServer().requestHandler(router::accept) //**accept -> method from the router obj. And instructs vert.x to call the accept method of the router when it receives a request.
                .listen(
                        // Retrieve the port from the configuration,
                        // default to  port 8080
                        config().getInteger("http.port", 8080),
                        result -> {
                            if (result.succeeded()) {
                                //DEPLOYING THE WORKER VERTICLE
                                DeploymentOptions workver = new DeploymentOptions().setWorker(true).setInstances(20);
                                vertx.deployVerticle(Employee.class.getName(),workver);
                                fut.complete();
                            } else {
                                fut.fail(result.cause());
                            }
                        }
                );
    }

    private void ExecuteBlockingH(RoutingContext routingContext) {
        executor.executeBlocking(future -> {
            //vertx.executeBlocking(future -> {
            try {
                System.out.println("Initial Timestamp: " + System.currentTimeMillis());
                System.out.println("Before Thread.Sleep Inside Execute Blocking Handler" + Thread.currentThread().getName());
                Thread.sleep(1000);
                System.out.println("After Thread.sleep Inside Execute Blocking Handler" + Thread.currentThread().getName());
            } catch (Exception ignore) {
            }
            future.complete("done!");
        },false,asyncResult -> {
            System.out.println("Inside Async Result of Execute Blocking "+Thread.currentThread().getName());
            if (asyncResult.succeeded()) {
                JsonObject response=new JsonObject();
                response.put("status","success");
                System.out.println("Final Timestamp: " + System.currentTimeMillis());
                System.out.println("Inside Succeeded Async Result of Execute Blocking "+Thread.currentThread().getName());
                routingContext.response().setStatusCode(200).end(Json.encodePrettily(response));
            }
            else
            {
                JsonObject response=new JsonObject();
                response.put("status","failed");
                System.out.println("Inside Failed Async Result of Execute Blocking "+Thread.currentThread().getName());
                routingContext.response().setStatusCode(500).end(Json.encodePrettily(response));

            }});
    }


    private void EmployeeData(RoutingContext routingContext) {
        System.out.println(Thread.currentThread().getName());
        //SENDING REQUESTS TO WORKER VERTICLE
        vertx.eventBus().send("message.archive",routingContext.getBodyAsString(),asyncResult ->{
            System.out.println("Reply from Event Bus "+Thread.currentThread().getName());
            if (asyncResult.succeeded()) {
                System.out.println("Reply Success from Event Bus "+Thread.currentThread().getName());
                JsonObject response=new JsonObject();
                response.put("status","success");
                System.out.println("Final Timestamp: " + System.currentTimeMillis());

                routingContext.response().setStatusCode(200).end(Json.encodePrettily(response));
            }
            else
            {
                System.out.println("Reply Failed from Event Bus "+Thread.currentThread().getName());
                JsonObject response=new JsonObject();
                response.put("status","failed");
                routingContext.response().setStatusCode(500).end(Json.encodePrettily(response));

            }
        });
    }



}