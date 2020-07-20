package aexp.sirius.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;

public class Employee extends AbstractVerticle {

    @Override
    //Future<void> so that there is no return in execution
    public void start(Future<Void> fut) {
        //CONSUMING REQUESTS FROM MAIN VERTICLE
        vertx.eventBus().consumer("message.archive", this::archive);
        System.out.println("Instances printing");
        fut.complete();
    }
    private <T> void archive(Message<T> tMessage) {
        String body = tMessage.body().toString();
        System.out.println("Initial Timestamp: " + System.currentTimeMillis());
        System.out.println("Before Thread.Sleep archive" + Thread.currentThread().getName());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("After Thread.sleep archive" + Thread.currentThread().getName());
        System.out.println("Final Timestamp: " + System.currentTimeMillis());
        tMessage.reply("success");
    }
}