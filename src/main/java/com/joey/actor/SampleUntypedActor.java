package com.joey.actor;

import akka.actor.OneForOneStrategy;
import akka.actor.SupervisorStrategy;
import akka.actor.UntypedActor;
import akka.japi.Function;
import scala.concurrent.duration.Duration;

/**
 * @author joey.wen
 * @date 2015/8/17
 */
public class SampleUntypedActor extends UntypedActor {
    public SampleUntypedActor() {

    }

    private static SupervisorStrategy strategy = new OneForOneStrategy(10, Duration.create("1 minute"),
            new Function<Throwable, SupervisorStrategy.Directive>(){

                @Override
                public SupervisorStrategy.Directive apply(Throwable param) throws Exception {
                    if (param instanceof ArithmeticException) {
                        return resume();
                    } else if (param instanceof NullPointerException) {
                        return restart();
                    } else if (param instanceof IllegalArgumentException) {
                        return stop();
                    } else {
                        return escalate();
                    }
                }

                private SupervisorStrategy.Directive escalate() {
                    return null;
                }

                private SupervisorStrategy.Directive stop() {
                    return null;
                }

                private SupervisorStrategy.Directive restart() {
                    return null;
                }

                private SupervisorStrategy.Directive resume() {
                    return null;
                }
            });

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return strategy;
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof String) {
            String msg = (String) message;

        }
    }
}
