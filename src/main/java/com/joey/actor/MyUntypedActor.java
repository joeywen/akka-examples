package com.joey.actor;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

/**
 * @author joey.wen
 * @date 2015/8/17
 */
public class MyUntypedActor extends UntypedActor {
    LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    public void onReceive(Object message) throws Exception {
        if (message instanceof String) {
            log.info("Received String message: {}", message);
            getSender().tell(message, getSelf());
        } else
            unhandled(message);
    }
}
