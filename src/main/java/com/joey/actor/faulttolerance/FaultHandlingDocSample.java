package com.joey.actor.faulttolerance;

import akka.actor.*;
import akka.dispatch.Mapper;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Function;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static akka.japi.Util.classTag;

/**
 * @author joey.wen
 * @date 2015/8/24
 */
public class FaultHandlingDocSample {

    public static void main(String[] args) {
        Config config = ConfigFactory.parseString("akka.loglevel = DEBUG \n" + "akka.actor.debug.lifecycle = on");
        ActorSystem system = ActorSystem.create("faulthandling", config);

        ActorRef worker = system.actorOf(Props.create(Worker.class), "worker");
        ActorRef listener = system.actorOf(Props.create(Listener.class), "listener");

        worker.tell(WorkerApi.Start, listener);
    }

    public static class Listener extends UntypedActor {

        final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

        @Override
        public void preStart() throws Exception {
            getContext().setReceiveTimeout(Duration.create(5000, TimeUnit.MILLISECONDS));
        }

        @Override
        public void onReceive(Object msg) throws Exception {
            log.debug("received message {}", msg);
            if (msg instanceof WorkerApi.Progress) {
                WorkerApi.Progress progress = (WorkerApi.Progress) msg;
                log.info("Current progress: {} %", progress.percent);
                if (progress.percent >= 100.0) {
                    log.info("That's all, shutting down");
                    getContext().system().shutdown();
                }
            } else if (msg == ReceiveTimeout.getInstance()) {
                // No progress within 15 seconds, ServiceUnavailable
                log.error("Shutting down due to unavailable service");
                getContext().system().shutdown();
            } else {
                unhandled(msg);
            }
        }
    }

    public interface WorkerApi {
        public static final Object Start = "Start";
        public static final Object Do = "Do";

        public static class Progress {
            public final double percent;

            public Progress(double percent) {
                this.percent = percent;
            }

            @Override
            public String toString() {
                return String.format("%s(%s)", getClass().getSimpleName(), percent);
            }
        }
    }

    public static class Worker extends UntypedActor {
        final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
        final Timeout askTimeout = new Timeout(Duration.create(5000, TimeUnit.MILLISECONDS));


        // the sender of the initial Start message will continuously be notified about progress
        ActorRef progressListener;
        final ActorRef counterService = getContext().actorOf(Props.create(CounterService.class), "counter");

        final int totalCount = 51;

        // Stop the CounterService child if it throws ServiceUnavailable
        private static SupervisorStrategy strategy = new OneForOneStrategy(-1,
                Duration.Inf(), new Function<Throwable, SupervisorStrategy.Directive>() {
            @Override
            public SupervisorStrategy.Directive apply(Throwable t) {
                if (t instanceof CounterServiceApi.ServiceUnavailable) {
                    return SupervisorStrategy.stop();
                } else {
                    return SupervisorStrategy.escalate();
                }
            }
        });

        @Override
        public void onReceive(Object msg) throws Exception {
            log.debug("received message {}", msg);
            if (msg.equals(WorkerApi.Start) && progressListener == null) {
                progressListener = getSender();
                getContext().system().scheduler().schedule(
                        Duration.Zero(), Duration.create(1, "second"), getSelf(), WorkerApi.Do,
                        getContext().dispatcher(), null
                );
            } else if (msg.equals(WorkerApi.Do)) {
                counterService.tell(new CounterServiceApi.Increment(1), getSelf());
                counterService.tell(new CounterServiceApi.Increment(1), getSelf());
                counterService.tell(new CounterServiceApi.Increment(1), getSelf());
                // Send current progress to the initial sender

                Patterns.pipe(Patterns.ask(counterService, CounterServiceApi.GetCurrentCount, askTimeout)
                        .mapTo(classTag(CounterServiceApi.CurrentCount.class))
                        .map(new Mapper<CounterServiceApi.CurrentCount, WorkerApi.Progress>() {
                            public WorkerApi.Progress apply(CounterServiceApi.CurrentCount c) {
                                return new WorkerApi.Progress(100.0 * c.count / totalCount);
                            }
                        }, getContext().dispatcher()), getContext().dispatcher())
                        .to(progressListener);
            } else {
                unhandled(msg);
            }
        }
    }


    public interface CounterServiceApi {
        public static final Object GetCurrentCount = "GetCurrentCount";

        public static class CurrentCount {
            public final String key;
            public final long count;

            public CurrentCount(long count, String key) {
                this.count = count;
                this.key = key;
            }

            @Override
            public String toString() {
                return String.format("%s(%s, %s)", getClass().getSimpleName(), key, count);
            }
        }


        public static class Increment {
            public final long n;

            public Increment(long n) {
                this.n = n;
            }

            public String toString() {
                return String.format("%s(%s)", getClass().getSimpleName(), n);
            }
        }



        public static class ServiceUnavailable extends RuntimeException {
            private static final long serviceVersionUID = 1L;

            public ServiceUnavailable(String msg) {
                super(msg);
            }
        }

    }

    public static class CounterService extends UntypedActor {

        static final Object Reconnect = "Reconnect";

        private static class SenderMsgPair {
            final ActorRef sender;
            final Object msg;

            public SenderMsgPair(ActorRef sender, Object msg) {
                this.sender = sender;
                this.msg = msg;
            }
        }


        final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
        final String key = getSelf().path().name();

        ActorRef storage;
        ActorRef counter;
        final List<SenderMsgPair> backlog = new ArrayList<SenderMsgPair>();
        final int MAX_BACKLOG = 100000;

        // Restart the storage child when StorageException is thrown.
        // After 3 restarts within 5 seconds it will be stopped.
        private static SupervisorStrategy strategy = new OneForOneStrategy(3, Duration.create(5000, TimeUnit.MILLISECONDS),
                new Function<Throwable, SupervisorStrategy.Directive>() {

                    @Override
                    public SupervisorStrategy.Directive apply(Throwable param) throws Exception {
                        if (param instanceof  StoreApi.StorageException) {
                            System.out.println("restart");
                            return SupervisorStrategy.restart();
                        } else {
                            System.out.println(" escalate ");
                            return SupervisorStrategy.escalate();
                        }
                    }
                }
        );

        @Override
        public SupervisorStrategy supervisorStrategy() {
            return strategy;
        }


        @Override
        public void preStart() throws Exception {
            initStorage();
        }

        /**
         * The child storage is restarted in case of failure, but after 3 restarts, and still failing it will be stopped.
         * Better to back-off than continuously failing. When it has been stopped we will schedule a Reconnect after a delay.
         * Watch the child so we receive Terminated message when it has been terminated.
         */
        void initStorage() {
            storage = getContext().watch(getContext().actorOf(Props.create(Storage.class), "storage"));

            // Tell the counter, if any, to use the new storage.
            if (counter != null) {
                counter.tell(new CounterApi.UseStorage(storage), getSelf());
            }

            // We need the initial value to be able to operate
            storage.tell(new StoreApi.Get(key), getSelf());
        }

        @Override
        public void onReceive(Object message) throws Exception {
            log.debug("received message {}", message);
            if (message instanceof StoreApi.Entry && ((StoreApi.Entry) message).key.equals(key) && counter == null) {
                // Reply from storage of the initial value, now we can create the Counter
                final long value = ((StoreApi.Entry) message).value;
                counter = getContext().actorOf(Props.create(Counter.class, key, value));

                // Tell the counter to use current storage
                counter.tell(new CounterApi.UseStorage(storage), getSelf());

                // add send the buffered backlog to the counter
                for (SenderMsgPair each : backlog) {
                    counter.tell(each.msg, each.sender);
                }

                backlog.clear();
            } else if (message instanceof CounterServiceApi.Increment) {
                forwardOrPlaceInBacklog(message);
            } else if (message.equals(CounterServiceApi.GetCurrentCount)) {
                forwardOrPlaceInBacklog(message);
            } else if (message instanceof Terminated) {
                // after 3 restarts the storage child is stopped.
                // We receive Terminated because we watch the child, see initStorage.

                storage = null;
                //Tell the counter that there is no storage for the moment
                counter.tell(new CounterApi.UseStorage(null), getSelf());
                //Try to re-establish storage after while
                getContext().system().scheduler().scheduleOnce(Duration.create(10000, TimeUnit.MILLISECONDS), getSelf(),
                 Reconnect, getContext().dispatcher(), null);
            } else if (message.equals(Reconnect) ) {
                // Re-establish storage after the scheduled delay
                initStorage();
            } else {
                unhandled(message);
            }
        }

        void forwardOrPlaceInBacklog(Object msg) {
            // We need the initial value from storage before we can start delegate to the counter.
            // Before that we place messages in a backlog, to be sent to the counter when it is initialized
            if (counter == null) {
                if (backlog.size() >= MAX_BACKLOG) {
                    throw new CounterServiceApi.ServiceUnavailable("CounterService not available, " + " lack of initail value");
                }

                backlog.add(new SenderMsgPair(getSender(), msg));
            } else {
                counter.forward(msg, getContext());
            }
        }

    }

    public interface CounterApi {
        public static class UseStorage{
            public final ActorRef storage;

            public UseStorage(ActorRef storage) {
                this.storage = storage;
            }

            @Override
            public String toString() {
                return String.format("%s(%s)", getClass().getSimpleName(), storage);
            }
        }
    }

    /**
     * The in memory count variable that will send current value to the Storage, if there is any storage available at the moment
     */
    public static class Counter extends UntypedActor {

        final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
        final String key;
        long count;
        ActorRef storage;

        public Counter(String key, long count) {
            this.key = key;
            this.count = count;
        }

        @Override
        public void onReceive(Object message) throws Exception {
            log.debug("received message {}", message);
            if (message instanceof CounterApi.UseStorage) {
                storage = ((CounterApi.UseStorage) message).storage;
                storeCount();
            } else if (message instanceof CounterServiceApi.Increment) {
                count += ((CounterServiceApi.Increment) message).n;
                storeCount();
            } else if (message.equals(CounterServiceApi.GetCurrentCount)) {
                getSender().tell(new CounterServiceApi.CurrentCount(count, key), getSelf());
            } else {
                unhandled(message);
            }
        }

        void storeCount () {
            // Delegate dangerous work, to protect our varilable state.
            // We can continue without storage.
            if (storage != null) {
                storage.tell(new StoreApi.Store(new StoreApi.Entry(key, count)), getSelf());
            }
        }
    }

    public interface StoreApi {

        public static class Store {
            public final Entry entry;

            public Store(Entry entry) {
                this.entry = entry;
            }

            @Override
            public String toString() {
                return String.format("%s(%s)", getClass().getSimpleName(), entry);
            }
        }

        public static class Entry {
            public final String key;
            public final long value;

            public Entry(String key, long value) {
                this.key = key;
                this.value = value;
            }

            @Override
            public String toString() {
                return String.format("%s(%s, %s)", getClass().getSimpleName(), key, value);
            }
        }

        public static class Get{
            public final String key;

            public Get(String key) {
                this.key = key;
            }

            @Override
            public String toString() {
                return String.format("%s(%s)", getClass().getSimpleName(), key);
            }
        }

        public static class StorageException extends RuntimeException {
            private static final long serialVersionUID = 1L;

            public StorageException(String message) {
                super(message);
            }
        }
    }

    /**
     * Saves key/value pairs to persistent storage when receiving Store message.
     * Replies with current value when receiving Get message. Will throw StorageException if the underlying data store is out of order
     */
    public static class Storage extends UntypedActor {

        final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
        final DummyDB db = DummyDB.instance;

        @Override
        public void onReceive(Object message) throws Exception {
            log.debug("received message {}", message);
            if (message instanceof StoreApi.Store) {
                StoreApi.Store store = (StoreApi.Store) message;
                db.save(store.entry.key, store.entry.value);
            } else if (message instanceof StoreApi.Get) {
                StoreApi.Get get = (StoreApi.Get) message;
                Long val = db.load(get.key);
                getSender().tell(new StoreApi.Entry(get.key, val == null ? Long.valueOf(0L) : val), getSelf());
            } else {
                unhandled(message);
            }
        }
    }


    public static class DummyDB {
        public static final DummyDB instance = new DummyDB();
        private final Map<String, Long> db = new HashMap<String, Long>();

        private DummyDB() {
        }

        public synchronized void save(String key, Long value) throws StoreApi.StorageException {
            if (11 <= value && value <= 14)
                throw new StoreApi.StorageException("Simulated store failure " + value);

            db.put(key, value);
        }

        public synchronized Long load(String key) throws StoreApi.StorageException {
            return db.get(key);
        }
    }
}
