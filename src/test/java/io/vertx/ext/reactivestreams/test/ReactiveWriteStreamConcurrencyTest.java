package io.vertx.ext.reactivestreams.test;

import io.netty.channel.DefaultEventLoop;
import io.netty.util.concurrent.EventExecutor;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.reactivestreams.ReactiveWriteStream;
import io.vertx.ext.reactivestreams.impl.ReactiveWriteStreamImpl;
import io.vertx.test.core.AsyncTestBase;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicBoolean;

public class ReactiveWriteStreamConcurrencyTest extends AsyncTestBase {

  private Vertx vertx;

  @Before
  public void before() {
    vertx = Vertx.vertx();
  }

  @Test
  public void raceConditionOnCheckSendInReactiveWriteStreamImpl() {
    FakeReadStream<Integer> readStream = new FakeReadStream<>();
    ReactiveWriteStream<Integer> reactiveWriteStream = new ReactiveWriteStreamImpl<>(vertx);

    EventExecutor eventLoop = new DefaultEventLoop();
    AsyncSubscriber<Integer> subscriber = new AsyncSubscriber<>(eventLoop);
    reactiveWriteStream.subscribe(subscriber);

    readStream.pipeTo(reactiveWriteStream);

    int i = 0;
    while (!subscriber.nullPointerExceptionOccurred && !readStream.exceptionOccurred) {
      // simulate latency of reading data with sleep, could be randomized
      sleep(2);
      readStream.addData(i++);
    }

    assertWaitUntil(() -> subscriber.nullPointerExceptionOccurred || readStream.exceptionOccurred);
  }

  private class FakeReadStream<T> implements ReadStream<T> {

    private Handler<T> dataHandler;
    private boolean paused;
    int pauseCount;
    int resumeCount;

    boolean exceptionOccurred = false;

    void addData(T data) {
      if (dataHandler != null) {
        try {
          dataHandler.handle(data);
        } catch(Exception e) {
          exceptionOccurred = true;
          e.printStackTrace();
        }
      }
    }

    public FakeReadStream<T> handler(Handler<T> handler) {
      this.dataHandler = handler;
      return this;
    }

    public FakeReadStream<T> pause() {
      paused = true;
      pauseCount++;
      return this;
    }

    @Override
    public ReadStream<T> fetch(long amount) {
      // Pump only use request/pause
      throw new UnsupportedOperationException();
    }

    public FakeReadStream<T> pause(Handler<Void> doneHandler) {
      pause();
      doneHandler.handle(null);
      return this;
    }

    public FakeReadStream<T> resume() {
      paused = false;
      resumeCount++;
      return this;
    }

    public FakeReadStream<T> resume(Handler<Void> doneHandler) {
      resume();
      doneHandler.handle(null);
      return this;
    }

    public FakeReadStream<T> exceptionHandler(Handler<Throwable> handler) {
      return this;
    }

    public FakeReadStream<T> endHandler(Handler<Void> endHandler) {
      return this;
    }
  }

  private class AsyncSubscriber<T> implements Subscriber<T> {
    static final long DEFAULT_LOW_WATERMARK = 4;
    static final long DEFAULT_HIGH_WATERMARK = 16;

    boolean nullPointerExceptionOccurred = false;

    private final EventExecutor executor;
    private final long demandLowWatermark;
    private final long demandHighWatermark;
    private final AtomicBoolean hasSubscription = new AtomicBoolean();
    private volatile Subscription subscription;
    private long outstandingDemand = 0;

    AsyncSubscriber(EventExecutor executor, long demandLowWatermark, long demandHighWatermark) {
      this.executor = executor;
      this.demandLowWatermark = demandLowWatermark;
      this.demandHighWatermark = demandHighWatermark;
    }

    AsyncSubscriber(EventExecutor executor) {
      this(executor, DEFAULT_LOW_WATERMARK, DEFAULT_HIGH_WATERMARK);
    }

    @Override
    public void onSubscribe(Subscription subscription) {
      if (subscription == null) {
        throw new NullPointerException("Null subscription");
      } else if (!hasSubscription.compareAndSet(false, true)) {
        subscription.cancel();
      } else {
        this.subscription = subscription;
        executor.execute(new Runnable() {
          @Override
          public void run() {
            // simulate latency with sleep, could be randomized
            sleep(30);
            maybeRequestMore();
          }
        });
      }
    }

    @Override
    public void onNext(T element) {
      System.out.println("!! OnNext: " + element);
      outstandingDemand--;
      maybeRequestMore();
    }

    @Override
    public void onError(Throwable t) {
      t.printStackTrace();
      System.out.println("!! onError");
    }

    @Override
    public void onComplete() {
      System.out.println("!! onComplete");
    }

    private void maybeRequestMore() {
      if (outstandingDemand <= demandLowWatermark) {
        long toRequest = demandHighWatermark - outstandingDemand;

        outstandingDemand = demandHighWatermark;
        try {
          subscription.request(toRequest);
        } catch (Exception e) {
          nullPointerExceptionOccurred = true;
          e.printStackTrace();
        }

      }
    }
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
    }
  }
}
