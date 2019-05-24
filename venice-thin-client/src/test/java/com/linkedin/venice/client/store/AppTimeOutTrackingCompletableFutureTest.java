package com.linkedin.venice.client.store;

import com.linkedin.venice.client.stats.ClientStats;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class AppTimeOutTrackingCompletableFutureTest {

  @Test
  public void testTimeout() {
    // Never complete
    CompletableFuture innerFuture = new CompletableFuture();
    ClientStats mockStats = mock(ClientStats.class);
    CompletableFuture trackingFuture = AppTimeOutTrackingCompletableFuture.track(innerFuture, mockStats);

    try {
      trackingFuture.get(1, TimeUnit.MILLISECONDS);
      fail("TimeoutException is expected");
    } catch (TimeoutException e) {
      // expected
      verify(mockStats).recordAppTimedOutRequest();
      return;
    } catch (Exception e) {
      fail("Only TimeoutException is expected");
    }
  }

  @Test
  public void testNoTimeout() {
    // complete right away
    String testResult = "test";
    CompletableFuture<String> innerFuture = new CompletableFuture<>();
    innerFuture.complete(testResult);
    ClientStats mockStats = mock(ClientStats.class);
    CompletableFuture<String> trackingFuture = AppTimeOutTrackingCompletableFuture.track(innerFuture, mockStats);

    try {
      String result = trackingFuture.get(1, TimeUnit.MILLISECONDS);
      Assert.assertEquals(result, testResult);
      verify(mockStats, never()).recordAppTimedOutRequest();
    } catch (Exception e) {
      fail("No Exception is expected, but received: " + e);
    }
  }

  @Test
  public void testException() {
    // complete right away
    Exception mockException = mock(Exception.class);
    CompletableFuture<String> innerFuture = new CompletableFuture<>();
    innerFuture.completeExceptionally(mockException);
    ClientStats mockStats = mock(ClientStats.class);
    CompletableFuture<String> trackingFuture = AppTimeOutTrackingCompletableFuture.track(innerFuture, mockStats);

    try {
      trackingFuture.get(1, TimeUnit.MILLISECONDS);
      fail("ExecutionException is expected");
    } catch (ExecutionException e) {
      Assert.assertEquals(e.getCause(), mockException);
      verify(mockStats, never()).recordAppTimedOutRequest();
    } catch (Exception e) {
      fail("Only ExecutionException is expected, but received: " + e);
    }
  }
}
