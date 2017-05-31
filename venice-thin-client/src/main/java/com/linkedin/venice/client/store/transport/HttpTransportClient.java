package com.linkedin.venice.client.store.transport;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.schema.SchemaData;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

/**
 * {@link CloseableHttpAsyncClient} based TransportClient implementation.
 */
public class HttpTransportClient extends TransportClient {
  private Logger logger = Logger.getLogger(HttpTransportClient.class);

  // Example: 'http://router-host:80/'
  protected final String routerUrl;
  private final CloseableHttpAsyncClient httpClient;

  public HttpTransportClient(String routerUrl) {
    this(routerUrl, HttpAsyncClients.createDefault());
  }

  public HttpTransportClient(String routerUrl, CloseableHttpAsyncClient httpClient) {
    this.routerUrl = ensureTrailingSlash(routerUrl);
    this.httpClient = httpClient;
    httpClient.start();
  }

  @Override
  public CompletableFuture<TransportClientResponse> get(String requestPath) {
    HttpGet request = getHttpGetRequest(requestPath);
    CompletableFuture<TransportClientResponse> valueFuture = new CompletableFuture<>();
    httpClient.execute(request, new HttpTransportClientCallback(valueFuture));
    return valueFuture;
  }

  @Override
  public CompletableFuture<TransportClientResponse> post(String requestPath, byte[] requestBody) {
    HttpPost request = getHttpPostRequest(requestPath, requestBody);
    CompletableFuture<TransportClientResponse> valueFuture = new CompletableFuture<>();
    httpClient.execute(request, new HttpTransportClientCallback(valueFuture));
    return valueFuture;
  }

  private String getHttpRequestUrl(String requestPath) {
    return routerUrl + requestPath;
  }

  private HttpGet getHttpGetRequest(String requestPath) {
    return new HttpGet(getHttpRequestUrl(requestPath));
  }

  private HttpPost getHttpPostRequest(String requestPath, byte[] body) {
    HttpPost httpPost = new HttpPost(getHttpRequestUrl(requestPath));
    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(new ByteArrayInputStream(body));
    httpPost.setEntity(entity);

    return httpPost;
  }

  @Override
  public void close() {
    try {
      httpClient.close();
      logger.debug("HttpStoreClient closed");
    } catch (IOException e) {
      logger.error("Failed to close internal CloseableHttpAsyncClient", e);
    }
  }

  /**
   * The same {@link CloseableHttpAsyncClient} could not be used to send out another request in its own callback function.
   * @return
   */
  @Override
  public TransportClient getCopyIfNotUsableInCallback() {
    return new HttpTransportClient(routerUrl);
  }

  private static class HttpTransportClientCallback extends TransportClientCallback implements FutureCallback<HttpResponse> {
    public HttpTransportClientCallback(CompletableFuture<TransportClientResponse> valueFuture) {
      super(valueFuture);
    }

    @Override
    public void failed(Exception ex) {
      getValueFuture().completeExceptionally(new VeniceClientException(ex));
    }

    @Override
    public void cancelled() {
      getValueFuture().completeExceptionally(new VeniceClientException("Request cancelled"));
    }

    @Override
    public void completed(HttpResponse result) {
      int statusCode = result.getStatusLine().getStatusCode();
      int schemaId = SchemaData.INVALID_VALUE_SCHEMA_ID;
      // If we try to retrieve the header value directly, and the 'getValue' will hang if the header doesn't exist.
        Header schemaIdHeader = result.getFirstHeader(HEADER_VENICE_SCHEMA_ID);
        if (HttpStatus.SC_OK == statusCode) {
          if (null != schemaIdHeader) {
            schemaId = Integer.parseInt(schemaIdHeader.getValue());
          }
      }
      byte[] body = null;
      try (InputStream bodyStream = result.getEntity().getContent()) {
        body = IOUtils.toByteArray(bodyStream);
      } catch (IOException e) {
        getValueFuture().completeExceptionally(new VeniceClientException(e));
        return;
      }

      completeFuture(statusCode, body, schemaId);
    }
  }

  public static String ensureTrailingSlash(String input) {
    if (input.endsWith("/")) {
      return input;
    } else {
      return input + "/";
    }
  }

  public String toString() {
    return this.getClass().getSimpleName() + "(routerUrl: " + routerUrl + ")";
  }
}
