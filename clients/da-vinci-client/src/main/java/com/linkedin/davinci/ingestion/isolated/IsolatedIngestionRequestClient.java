package com.linkedin.davinci.ingestion.isolated;

import com.linkedin.davinci.ingestion.HttpClientTransport;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.enums.IngestionAction;
import com.linkedin.venice.ingestion.protocol.enums.IngestionReportType;
import com.linkedin.venice.security.SSLFactory;
import java.io.Closeable;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * IsolatedIngestionRequestClient sends requests to monitor service in main process and retrieves responses.
 */
public class IsolatedIngestionRequestClient implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(IsolatedIngestionRequestClient.class);

  private final HttpClientTransport httpClientTransport;

  public IsolatedIngestionRequestClient(Optional<SSLFactory> sslFactory, int port) {
    httpClientTransport = new HttpClientTransport(sslFactory, port);
  }

  public void reportIngestionStatus(IngestionTaskReport report) {
    String topicName = report.topicName.toString();
    int partitionId = report.partitionId;
    // Avoid sending binary data in OffsetRecord and pollute logs.
    LOGGER.info(
        String.format(
            "Sending ingestion report %s, isPositive: %b, message: %s for partition: %d of topic: %s at offset: %d",
            IngestionReportType.valueOf(report.reportType),
            report.isPositive,
            report.message,
            partitionId,
            topicName,
            report.offset));
    try {
      httpClientTransport.sendRequest(IngestionAction.REPORT, report);
    } catch (Exception e) {
      LOGGER.warn("Failed to send report with exception for topic: " + topicName + ", partition: " + partitionId, e);
    }
  }

  @Override
  public void close() {
    httpClientTransport.close();
  }
}