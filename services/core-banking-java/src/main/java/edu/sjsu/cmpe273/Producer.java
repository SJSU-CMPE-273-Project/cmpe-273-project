package edu.sjsu.cmpe273;

import com.google.protobuf.util.JsonFormat;
import edu.sjsu.cmpe273.relayguard.v1.NotificationRequested;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

public final class Producer {
  static String env(String key, String fallback) { return System.getenv().getOrDefault(key, fallback); }
  public static void main(String[] args) throws Exception {
    var builder = NotificationRequested.newBuilder();
    JsonFormat.parser().merge(new String(System.in.readAllBytes(), StandardCharsets.UTF_8), builder);
    var message = builder.build();
    if (message.getSequence() <= 0) throw new IllegalArgumentException("sequence outside BIGINT range");
    var payload = JsonFormat.printer().omittingInsignificantWhitespace().print(message);
    if (List.of(args).contains("--publish")) {
      try (var client = IggyTcpClient.builder().host(env("IGGY_HOST", "localhost"))
          .port(Integer.parseInt(env("IGGY_PORT", "8090")))
          .credentials(env("IGGY_ROOT_USERNAME", "iggy"), env("IGGY_ROOT_PASSWORD", "Secret123")).buildAndLogin()) {
        client.messages().sendMessages(StreamId.of("relayguard"), TopicId.of("requests"),
          Partitioning.partitionId(Long.parseLong(System.getenv("IGGY_PARTITION"))), List.of(Message.of(payload)));
      }
    } else {
      System.out.println("{\"json\":" + payload + ",\"binary\":\"" +
        Base64.getEncoder().encodeToString(message.toByteArray()) + "\"}");
    }
  }
}
