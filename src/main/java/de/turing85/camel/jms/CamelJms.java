package de.turing85.camel.jms;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.ConnectionFactory;

import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.main.Main;
import org.apache.camel.main.MainConfigurationProperties;
import org.apache.qpid.jms.JmsConnectionFactory;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.jms;
import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.timer;

public class CamelJms {
  public static void main(String... args) throws Exception {
    Main main = new Main();
    try (MainConfigurationProperties configure = main.configure()) {
      configure.addRoutesBuilder(senderRoute());
      configure.addRoutesBuilder(receiverRoute());
      main.run();
    }
  }

  private static RouteBuilder receiverRoute() {
    return new RouteBuilder() {
      @Override
      public void configure() {
        from(jms("topic:numbers").connectionFactory(connectionFactory())
            .clientId(Optional.ofNullable(System.getenv("CLIENT_ID")).orElse("receiver-client"))
            .subscriptionShared(true).subscriptionDurable(true)
            .subscriptionName(Optional.ofNullable(System.getenv("SUBSCRIPTION_NAME"))
                .orElse("receiver-subscription"))
            .cacheLevelName("CACHE_NONE")).routeId("message-receiver")
            .log(LoggingLevel.INFO, "Received: ${body}");
      }
    };
  }

  private static ConnectionFactory connectionFactory() {
    return new JmsConnectionFactory(Optional.ofNullable(System.getenv("AMQ_USER")).orElse("admin"),
        Optional.ofNullable(System.getenv("AMQ_PASSWORT")).orElse("admin"),
        Optional.ofNullable(System.getenv("AMQ_URL")).orElse("amqp://localhost:5672"));
  }

  private static RouteBuilder senderRoute() {
    return new RouteBuilder() {
      private final AtomicInteger counter = new AtomicInteger();

      @Override
      public void configure() {
        from(timer("sender-timer").fixedRate(true).period(1000)).routeId("message-sender")
            .setBody(exchange -> counter.getAndIncrement())
            .to(jms("topic:numbers").connectionFactory(connectionFactory())
                .clientId(Optional.ofNullable(System.getenv("CLIENT_ID")).orElse("sender-client")))
            .log("Sent: ${body}");
        // @formatter:on
      }
    };
  }
}
