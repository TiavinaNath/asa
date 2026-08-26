package gen.patrimoine.cas.handler;

import static gen.patrimoine.cas.concurrency.ThreadRenamer.renameWorkerThread;
import static java.lang.System.getenv;
import static java.lang.Thread.currentThread;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import gen.patrimoine.cas.PojaApplication;
import gen.patrimoine.cas.PojaGenerated;
import gen.patrimoine.cas.endpoint.EndpointConf;
import gen.patrimoine.cas.endpoint.event.EventConf;
import gen.patrimoine.cas.endpoint.event.consumer.EventConsumer;
import gen.patrimoine.cas.endpoint.event.consumer.model.ConsumableEvent;
import gen.patrimoine.cas.endpoint.event.consumer.model.ConsumableEventTyper;
import io.sentry.Sentry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import software.amazon.awssdk.regions.Region;

@Slf4j
@PojaGenerated
public class MailboxEventHandler implements RequestHandler<SQSEvent, String> {

  public static final String SPRING_SERVER_PORT_FOR_RANDOM_VALUE = "0";
  private final ConsumableEventTyper consumableEventTyper =
      new ConsumableEventTyper(
          new EndpointConf().objectMapper(), new EventConf(Region.of(getenv("AWS_REGION"))));

  @Override
  public String handleRequest(SQSEvent event, Context context) {
    renameWorkerThread(currentThread());
    log.info("Received: event={}, awsReqId={}", event, context.getAwsRequestId());
    try {
      List<SQSMessage> messages = event.getRecords();
      consumableEventTyper
          .apply(messages)
          .forEach(ConsumableEvent::newRandomVisibilityTimeout); // note(init-visibility)
      log.info("SQS messages: {}", messages);

      try (var applicationContext = applicationContext()) {
        var eventConsumer = applicationContext.getBean(EventConsumer.class);
        var messageConverter = applicationContext.getBean(ConsumableEventTyper.class);

        eventConsumer.accept(messageConverter.apply(messages));
      }
      return "ok";
    } catch (Exception e) {
      log.error("Error while processing SQS event", e);
      Sentry.captureException(e);
      throw e;
    } finally {
      Sentry.flush(Duration.ofSeconds(5).toMillis());
    }
  }

  private ConfigurableApplicationContext applicationContext(String... args) {
    SpringApplication application = new SpringApplication(PojaApplication.class);
    application.setDefaultProperties(Map.of("server.port", SPRING_SERVER_PORT_FOR_RANDOM_VALUE));
    application.setAdditionalProfiles("worker");
    return application.run(args);
  }
}
