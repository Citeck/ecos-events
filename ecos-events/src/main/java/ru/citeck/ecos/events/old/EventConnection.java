package ru.citeck.ecos.events.old;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import ru.citeck.ecos.events.old.data.dto.EventDto;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @author Roman Makarskiy
 */
@Slf4j
public class EventConnection {

    private final ConnectionFactory connectionFactory = new ConnectionFactory();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private Connection connection;

    private static final String TYPE_TOPIC = "topic";
    private static final String EXCHANGE_NAME = "events";
    private static final String DEAD_LETTER_EXCHANGE_PARAM = "x-dead-letter-exchange";
    private static final String MESSAGE_TTL_PARAM = "x-message-ttl";
    private static final int MESSAGE_TTL_5_MIN = 300000;
    private static final String RETRYING_POSTFIX = "._retry";

    private static final String TENANT_PATTERN = "tenant.%s.%s";

    private EventConnection() {

    }

    public void emit(EventDto eventDTO, String tenantId) {
        try (Channel channel = connection.createChannel()) {
            final String exchangeId = generateExchangeName(tenantId);
            final String type = eventDTO.resolveType();

            channel.exchangeDeclare(exchangeId, TYPE_TOPIC, true, false, null);

            channel.basicPublish(exchangeId, type, MessageProperties.PERSISTENT_TEXT_PLAIN,
                    objectMapper.writeValueAsBytes(eventDTO));

            log.debug(String.format("Emit event, tenant <%s>, routingKey <%s>, event <%s>", tenantId, type, eventDTO));
        } catch (TimeoutException | IOException e) {
            throw new EventConnectionException("Unable connect to event. Check you connection configuration", e);
        }
    }

    public void receive(String bindingKey, String queueName, String tenantId,
                        EventDeliveryCallback eventDeliveryCallback)
            throws IOException {
        final String workingQueueId = generateQueueName(queueName, tenantId);
        final String workingExchangeId = generateExchangeName(tenantId);
        final String retryingQueueId = workingQueueId + RETRYING_POSTFIX;
        final String retryingExchangeId = workingExchangeId + RETRYING_POSTFIX;

        Channel channel = connection.createChannel();

        channel.exchangeDeclare(workingExchangeId, TYPE_TOPIC, true, false, null);

        Map<String, Object> workingArgs = new HashMap<>();
        workingArgs.put(DEAD_LETTER_EXCHANGE_PARAM, retryingExchangeId);

        String workQueue = channel.queueDeclare(workingQueueId, true, false, false, workingArgs).getQueue();
        channel.queueBind(workQueue, workingExchangeId, bindingKey);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            eventDeliveryCallback.handle(consumerTag, delivery, channel);
            log.debug(String.format("Receive event, tenant <%s>, bindingKey <%s>, queueName <%s>, msg:\n%s", tenantId,
                    bindingKey, workingQueueId, new String(delivery.getBody(), StandardCharsets.UTF_8)));
        };

        channel.basicConsume(workingQueueId, false, deliverCallback, consumerTag -> {
        });

        Map<String, Object> retryingArgs = new HashMap<>();
        retryingArgs.put(DEAD_LETTER_EXCHANGE_PARAM, workingExchangeId);
        retryingArgs.put(MESSAGE_TTL_PARAM, MESSAGE_TTL_5_MIN);

        channel.exchangeDeclare(retryingExchangeId, TYPE_TOPIC);
        String retryQueue = channel.queueDeclare(retryingQueueId, true, false, false,
                retryingArgs).getQueue();
        channel.queueBind(retryQueue, retryingExchangeId, "#");
    }

    private String generateExchangeName(String tenantId) {
        return String.format(TENANT_PATTERN, tenantId, EXCHANGE_NAME);
    }

    private String generateQueueName(String queue, String tenantId) {
        return String.format(TENANT_PATTERN, tenantId, queue);
    }

    public static class Builder {

        private String host;
        private int port;
        private String username;
        private String password;

        public Builder() {

        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public EventConnection build() {
            EventConnection eventConnection = new EventConnection();

            StringBuilder infoMsg = new StringBuilder("\n======= Start builder event connection =======\n");

            if (!ConnectionFactory.DEFAULT_HOST.equals(this.host)) {
                infoMsg.append("host: ").append(this.host).append("\n");
                eventConnection.connectionFactory.setHost(this.host);
            }

            if (this.port != 0) {
                infoMsg.append("port: ").append(this.port).append("\n");
                eventConnection.connectionFactory.setPort(this.port);
            }

            if (StringUtils.isNotBlank(this.username)) {
                infoMsg.append("username: ").append(this.username).append("\n");
                eventConnection.connectionFactory.setUsername(this.username);
            }

            if (StringUtils.isNotBlank(this.password)) {
                infoMsg.append("password: ").append("******").append("\n");
                eventConnection.connectionFactory.setPassword(this.password);
            }

            try {
                eventConnection.connection = eventConnection.connectionFactory.newConnection();
            } catch (IOException | TimeoutException e) {
                throw new EventConnectionException(
                        "Unable connect to events server. Check you connection configuration,", e
                );
            }

            infoMsg.append("============= Connected to events ============\n");

            log.info(infoMsg.toString());

            return eventConnection;
        }

    }

}
