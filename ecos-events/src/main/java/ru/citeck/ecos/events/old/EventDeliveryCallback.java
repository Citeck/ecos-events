package ru.citeck.ecos.events.old;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Delivery;

import java.io.IOException;

/**
 * @author Roman Makarskiy
 */
@FunctionalInterface
public interface EventDeliveryCallback {

    void handle(String consumerTag, Delivery message, Channel channel) throws IOException;

}
