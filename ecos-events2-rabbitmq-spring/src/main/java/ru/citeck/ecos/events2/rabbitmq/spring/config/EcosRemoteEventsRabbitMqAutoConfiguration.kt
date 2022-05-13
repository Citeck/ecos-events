package ru.citeck.ecos.events2.rabbitmq.spring.config

import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration

/**
 * Auto configuration to initialize ecos remote events rabbit mq beans.
 *
 * @author Roman Makarskiy
 */
@Configuration
@ComponentScan(basePackages = ["ru.citeck.ecos.events2.rabbitmq.spring"])
open class EcosRemoteEventsRabbitMqAutoConfiguration
