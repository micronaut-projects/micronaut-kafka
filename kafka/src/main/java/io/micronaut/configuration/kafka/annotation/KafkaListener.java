/*
 * Copyright 2017-2020 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.kafka.annotation;

import io.micronaut.context.annotation.*;
import io.micronaut.messaging.annotation.MessageListener;
import org.apache.kafka.common.IsolationLevel;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * <p>Annotation applied at the class level to indicate that a bean is a Kafka {@link org.apache.kafka.clients.consumer.Consumer}.</p>
 *
 *
 * @author graemerocher
 * @since 1.0
 */
@Documented
@Retention(RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@MessageListener
public @interface KafkaListener {

    /**
     * Sets the consumer group id of the Kafka consumer. If not specified the group id is configured
     * to be the value of {@link io.micronaut.runtime.ApplicationConfiguration#getName()} otherwise
     * the name of the class is used.
     *
     * @return The group id
     */
    @AliasFor(member = "groupId")
    String value() default "";

    /**
     * The same as {@link #value()}.
     *
     * @return The group id
     */
    @AliasFor(member = "value")
    String groupId() default "";

    /**
     * A unique string (UUID) can be appended to the group ID.
     * In that case, each consumer will be the only member of a unique consumer group.
     *
     * @return True to make each group ID unique. Defaults to false.
     */
    boolean uniqueGroupId() default false;

    /**
     * Sets the client id of the Kafka consumer. If not specified the client id is configured
     * to be the value of {@link io.micronaut.runtime.ApplicationConfiguration#getName()}.
     *
     * @return The client id
     */
    String clientId() default "";

    /**
     * The {@link OffsetStrategy} to use for the consumer.
     *
     * @return The {@link OffsetStrategy}
     */
    OffsetStrategy offsetStrategy() default OffsetStrategy.AUTO;

    /**
     * @return The strategy to use to start consuming records
     */
    OffsetReset offsetReset() default OffsetReset.LATEST;

    /**
     * The client id of the producer that is used for {@link io.micronaut.messaging.annotation.SendTo}.
     *
     * @return the producer client id
     */
    String producerClientId() default "";

    /**
     * This setting applies only for the producer that is used for {@link io.micronaut.messaging.annotation.SendTo}.
     *
     * The TransactionalId to use for transactional delivery. This enables reliability semantics which span multiple producer
     * sessions since it allows the client to guarantee that transactions using the same TransactionalId have been completed prior to starting any new transactions.
     * If no TransactionalId is provided, then the producer is limited to idempotent delivery.
     * If a TransactionalId is configured, <code>enable.idempotence</code> is implied.
     * By default, the TransactionId is not configured, which means transactions cannot be used.
     *
     * @return the producer transaction id
     */
    String producerTransactionalId() default "";

    /**
     * Kafka consumer isolation level to control how to read messages written transactionally.
     *
     * See {@link org.apache.kafka.clients.consumer.ConsumerConfig#ISOLATION_LEVEL_CONFIG}.
     *
     * @return The isolation level
     */
    IsolationLevel isolation() default IsolationLevel.READ_UNCOMMITTED;

    /**
     * Setting the error strategy allows you to resume at the next offset
     * or to seek the consumer (stop on error) to the failed offset so that
     * it can retry if an error occurs
     *
     * The consumer bean is still able to implement a custom exception handler to replace
     * {@link io.micronaut.configuration.kafka.exceptions.DefaultKafkaListenerExceptionHandler}
     * and set the error strategy.
     *
     * @return The strategy to use when an error occurs
     */
    ErrorStrategy errorStrategy() default @ErrorStrategy();

    /**
     * Dynamically configure the number of threads of a Kafka consumer.
     *
     * <p>Kafka consumers are by default single threaded. If you wish to increase the number of threads
     * for a consumer you can alter this setting. Note that this means that multiple partitions will
     * be allocated to a single application.
     *
     * <p>NOTE: When using this setting if your bean is {@link jakarta.inject.Singleton} then local state will be
     * shared between invocations from different consumer threads</p>
     *
     * <p>{@code threadsValue} takes precedence over {@code threads} if they are both set.
     *
     * @return The number of threads
     * @see KafkaListener#threads()
     */
    @AliasFor(member = "threads")
    String threadsValue() default "";

    /**
     * Statically configure the number of threads of a Kafka consumer.
     *
     * <p>Kafka consumers are by default single threaded. If you wish to increase the number of threads
     * for a consumer you can alter this setting. Note that this means that multiple partitions will
     * be allocated to a single application.
     *
     * <p>NOTE: When using this setting if your bean is {@link jakarta.inject.Singleton} then local state will be
     * shared between invocations from different consumer threads</p>
     *
     * <p>{@code threads} will be overridden by {@code threadsValue} if they are both set.
     *
     * @return The number of threads
     * @see KafkaListener#threadsValue()
     */
    int threads() default 1;

    /**
     * The timeout to use for calls to {@link org.apache.kafka.clients.consumer.Consumer#poll(java.time.Duration)}.
     *
     * @return The timeout. Defaults to 100ms
     */
    String pollTimeout() default "100ms";

    /**
     * The session timeout for a consumer. See {@code session.timeout.ms}.
     *
     * @return The session timeout as a duration.
     * @see org.apache.kafka.clients.consumer.ConsumerConfig#SESSION_TIMEOUT_MS_CONFIG
     */
    String sessionTimeout() default "";

    /**
     * The heart beat interval for the consumer. See {@code heartbeat.interval.ms}.
     *
     * @return The heartbeat interval as a duration.
     * @see org.apache.kafka.clients.consumer.ConsumerConfig#HEARTBEAT_INTERVAL_MS_CONFIG
     */
    String heartbeatInterval() default "";

    /**
     * For listeners that return reactive types, message offsets are committed without blocking the consumer.
     * If the reactive type produces an error, then the message can be lost. Enabling this setting allows the consumer
     * to redelivery the message so that it can be processed again by another consumer that may have better luck.
     *
     * @return True if redelivery should be enabled. Defaults to false.
     */
    boolean redelivery() default false;

    /**
     * By default each listener will consume a single {@link org.apache.kafka.clients.consumer.ConsumerRecord}.
     *
     * By setting this value to {@code true} and specifying a container type in the method signatures you can indicate that the method should instead receive all the records at once in a batch.
     *
     * @return Whether to receive a batch of records or not
     */
    boolean batch() default false;

    /**
     * Additional properties to configure with for Consumer.
     *
     * @return The properties
     */
    Property[] properties() default {};

    /**
     * Setting to allow start the consumer in paused mode.
     *
     * @return the auto startup setting
     */
    boolean autoStartup() default true;
}
