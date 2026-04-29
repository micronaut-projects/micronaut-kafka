package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.annotation.ConsumerCreationStrategy
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.core.annotation.AnnotationValue
import io.micronaut.inject.BeanDefinitionReference
import io.micronaut.inject.ExecutableMethod
import io.micronaut.messaging.exceptions.MessagingSystemException
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.serialization.Deserializer
import spock.lang.Specification

import java.lang.reflect.Method
import java.util.regex.Pattern
import java.util.regex.PatternSyntaxException

class ConsumerCreationStrategySupportSpec extends Specification {

    void 'setupConsumerSubscription subscribes once with all topics'() {
        given:
        Consumer consumer = Mock()
        ExecutableMethod<?, ?> executableMethod = mockExecutableMethod()
        List<AnnotationValue<Topic>> topicAnnotations = [
            AnnotationValue.builder(Topic).member('value', ['foo'] as String[]).build(),
            AnnotationValue.builder(Topic).member('value', ['bar'] as String[]).build()
        ]

        when:
        invokeSetupConsumerSubscription(executableMethod, topicAnnotations, new Object(), consumer)

        then:
        1 * consumer.subscribe(['foo', 'bar'])
        0 * _
    }

    void 'setupConsumerSubscription combines topic names and patterns into one pattern subscription'() {
        given:
        Consumer consumer = Mock()
        ExecutableMethod<?, ?> executableMethod = mockExecutableMethod()
        List<AnnotationValue<Topic>> topicAnnotations = [
            AnnotationValue.builder(Topic).member('value', ['foo'] as String[]).build(),
            AnnotationValue.builder(Topic).member('patterns', ['bar-.*'] as String[]).build()
        ]

        when:
        invokeSetupConsumerSubscription(executableMethod, topicAnnotations, new Object(), consumer)

        then:
        1 * consumer.subscribe({
            it instanceof Pattern &&
                it.matcher('foo').matches() &&
                it.matcher('bar-1').matches() &&
                !it.matcher('baz').matches()
        })
        0 * _
    }

    void 'method topics override class topics for per-topic listeners'() {
        given:
        def beanDefinition = loadBeanDefinition(MethodTopicOverridesClassTopicListener)
        def method = beanDefinition.executableMethods.find { it.name == 'receive' }

        when:
        def topicAnnotations = invokeResolveTopicAnnotations(beanDefinition, method, [method])

        then:
        topicAnnotations*.stringValues().flatten() == ['method-topic']
    }

    void 'topic aware deserializer delegates by topic'() {
        given:
        Deserializer fooDeserializer = Mock()
        Deserializer barDeserializer = Mock()
        TopicRouter<Deserializer<?>> router = new TopicRouter<>()
        router.register(['foo'] as String[], [] as String[], fooDeserializer, 'FooListener#receive')
        router.register(['bar'] as String[], [] as String[], barDeserializer, 'BarListener#receive')
        TopicAwareDeserializer deserializer = new TopicAwareDeserializer(router, 'value')

        when:
        def fooResult = deserializer.deserialize('foo', 'one'.bytes)
        def barResult = deserializer.deserialize('bar', 'two'.bytes)

        then:
        fooResult == 'foo'
        barResult == 'bar'
        1 * fooDeserializer.deserialize('foo', 'one'.bytes) >> 'foo'
        1 * barDeserializer.deserialize('bar', 'two'.bytes) >> 'bar'
        0 * _
    }

    void 'topic router rejects overlapping direct and pattern routes with different values'() {
        given:
        TopicRouter<Deserializer<?>> router = new TopicRouter<>()
        router.register(['foo'] as String[], [] as String[], Mock(Deserializer), 'FooListener#receive')
        router.register([] as String[], ['foo'] as String[], Mock(Deserializer), 'PatternListener#receive')

        when:
        router.resolve('foo', 'deserializer')

        then:
        def e = thrown(MessagingSystemException)
        e.message.contains('direct deserializer route')
        e.message.contains('PatternListener#receive')
    }

    void 'consumer info wraps invalid topic patterns with messaging system exception'() {
        when:
        consumerInfo(InvalidPatternPerClassListener)

        then:
        def e = thrown(MessagingSystemException)
        e.message.contains('Invalid @Topic pattern [[foo]')
        e.message.contains('InvalidPatternPerClassListener#receive')
        e.cause instanceof PatternSyntaxException
    }

    private ExecutableMethod<?, ?> mockExecutableMethod() {
        Stub(ExecutableMethod) {
            getDeclaringType() >> ConsumerCreationStrategySupportSpec
            getName() >> 'receive'
        }
    }

    private void invokeSetupConsumerSubscription(
        ExecutableMethod<?, ?> method,
        List<AnnotationValue<Topic>> topicAnnotations,
        Object consumerBean,
        Consumer<?, ?> consumer
    ) {
        Method reflectedMethod = KafkaConsumerProcessor.getDeclaredMethod(
            'setupConsumerSubscription',
            ExecutableMethod,
            List,
            Object,
            Consumer
        )
        reflectedMethod.accessible = true
        reflectedMethod.invoke(null, method, topicAnnotations, consumerBean, consumer)
    }

    private ConsumerInfo consumerInfo(Class<?> beanType) {
        def beanDefinition = loadBeanDefinition(beanType)
        def methods = beanDefinition.executableMethods.findAll {
            !it.getDeclaredAnnotationValuesByType(Topic).isEmpty()
        }
        new ConsumerInfo(
            'test-client',
            'test-group',
            OffsetStrategy.AUTO,
            methods[0].getAnnotation(KafkaListener),
            new Properties(),
            methods
        )
    }

    private def loadBeanDefinition(Class<?> beanType) {
        loadBeanDefinitionReference(beanType).load()
    }

    private BeanDefinitionReference<?> loadBeanDefinitionReference(Class<?> beanType) {
        def definitionType = Class.forName("${beanType.packageName}.\$${beanType.simpleName}\$Definition")
        return (BeanDefinitionReference<?>) definitionType.getDeclaredConstructor().newInstance()
    }

    private List<AnnotationValue<Topic>> invokeResolveTopicAnnotations(
        def beanDefinition,
        ExecutableMethod<?, ?> method,
        List<ExecutableMethod<?, ?>> methods
    ) {
        Method reflectedMethod = KafkaConsumerProcessor.getDeclaredMethod(
            'resolveTopicAnnotations',
            io.micronaut.inject.BeanDefinition,
            ExecutableMethod,
            ConsumerCreationStrategy,
            List
        )
        reflectedMethod.accessible = true
        return (List<AnnotationValue<Topic>>) reflectedMethod.invoke(
            null,
            beanDefinition,
            method,
            ConsumerCreationStrategy.PER_TOPIC,
            methods
        )
    }
}
