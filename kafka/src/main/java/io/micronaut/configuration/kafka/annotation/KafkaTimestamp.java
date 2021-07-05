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

import io.micronaut.core.bind.annotation.Bindable;

import java.lang.annotation.*;

/**
 * Parameter level annotation to indicate which parameter is bound to the Kafka Producer timestamp.
 *
 * @author Boris Finkelshteyn
 * @since 1.2
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER})
@Bindable
@Inherited
public @interface KafkaTimestamp {
}
