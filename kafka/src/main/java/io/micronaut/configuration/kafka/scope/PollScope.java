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
package io.micronaut.configuration.kafka.scope;

import io.micronaut.runtime.context.scope.ThreadLocal;

/**
 * Extension of {@link ThreadLocal} that recreates / refreshed the bean for every consumer poll cycle,
 * similar to {@link  io.micronaut.runtime.http.scope.RequestScope} but for consumers
 * @author Haiden Rothwell
 * @since 5.4.0
 */
@ThreadLocal
public @interface PollScope {
}
