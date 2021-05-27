/*
 * Copyright 2017-2021 original authors
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
package io.micronaut.configuration.kafka.graal;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import org.apache.kafka.common.utils.ByteBufferUnmapper;
import org.apache.kafka.common.utils.Java;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

@TargetClass(value = ByteBufferUnmapper.class)
@SuppressWarnings("MissingJavadocType")
public final class ByteBufferUnmapperSubstitute {

    @Substitute
    public static void unmap(String resourceDescription, ByteBuffer buffer) throws IOException {
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException("Unmapping only works with direct buffers");
        }

        try {
            if (Java.IS_JAVA9_COMPATIBLE) {
                Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
                Method m = unsafeClass.getMethod("cleaner", void.class, ByteBuffer.class);
                Field f = unsafeClass.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                Object theUnsafe = f.get(null);
                m.invoke(theUnsafe, buffer);
            } else {
                Class<?> directBufferClass = Class.forName("java.nio.DirectByteBuffer");
                Method cleanerMethod = directBufferClass.getMethod("cleaner");
                cleanerMethod.setAccessible(true);
                Object cleaner = cleanerMethod.invoke(buffer);
                if (cleaner != null) {
                    Class<?> cleanerClass = Class.forName("sun.misc.Cleaner");
                    Method cleanMethod = cleanerClass.getMethod("clean");
                    cleanMethod.setAccessible(true);
                    cleanMethod.invoke(cleaner);
                }
            }
        } catch (Throwable throwable) {
            throw new IOException("Unable to unmap the mapped buffer: " + resourceDescription, throwable);
        }
    }

}
