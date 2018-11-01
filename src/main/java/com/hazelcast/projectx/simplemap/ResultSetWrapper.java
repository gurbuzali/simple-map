/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.projectx.simplemap;

import com.hazelcast.projectx.simplemap.operation.SetOperationFactory.SetOperationType;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.jet.Util.entry;

public class ResultSetWrapper<E> implements Set<E> {

    private final SerializationService ss;
    private final Object[] objects;
    private final SetOperationType setOperationType;

    private boolean converted = false;

    ResultSetWrapper(SerializationService serializationService, Object[] objects, SetOperationType setOperationType) {
        this.ss = serializationService;
        this.objects = objects;
        this.setOperationType = setOperationType;
    }

    @Override
    public int size() {
        return objects.length;
    }

    @Override
    public boolean isEmpty() {
        return objects.length == 0;
    }

    @Override
    public boolean contains(Object o) {
        for (E next : this) {
            if (next.equals(o)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return c.stream().allMatch(this::contains);
    }

    @Override
    public Iterator<E> iterator() {
        return new Iterator<E>() {

            int count;

            @Override
            public boolean hasNext() {
                boolean hasNext = count < objects.length;
                if (!hasNext) {
                    converted = true;
                }
                return hasNext;
            }

            @Override
            public E next() {
                E object = toObject(objects[count]);
                objects[count] = object;
                count++;
                return object;
            }
        };
    }

    @Override
    public Object[] toArray() {
        convertAll();
        return objects;
    }

    @Override
    public <T> T[] toArray(T[] a) {
        convertAll();
        if (a.length < objects.length) {
            return (T[]) Arrays.copyOf(objects, objects.length, a.getClass());
        }
        System.arraycopy(objects, 0, a, 0, objects.length);
        if (a.length > objects.length) {
            a[objects.length] = null;
        }
        return a;
    }

    @Override
    public boolean add(E e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    private void convertAll() {
        if (!converted) {
            for (int i = 0; i < objects.length; i++) {
                objects[i] = toObject(objects[i]);
            }
            converted = true;
        }
    }

    private E toObject(Object o) {
        if (converted) {
            return (E) o;
        }
        switch (setOperationType) {
            case KEY:
            case VALUE:
                return ss.toObject(o);
            case ENTRY:
                Map.Entry entry = (Map.Entry) o;
                return (E) entry(ss.toObject(entry.getKey()), ss.toObject(entry.getValue()));
            default:
                throw new IllegalArgumentException();
        }
    }
}
