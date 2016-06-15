/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import com.google.common.reflect.TypeToken;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

class MockRow implements Row {

    private final ColumnDefinitions columnDefinitions;
    private final Map<String, Object> columnValues = new HashMap<String, Object>();

    public MockRow(ColumnDefinitions columnDefinitions) {
        this.columnDefinitions = columnDefinitions;
    }

    <T extends MockRow>T setColumnValue(String column, Object value) {
        checkColumnExists(column);
        columnValues.put(column, value);
        return (T)this;
    }

    private void checkColumnExists(String column) {
        if (!columnDefinitions.contains(column)) {
            throw new IllegalArgumentException(column + " is not a column defined in this metadata");
        }
    }

    @Override
    public ColumnDefinitions getColumnDefinitions() {
        return columnDefinitions;
    }

    @Override
    public Token getToken(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public Token getToken(String name) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public Token getPartitionKeyToken() {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public boolean isNull(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public boolean getBool(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public byte getByte(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public short getShort(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public int getInt(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public long getLong(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public Date getTimestamp(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public LocalDate getDate(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public long getTime(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public float getFloat(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public double getDouble(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public ByteBuffer getBytesUnsafe(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public ByteBuffer getBytes(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public String getString(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public BigInteger getVarint(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public BigDecimal getDecimal(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public UUID getUUID(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public InetAddress getInet(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public <T> List<T> getList(int i, Class<T> elementsClass) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public <T> List<T> getList(int i, TypeToken<T> elementsType) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public <T> Set<T> getSet(int i, Class<T> elementsClass) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public <T> Set<T> getSet(int i, TypeToken<T> elementsType) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public <K, V> Map<K, V> getMap(int i, TypeToken<K> keysType, TypeToken<V> valuesType) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public UDTValue getUDTValue(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public TupleValue getTupleValue(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public Object getObject(int i) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public <T> T get(int i, Class<T> targetClass) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public <T> T get(int i, TypeToken<T> targetType) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public <T> T get(int i, TypeCodec<T> codec) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public boolean isNull(String name) {
        checkColumnExists(name);
        //todo is implementation correct?
        return columnValues.get(name) == null;
    }

    @Override
    public boolean getBool(String name) {
        checkColumnExists(name);
        Boolean value = (Boolean)columnValues.get(name);
        return value == null ? false : value;
    }

    @Override
    public byte getByte(String name) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public short getShort(String name) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public int getInt(String name) {
        checkColumnExists(name);
        Integer value = (Integer)columnValues.get(name);
        return value == null ? 0 : value;
    }

    @Override
    public long getLong(String name) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public Date getTimestamp(String name) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public LocalDate getDate(String name) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public long getTime(String name) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public float getFloat(String name) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public double getDouble(String name) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public ByteBuffer getBytesUnsafe(String name) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public ByteBuffer getBytes(String name) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public String getString(String name) {
        checkColumnExists(name);
        return (String)columnValues.get(name);
    }

    @Override
    public BigInteger getVarint(String name) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public BigDecimal getDecimal(String name) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public UUID getUUID(String name) {
        checkColumnExists(name);
        return (UUID)columnValues.get(name);
    }

    @Override
    public InetAddress getInet(String name) {
        checkColumnExists(name);
        return (InetAddress) columnValues.get(name);
    }

    @Override
    public <T> List<T> getList(String name, Class<T> elementsClass) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public <T> List<T> getList(String name, TypeToken<T> elementsType) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public <T> Set<T> getSet(String name, Class<T> elementsClass) {
        checkColumnExists(name);
        return (Set<T>)columnValues.get(name);
    }

    @Override
    public <T> Set<T> getSet(String name, TypeToken<T> elementsType) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public <K, V> Map<K, V> getMap(String name, TypeToken<K> keysType, TypeToken<V> valuesType) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public UDTValue getUDTValue(String name) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public TupleValue getTupleValue(String name) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public Object getObject(String name) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public <T> T get(String name, Class<T> targetClass) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public <T> T get(String name, TypeToken<T> targetType) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }

    @Override
    public <T> T get(String name, TypeCodec<T> codec) {
        throw new UnsupportedOperationException("Not implemented in test mock framework");
    }
}
