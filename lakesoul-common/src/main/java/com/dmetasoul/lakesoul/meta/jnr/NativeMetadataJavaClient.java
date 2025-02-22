// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package com.dmetasoul.lakesoul.meta.jnr;

import com.alibaba.fastjson.JSON;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.DataBaseProperty;
import com.dmetasoul.lakesoul.meta.entity.JniWrapper;
import com.google.protobuf.InvalidProtocolBufferException;
import jnr.ffi.ObjectReferenceManager;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.dmetasoul.lakesoul.meta.jnr.NativeUtils.*;

public class NativeMetadataJavaClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NativeMetadataJavaClient.class);

    private long timeout;

    private Pointer tokioPostgresClient = null;
    private Pointer tokioRuntime = null;

    protected final LibLakeSoulMetaData libLakeSoulMetaData;

    protected final ObjectReferenceManager<LibLakeSoulMetaData.BooleanCallback> booleanCallbackObjectReferenceManager;

    protected final ObjectReferenceManager<LibLakeSoulMetaData.StringCallback> stringCallbackObjectReferenceManager;

    protected final ObjectReferenceManager<LibLakeSoulMetaData.IntegerCallback> integerCallbackObjectReferenceManager;

    private static NativeMetadataJavaClient instance = null;

    private final ReentrantReadWriteLock lock;

    public NativeMetadataJavaClient() {
        this(5000L, 1 << 12);
    }

    public NativeMetadataJavaClient(long timeout, int bufferSize) {
        this.timeout = timeout;
        libLakeSoulMetaData = JnrLoader.get();
        booleanCallbackObjectReferenceManager = Runtime.getRuntime(libLakeSoulMetaData).newObjectReferenceManager();
        stringCallbackObjectReferenceManager = Runtime.getRuntime(libLakeSoulMetaData).newObjectReferenceManager();
        integerCallbackObjectReferenceManager = Runtime.getRuntime(libLakeSoulMetaData).newObjectReferenceManager();

        lock = new ReentrantReadWriteLock();
        initialize();
    }

    public static synchronized NativeMetadataJavaClient getInstance() {
        if (instance == null) {
            instance = new NativeMetadataJavaClient();
        }
        return instance;
    }

    public static synchronized void shutDownInstance() {
        closeAll();
    }

    public Pointer getTokioPostgresClient() {
        return tokioPostgresClient;
    }

    public ObjectReferenceManager<LibLakeSoulMetaData.BooleanCallback> getbooleanCallbackObjectReferenceManager() {
        return booleanCallbackObjectReferenceManager;
    }

    public ObjectReferenceManager<LibLakeSoulMetaData.StringCallback> getStringCallbackObjectReferenceManager() {
        return stringCallbackObjectReferenceManager;
    }

    public ObjectReferenceManager<LibLakeSoulMetaData.IntegerCallback> getIntegerCallbackObjectReferenceManager() {
        return integerCallbackObjectReferenceManager;
    }

    public Runtime getRuntime() {
        return Runtime.getRuntime(libLakeSoulMetaData);
    }

    public LibLakeSoulMetaData getLibLakeSoulMetaData() {
        return libLakeSoulMetaData;
    }

    static class ReferencedBooleanCallback implements LibLakeSoulMetaData.BooleanCallback {
        public final BiConsumer<Boolean, String> callback;
        private final Pointer key;
        private final ObjectReferenceManager<LibLakeSoulMetaData.BooleanCallback> referenceManager;

        public ReferencedBooleanCallback(BiConsumer<Boolean, String> callback, ObjectReferenceManager<LibLakeSoulMetaData.BooleanCallback> referenceManager) {
            this.callback = callback;
            this.referenceManager = referenceManager;
            key = this.referenceManager.add(this);
        }

        @Override
        public void invoke(Boolean result, String msg) {
            callback.accept(result, msg);
            close();
        }

        public void close() {
            if (key != null) {
                referenceManager.remove(key);
            }
        }
    }

    static class ReferencedIntegerCallback implements LibLakeSoulMetaData.IntegerCallback {
        public final BiConsumer<Integer, String> callback;
        private final Pointer key;
        private final ObjectReferenceManager<LibLakeSoulMetaData.IntegerCallback> referenceManager;

        public ReferencedIntegerCallback(BiConsumer<Integer, String> callback, ObjectReferenceManager<LibLakeSoulMetaData.IntegerCallback> referenceManager) {
            this.callback = callback;
            this.referenceManager = referenceManager;
            key = this.referenceManager.add(this);
        }

        @Override
        public void invoke(Integer result, String msg) {
            callback.accept(result, msg);
            close();
        }

        public void close() {
            if (key != null) {
                referenceManager.remove(key);
            }
        }
    }

    static class ReferencedStringCallback implements LibLakeSoulMetaData.StringCallback, AutoCloseable {
        public final BiConsumer<String, String> callback;
        private final Pointer key;
        private final ObjectReferenceManager<LibLakeSoulMetaData.StringCallback> referenceManager;

        public ReferencedStringCallback(BiConsumer<String, String> callback, ObjectReferenceManager<LibLakeSoulMetaData.StringCallback> referenceManager) {
            this.callback = callback;
            this.referenceManager = referenceManager;
            key = this.referenceManager.add(this);
        }

        @Override
        public void invoke(String result, String msg) {
            callback.accept(result, msg);
            close();
        }

        @Override
        public void close() {
            if (key != null) {
                referenceManager.remove(key);
            }
        }
    }

    private void initialize() {
        libLakeSoulMetaData.rust_logger_init();
        DataBaseProperty dataBaseProperty = DBUtil.getDBInfo();
        tokioRuntime = libLakeSoulMetaData.create_tokio_runtime();

        String config = String.format(
                "host=%s port=%s dbname=%s user=%s password=%s connect_timeout=10 ",
                dataBaseProperty.getHost(),
                dataBaseProperty.getPort(),
                dataBaseProperty.getDbName(),
                dataBaseProperty.getUsername(),
                dataBaseProperty.getPassword());
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        tokioPostgresClient = libLakeSoulMetaData.create_tokio_postgres_client(
                new ReferencedBooleanCallback((bool, msg) -> {
                    if (msg == null || msg.isEmpty()) {
                        future.complete(bool);
                    } else {
                        System.err.println(msg);
                        future.completeExceptionally(new IOException(msg));
                    }
                }, getbooleanCallbackObjectReferenceManager()),
                config,
                tokioRuntime
        );
        try {
            future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            LOG.error("Configuring postgres with {} timeout", dataBaseProperty);
            throw new RuntimeException(e);
        }
    }


    public JniWrapper executeQuery(Integer queryType, List<String> params) {
        try {
            getReadLock();
            int retryCounter = NATIVE_METADATA_MAX_RETRY_ATTEMPTS;
            while (retryCounter >= 0) {
                try {
                    final CompletableFuture<Integer> queryFuture = new CompletableFuture<>();
                    Pointer queryResult = getLibLakeSoulMetaData().execute_query(
                            new ReferencedIntegerCallback((result, msg) -> {
                                if (msg == null || msg.isEmpty()) {
                                    queryFuture.complete(result);
                                } else {
                                    queryFuture.completeExceptionally(new SQLException(msg));
                                }
                            }, getIntegerCallbackObjectReferenceManager()),
                            tokioRuntime,
                            tokioPostgresClient,
                            queryType,
                            String.join(PARAM_DELIM, params)
                    );
                    Integer len = queryFuture.get(timeout, TimeUnit.MILLISECONDS);
                    if (len < 0) return null;
                    Integer lenWithTail = len + 1;

                    Pointer buffer = Runtime.getRuntime(libLakeSoulMetaData).getMemoryManager().allocateDirect(lenWithTail, true);

                    final CompletableFuture<Boolean> importFuture = new CompletableFuture<>();
                    getLibLakeSoulMetaData().export_bytes_result(
                            new ReferencedBooleanCallback((result, msg) -> {
                                if (msg == null || msg.isEmpty()) {
                                    importFuture.complete(result);
                                } else {
                                    importFuture.completeExceptionally(new SQLException(msg));
                                }
                            }, getbooleanCallbackObjectReferenceManager()),
                            queryResult,
                            len,
                            buffer.address()
                    );
                    Boolean b = importFuture.get(timeout, TimeUnit.MILLISECONDS);
                    if (!b) return null;

                    byte[] bytes = new byte[len];
                    buffer.get(0, bytes, 0, len);
                    JniWrapper jniWrapper = JniWrapper.parseFrom(bytes);
                    getLibLakeSoulMetaData().free_bytes_result(queryResult);
                    return jniWrapper;

                } catch (InvalidProtocolBufferException | InterruptedException | ExecutionException e) {
                    if (retryCounter == 0) {
                        shutDownInstance();
                        throw new RuntimeException(e);
                    } else {
                        enlargeTimeout();
                        retryCounter--;
                    }
                } catch (TimeoutException e) {
                    if (retryCounter == 0) {
                        LOG.error("Execute Query {} with {} timeout", queryType, params);
                        throw new RuntimeException(e);
                    } else {
                        enlargeTimeout();
                        retryCounter--;
                    }
                }
            }
        } finally {
            unlockReadLock();
        }
        return null;
    }

    private void enlargeTimeout() {
        timeout += 5000L;
    }

    private void getReadLock() {
        lock.readLock().lock();
    }

    private void unlockReadLock() {
        lock.readLock().unlock();
    }

    private void getWriteLock() {
        lock.writeLock().lock();
    }

    private void unlockWriteLock() {
        lock.writeLock().unlock();
    }


    public Integer executeInsert(Integer insertType, JniWrapper jniWrapper) {
        try {
            getWriteLock();
            int retryCounter = NATIVE_METADATA_MAX_RETRY_ATTEMPTS;
            while (retryCounter >= 0) {
                try {
                    final CompletableFuture<Integer> future = new CompletableFuture<>();

                    byte[] bytes = jniWrapper.toByteArray();
                    Pointer buffer = Runtime.getRuntime(libLakeSoulMetaData).getMemoryManager().allocateDirect(bytes.length, true);;
                    buffer.put(0, bytes, 0, bytes.length);

                    getLibLakeSoulMetaData().execute_insert(
                            new ReferencedIntegerCallback((result, msg) -> {
                                if (msg == null || msg.isEmpty()) {
                                    future.complete(result);
                                } else {
                                    future.completeExceptionally(new SQLException(msg));
                                }
                            }, getIntegerCallbackObjectReferenceManager()),
                            tokioRuntime,
                            tokioPostgresClient,
                            insertType,
                            buffer.address(),
                            bytes.length
                    );
                    return future.get(timeout, TimeUnit.MILLISECONDS);
                } catch (InterruptedException | ExecutionException e) {
                    if (retryCounter == 0) {
                        shutDownInstance();
                        throw new RuntimeException(e);
                    } else {
                        enlargeTimeout();
                        retryCounter--;
                    }
                } catch (TimeoutException e) {
                    if (retryCounter == 0) {
                        LOG.error("Execute Insert {} with {} timeout", insertType, jniWrapper);
                        throw new RuntimeException(e);
                    } else {
                        enlargeTimeout();
                        retryCounter--;
                    }
                }
            }
        } finally {
            unlockWriteLock();
        }
        return -1;
    }

    public Integer executeUpdate(Integer updateType, List<String> params) {
        try {
            getWriteLock();
            int retryCounter = NATIVE_METADATA_MAX_RETRY_ATTEMPTS;
            while (retryCounter >= 0) {
                try {
                    final CompletableFuture<Integer> future = new CompletableFuture<>();

                    getLibLakeSoulMetaData().execute_update(
                            new ReferencedIntegerCallback((result, msg) -> {
                                if (msg == null || msg.isEmpty()) {
                                    future.complete(result);
                                } else {
                                    future.completeExceptionally(new SQLException(msg));
                                }
                            }, getIntegerCallbackObjectReferenceManager()),
                            tokioRuntime,
                            tokioPostgresClient,
                            updateType,
                            String.join(PARAM_DELIM, params)
                    );
                    return future.get(timeout, TimeUnit.MILLISECONDS);
                } catch (InterruptedException | ExecutionException e) {
                    if (retryCounter == 0) {
                        shutDownInstance();
                        throw new RuntimeException(e);
                    } else {
                        enlargeTimeout();
                        retryCounter--;
                    }
                } catch (TimeoutException e) {
                    if (retryCounter == 0) {
                        LOG.error("Execute Update {} with {} timeout", updateType, params);
                        throw new RuntimeException(e);
                    } else {
                        enlargeTimeout();
                        retryCounter--;
                    }
                }
            }
        } finally {
            unlockWriteLock();
        }
        return -1;
    }

    public List<String> executeQueryScalar(Integer queryScalarType, List<String> params) {
        try {
            getReadLock();
            int retryCounter = NATIVE_METADATA_MAX_RETRY_ATTEMPTS;
            while (retryCounter >= 0) {
                try {
                    final CompletableFuture<String> future = new CompletableFuture<>();

                    getLibLakeSoulMetaData().execute_query_scalar(
                            new ReferencedStringCallback((result, msg) -> {
                                if (msg == null || msg.isEmpty()) {
                                    future.complete(result);
                                } else {
                                    future.completeExceptionally(new SQLException(msg));
                                }
                            }, getStringCallbackObjectReferenceManager()),
                            tokioRuntime,
                            tokioPostgresClient,
                            queryScalarType,
                            String.join(PARAM_DELIM, params)
                    );
                    String result = future.get(timeout, TimeUnit.MILLISECONDS);
                    if (result.isEmpty()) return Collections.emptyList();
                    return Arrays.stream(result.split(PARAM_DELIM)).collect(Collectors.toList());
                } catch (InterruptedException | ExecutionException e) {
                    if (retryCounter == 0) {
                        shutDownInstance();
                        throw new RuntimeException(e);
                    } else {
                        enlargeTimeout();
                        retryCounter--;
                    }
                } catch (TimeoutException e) {
                    if (retryCounter == 0) {
                        LOG.error("Execute QueryScalar {} with {} timeout", queryScalarType, params);
                        throw new RuntimeException(e);
                    } else {
                        enlargeTimeout();
                        retryCounter--;
                    }
                }
            }
        } finally {
            unlockReadLock();
        }
        return Collections.emptyList();
    }

    public static Integer insert(NativeUtils.CodedDaoType insertType, JniWrapper jniWrapper) {
        return getInstance().executeInsert(insertType.getCode(), jniWrapper);
    }

    public static JniWrapper query(NativeUtils.CodedDaoType queryType, List<String> params) {
        if (params.size() != queryType.getParamsNum()) {
            throw new RuntimeException("Params Num mismatch for " + queryType.name() + ", params=" + params + " paramsNum=" + params.size());
        }
        return getInstance().executeQuery(queryType.getCode(), params);
    }

    public static Integer update(NativeUtils.CodedDaoType updateType, List<String> params) {
        if (params.size() != updateType.getParamsNum()) {
            throw new RuntimeException("Params Num mismatch for " + updateType.name() + ", params=" + params + " paramsNum=" + params.size());
        }
        return getInstance().executeUpdate(updateType.getCode(), params);
    }

    public static List<String> queryScalar(NativeUtils.CodedDaoType queryScalarType, List<String> params) {
        if (params.size() != queryScalarType.getParamsNum()) {
            throw new RuntimeException("Params Num mismatch for " + queryScalarType.name() + ", params=" + params + " paramsNum=" + params.size());
        }
        return getInstance().executeQueryScalar(queryScalarType.getCode(), params);
    }

    public static int cleanMeta() {
        final CompletableFuture<Integer> future = new CompletableFuture<>();

        NativeMetadataJavaClient instance = getInstance();
        instance.getWriteLock();
        try {
            instance.getLibLakeSoulMetaData().clean_meta_for_test(
                    new ReferencedIntegerCallback((result, msg) -> {
                        if (msg == null || msg.isEmpty()) {
                            future.complete(result);
                        } else {
                            future.completeExceptionally(new SQLException(msg));
                        }
                    }, instance.getIntegerCallbackObjectReferenceManager()),
                    instance.tokioRuntime,
                    instance.tokioPostgresClient
            );
            return future.get(instance.timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            LOG.error("Clean Meta timeout");
            throw new RuntimeException(e);
        } finally {
            instance.unlockWriteLock();
        }
    }

    @Override
    public void close() {
        if (tokioRuntime != null) {
            libLakeSoulMetaData.free_tokio_runtime(tokioRuntime);
            tokioRuntime = null;
        }
        if (tokioPostgresClient != null) {
            libLakeSoulMetaData.free_tokio_postgres_client(tokioPostgresClient);
            tokioPostgresClient = null;
        }
    }

    public static synchronized void closeAll() {
        if (instance != null) {
            instance.close();
            instance = null;
        }
    }

    static {
        java.lang.Runtime.getRuntime().addShutdownHook(new Thread(NativeMetadataJavaClient::closeAll));
    }

    /**
     * if ffi function failed with -100
     * should recreate pg client and prepared map
     *
     * @param tableName name
     * @param namespace the np of TableInfo
     * @return split(partition) desc array in json format by table_name, namespace , filter(WIP)
     */
    public List<SplitDesc> createSplitDescArray(String tableName, String namespace) {
        getReadLock();
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        Pointer ptr = getLibLakeSoulMetaData()
                .create_split_desc_array(
                        new ReferencedBooleanCallback((result, msg) -> {
                            if (msg != null) {
                                System.err.println(msg);
                            }
                            future.complete(result);
                        }, getbooleanCallbackObjectReferenceManager()),
                        tokioPostgresClient,
                        tokioRuntime,
                        tableName,
                        namespace);
        try {
            Boolean ans = future.get(timeout, TimeUnit.MILLISECONDS);
            if (ans) {
                //  This copies a zero (nul) terminated by array from native memory.
                String json = ptr.getString(0);
                List<SplitDesc> list = JSON.parseArray(json, SplitDesc.class);
                getLibLakeSoulMetaData().free_split_desc_array(ptr);
                return list;
            }
            // other errors
            throw new RuntimeException("create split desc array failed");
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            LOG.error("create split desc array timeout");
            throw new RuntimeException(e);
        } finally {
            unlockReadLock();
        }
    }
}
