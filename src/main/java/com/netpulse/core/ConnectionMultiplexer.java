package com.netpulse.core;

import sun.misc.Unsafe;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public final class ConnectionMultiplexer {

    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Unable to acquire Unsafe instance", e);
        }
    }

    private static final int CACHE_LINE_SIZE = 64;
    private static final int DEFAULT_RING_BUFFER_SIZE = 1024 * 1024;
    private static final int TIMING_WHEEL_TICKS = 512;
    private static final long TIMING_WHEEL_TICK_DURATION_NS = 1_000_000L; // 1ms

    private final EventLoop[] eventLoops;
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final DirectMemoryPool memoryPool;

    public ConnectionMultiplexer(int coreCount, int poolCapacity, long poolBlockSize) {
        if (coreCount <= 0) {
            throw new IllegalArgumentException("Core count must be positive");
        }
        this.memoryPool = new DirectMemoryPool(poolCapacity, poolBlockSize);
        this.eventLoops = new EventLoop[coreCount];
        
        ThreadFactory factory = r -> {
            Thread t = new Thread(r, "net-pulse-event-loop");
            t.setDaemon(true);
            return t;
        };

        for (int i = 0; i < coreCount; i++) {
            this.eventLoops[i] = new EventLoop(i, memoryPool, factory);
        }
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        for (EventLoop loop : eventLoops) {
            loop.start();
        }
    }

    public void shutdown() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        for (EventLoop loop : eventLoops) {
            loop.shutdown();
        }
        memoryPool.releaseAll();
    }

    public boolean register(SocketChannel channel, int ops, ChannelContext context) {
        if (!running.get()) {
            return false;
        }
        int index = Math.abs(roundRobinCounter.getAndIncrement() % eventLoops.length);
        return eventLoops[index].enqueueRegistration(channel, ops, context);
    }

    public DirectBuffer allocateBuffer() {
        return memoryPool.acquire();
    }

    public static final class DirectBuffer {
        private final long address;
        private final long capacity;
        private final long blockId;
        private final DirectMemoryPool owner;
        private final AtomicBoolean active = new AtomicBoolean(true);

        DirectBuffer(long address, long capacity, long blockId, DirectMemoryPool owner) {
            this.address = address;
            this.capacity = capacity;
            this.blockId = blockId;
            this.owner = owner;
        }

        public long address() {
            return address;
        }

        public long capacity() {
            return capacity;
        }

        public void release() {
            if (active.compareAndSet(true, false)) {
                owner.recycle(blockId);
            }
        }
    }

    public static final class DirectMemoryPool {
        private final long baseAddress;
        private final long blockSize;
        private final int totalBlocks;
        private final long[] freeList;
        private int head;
        private final Object lock = new Object();

        public DirectMemoryPool(int totalBlocks, long blockSize) {
            this.totalBlocks = totalBlocks;
            this.blockSize = blockSize;
            long totalSize = totalBlocks * blockSize;
            this.baseAddress = UNSAFE.allocateMemory(totalSize);
            this.freeList = new long[totalBlocks];
            for (int i = 0; i < totalBlocks; i++) {
                freeList[i] = i;
            }
            this.head = totalBlocks;
        }

        public DirectBuffer acquire() {
            synchronized (lock) {
                if (head <= 0) {
                    throw new OutOfMemoryError("Direct memory pool exhausted");
                }
                long blockIndex = freeList[--head];
                long addr = baseAddress + (blockIndex * blockSize);
                return new DirectBuffer(addr, blockSize, blockIndex, this);
            }
        }

        void recycle(long blockIndex) {
            synchronized (lock) {
                if (head >= totalBlocks) {
                    throw new IllegalStateException("Double free detected in memory pool");
                }
                freeList[head++] = blockIndex;
            }
        }

        public void releaseAll() {
            synchronized (lock) {
                UNSAFE.freeMemory(baseAddress);
            }
        }
    }

    public static final class ChannelContext {
        private volatile long lastActiveTimeNs;
        private volatile boolean readSuspended;
        private final long id;

        public ChannelContext(long id) {
            this.id = id;
            this.lastActiveTimeNs = System.nanoTime();
        }

        public long id() {
            return id;
        }

        public long lastActiveTimeNs() {
            return lastActiveTimeNs;
        }

        public void updateActivity() {
            this.lastActiveTimeNs = System.nanoTime();
        }

        public boolean isReadSuspended() {
            return readSuspended;
        }

        public void setReadSuspended(boolean readSuspended) {
            this.readSuspended = readSuspended;
        }
    }

    public static final class MpscRingBuffer {
        private final long[] sequenceBuffer;
        private final Object[] objectBuffer;
        private final int capacityMask;
        private final int capacity;
        
        @sun.misc.Contended
        private volatile long producerIndex = 0L;
        
        @sun.misc.Contended
        private volatile long consumerIndex = 0L;

        public MpscRingBuffer(int capacity) {
            if ((capacity & (capacity - 1)) != 0) {
                throw new IllegalArgumentException("Capacity must be a power of 2");
            }
            this.capacity = capacity;
            this.capacityMask = capacity - 1;
            this.sequenceBuffer = new long[capacity];
            this.objectBuffer = new Object[capacity];
            for (int i = 0; i < capacity; i++) {
                UNSAFE.putLongVolatile(sequenceBuffer, Unsafe.ARRAY_LONG_BASE_OFFSET + (i * Unsafe.ARRAY_LONG_INDEX_SCALE), i);
            }
        }

        public boolean offer(Object item) {
            long currentProducerIndex;
            long sequenceAddress;
            long seq;
            
            do {
                currentProducerIndex = producerIndex;
                sequenceAddress = Unsafe.ARRAY_LONG_BASE_OFFSET + ((currentProducerIndex & capacityMask) * Unsafe.ARRAY_LONG_INDEX_SCALE);
                seq = UNSAFE.getLongVolatile(sequenceBuffer, sequenceAddress);
                long diff = seq - currentProducerIndex;
                
                if (diff == 0L) {
                    if (UNSAFE.compareAndSwapLong(this, producerIndexOffset(), currentProducerIndex, currentProducerIndex + 1L)) {
                        break;
                    }
                } else if (diff < 0L) {
                    return false; // Buffer full
                }
            } while (true);

            long elementAddress = Unsafe.ARRAY_OBJECT_BASE_OFFSET + ((currentProducerIndex & capacityMask) * Unsafe.ARRAY_OBJECT_INDEX_SCALE);
            UNSAFE.putOrderedObject(objectBuffer, elementAddress, item);
            UNSAFE.putOrderedLong(sequenceBuffer, sequenceAddress, currentProducerIndex + 1L);
            return true;
        }

        public int drain(Object[] target, int limit) {
            long currentConsumerIndex = consumerIndex;
            int n = 0;
            
            for (int i = 0; i < limit; i++) {
                long idx = currentConsumerIndex + i;
                long sequenceAddress = Unsafe.ARRAY_LONG_BASE_OFFSET + ((idx & capacityMask) * Unsafe.ARRAY_LONG_INDEX_SCALE);
                long seq = UNSAFE.getLongVolatile(sequenceBuffer, sequenceAddress);
                long diff = seq - (idx + 1L);
                
                if (diff == 0L) {
                    long elementAddress = Unsafe.ARRAY_OBJECT_BASE_OFFSET + ((idx & capacityMask) * Unsafe.ARRAY_OBJECT_INDEX_SCALE);
                    target[i] = objectBuffer[(int)(idx & capacityMask)];
                    UNSAFE.putObject(objectBuffer, elementAddress, null);
                    UNSAFE.putOrderedLong(sequenceBuffer, sequenceAddress, idx + capacity);
                    n++;
                } else {
                    break;
                }
            }
            
            if (n > 0) {
                UNSAFE.putOrderedLong(this, consumerIndexOffset(), currentConsumerIndex + n);
            }
            return n;
        }

        private static long producerIndexOffset() {
            try {
                return UNSAFE.objectFieldOffset(MpscRingBuffer.class.getDeclaredField("producerIndex"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private static long consumerIndexOffset() {
            try {
                return UNSAFE.objectFieldOffset(MpscRingBuffer.class.getDeclaredField("consumerIndex"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static final class TimingWheel {
        private final TimerTask[][] wheel;
        private final int ticksMask;
        private final long tickDurationNs;
        private long currentTick = 0L;
        private long lastTickTimeNs;

        public static final class TimerTask {
            final Runnable task;
            long expirationNs;
            TimerTask next;

            public TimerTask(Runnable task, long expirationNs) {
                this.task = task;
                this.expirationNs = expirationNs;
            }
        }

        public TimingWheel(int ticks, long tickDurationNs) {
            if ((ticks & (ticks - 1)) != 0) {
                throw new IllegalArgumentException("Ticks must be power of 2");
            }
            this.wheel = new TimerTask[ticks][];
            for (int i = 0; i < ticks; i++) {
                this.wheel[i] = new TimerTask[0];
            }
            this.ticksMask = ticks - 1;
            this.tickDurationNs = tickDurationNs;
            this.lastTickTimeNs = System.nanoTime();
        }

        public void schedule(Runnable task, long delayNs) {
            long expiration = System.nanoTime() + delayNs;
            TimerTask timerTask = new TimerTask(task, expiration);
            int slot = (int)((expiration / tickDurationNs) & ticksMask);
            
            synchronized (wheel) {
                TimerTask head = wheel[slot][0];
                timerTask.next = head;
                wheel[slot] = new TimerTask[] { timerTask };
            }
        }

        public void advance() {
            long now = System.nanoTime();
            long elapsed = now - lastTickTimeNs;
            if (elapsed < tickDurationNs) {
                return;
            }
            
            long ticksToAdvance = elapsed / tickDurationNs;
            lastTickTimeNs += ticksToAdvance * tickDurationNs;

            for (long i = 0; i < ticksToAdvance; i++) {
                int slot = (int)(currentTick & ticksMask);
                TimerTask tasks;
                synchronized (wheel) {
                    tasks = wheel[slot][0];
                    wheel[slot] = new TimerTask[0];
                }

                while (tasks != null) {
                    try {
                        tasks.task.run();
                    } catch (Throwable t) {
                        // Cascading exception isolation
                        t.printStackTrace();
                    }
                    tasks = tasks.next;
                }
                currentTick++;
            }
        }
    }

    public static final class EventLoop implements Runnable {
        private final int id;
        private final DirectMemoryPool memoryPool;
        private final ThreadFactory threadFactory;
        private final MpscRingBuffer ringBuffer;
        private final TimingWheel timingWheel;
        private Selector selector;
        private Thread thread;
        private final AtomicBoolean active = new AtomicBoolean(false);
        private final Object[] drainArray = new Object[1024];

        private static final class Registration {
            final SocketChannel channel;
            final int ops;
            final ChannelContext context;

            Registration(SocketChannel channel, int ops, ChannelContext context) {
                this.channel = channel;
                this.ops = ops;
                this.context = context;
            }
        }

        public EventLoop(int id, DirectMemoryPool memoryPool, ThreadFactory threadFactory) {
            this.id = id;
            this.memoryPool = memoryPool;
            this.threadFactory = threadFactory;
            this.ringBuffer = new MpscRingBuffer(DEFAULT_RING_BUFFER_SIZE);
            this.timingWheel = new TimingWheel(TIMING_WHEEL_TICKS, TIMING_WHEEL_TICK_DURATION_NS);
            try {
                this.selector = Selector.open();
            } catch (IOException e) {
                throw new RuntimeException("Failed to open selector", e);
            }
        }

        public void start() {
            if (!active.compareAndSet(false, true)) {
                return;
            }
            this.thread = threadFactory.newThread(this);
            this.thread.start();
        }

        public void shutdown() {
            if (!active.compareAndSet(true, false)) {
                return;
            }
            if (selector != null) {
                selector.wakeup();
            }
        }

        public boolean enqueueRegistration(SocketChannel channel, int ops, ChannelContext context) {
            Registration reg = new Registration(channel, ops, context);
            return ringBuffer.offer(reg);
        }

        @Override
        public void run() {
            while (active.get()) {
                try {
                    processRegistrations();
                    timingWheel.advance();

                    int selectedKeys = selector.select(1L);
                    if (selectedKeys > 0) {
                        Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                        while (iterator.hasNext()) {
                            SelectionKey key = iterator.next();
                            iterator.remove();

                            if (!key.isValid()) {
                                continue;
                            }

                            try {
                                if (key.isReadable()) {
                                    handleRead(key);
                                }
                                if (key.isWritable()) {
                                    handleWrite(key);
                                }
                            } catch (Exception e) {
                                handleSelectorCrash(key, e);
                            }
                        }
                    }
                } catch (Throwable t) {
                    recoverSelector(t);
                }
            }
            cleanup();
        }

        private void processRegistrations() {
            int drained = ringBuffer.drain(drainArray, drainArray.length);
            for (int i = 0; i < drained; i++) {
                Object obj = drainArray[i];
                drainArray[i] = null;
                if (obj instanceof Registration) {
                    Registration reg = (Registration) obj;
                    try {
                        reg.channel.configureBlocking(false);
                        SelectionKey key = reg.channel.register(selector, reg.ops, reg.context);
                        key.attach(reg.context);
                    } catch (IOException e) {
                        try {
                            reg.channel.close();
                        } catch (IOException ignored) {}
                    }
                }
            }
        }

        private void handleRead(SelectionKey key) throws IOException {
            SocketChannel channel = (SocketChannel) key.channel();
            ChannelContext context = (ChannelContext) key.attachment();
            
            if (context != null && context.isReadSuspended()) {
                key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
                return;
            }

            DirectBuffer buffer =