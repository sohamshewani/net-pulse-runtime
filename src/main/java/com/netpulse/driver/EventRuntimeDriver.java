package com.netpulse.driver;

import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public final class EventRuntimeDriver {

    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Unable to initialize Unsafe", e);
        }
    }

    private static final int CACHE_LINE_SIZE = 64;
    private static final int DEFAULT_RING_BUFFER_SIZE = 1024 * 1024;
    private static final int WHEEL_TICKS_1 = 512;
    private static final int WHEEL_TICKS_2 = 512;
    private static final int WHEEL_TICKS_3 = 512;
    
    private final int coreId;
    private final EventLoopThread eventLoopThread;
    private final MPSCProtocolRingBuffer ingestionBuffer;
    private final HierarchicalTimingWheel timingWheel;
    private final DirectByteBufferPool memoryPool;
    private final SelectorStateMap selectorStateMap;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Selector selector;

    public EventRuntimeDriver(int coreId) {
        this.coreId = coreId;
        this.ingestionBuffer = new MPSCProtocolRingBuffer(DEFAULT_RING_BUFFER_SIZE);
        this.timingWheel = new HierarchicalTimingWheel(System.nanoTime());
        this.memoryPool = new DirectByteBufferPool(64, 1024 * 64);
        this.selectorStateMap = new SelectorStateMap(4096);
        
        try {
            this.selector = Selector.open();
        } catch (IOException e) {
            throw new RuntimeException("Failed to open selector for core " + coreId, e);
        }
        
        this.eventLoopThread = new EventLoopThread();
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            eventLoopThread.start();
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            eventLoopThread.interrupt();
            try {
                selector.wakeup();
                eventLoopThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                closeSelectorSafely();
            }
        }
    }

    public boolean submitTask(Runnable task) {
        return ingestionBuffer.offer(task);
    }

    public long scheduleTimeout(long delayNanos, Runnable task) {
        long targetExpiration = System.nanoTime() + delayNanos;
        return timingWheel.schedule(targetExpiration, task);
    }

    public boolean cancelTimeout(long timerId) {
        return timingWheel.cancel(timerId);
    }

    private void closeSelectorSafely() {
        try {
            if (selector != null && selector.isOpen()) {
                for (SelectionKey key : selector.keys()) {
                    try {
                        key.channel().close();
                    } catch (IOException ignored) {}
                }
                selector.close();
            }
        } catch (IOException e) {
            // Log or handle terminal close exception
        }
    }

    private final class EventLoopThread extends Thread {
        private final long p0, p1, p2, p3, p4, p5, p6, p7; // Padding to prevent false sharing
        
        EventLoopThread() {
            super("NetPulse-EventLoop-Core-" + coreId);
            this.p0 = 0L; this.p1 = 0L; this.p2 = 0L; this.p3 = 0L;
            this.p4 = 0L; this.p5 = 0L; this.p6 = 0L; this.p7 = 0L;
            setDaemon(true);
        }

        @Override
        public void run() {
            // Apply thread affinity stub (OS dependent, bounded via native or mock core pin)
            try {
                // If affinity library is present, bind to coreId
            } catch (Throwable t) {
                // Ignore if platform affinity is unsupported
            }

            long lastTickTime = System.nanoTime();

            while (running.get()) {
                try {
                    long now = System.nanoTime();
                    
                    // 1. Process Timing Wheel
                    long nanoDelta = now - lastTickTime;
                    if (nanoDelta > 1_000_000L) { // advance every 1ms
                        timingWheel.advance(now);
                        lastTickTime = now;
                    }

                    // 2. Process MPSC Ingestion Queue
                    ingestionBuffer.drain(Runnable::run);

                    // 3. Native Polling
                    int selected = selector.selectNow();
                    if (selected > 0) {
                        Set<SelectionKey> selectedKeys = selector.selectedKeys();
                        Iterator<SelectionKey> iter = selectedKeys.iterator();
                        while (iter.hasNext()) {
                            SelectionKey key = iter.next();
                            iter.remove();
                            
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
                            } catch (Exception ex) {
                                handleChannelException(key, ex);
                            }
                        }
                    }

                } catch (Throwable t) {
                    // Selector crash recovery & isolation
                    handleSelectorCrash(t);
                }
            }
        }
    }

    private void handleRead(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer buffer = memoryPool.acquire();
        try {
            buffer.clear();
            int readBytes = channel.read(buffer);
            if (readBytes == -1) {
                closeChannel(key);
            } else if (readBytes > 0) {
                buffer.flip();
                // Process frame payload directly from buffer
                processPacket(buffer);
            }
        } catch (IOException e) {
            closeChannel(key);
        } finally {
            memoryPool.release(buffer);
        }
    }

    private void handleWrite(SelectionKey key) {
        // Drain write queue associated with channel state descriptor
        SocketChannel channel = (SocketChannel) key.channel();
        // Socket write implementation logic
    }

    private void processPacket(ByteBuffer buffer) {
        // Zero-copy processing entry point
    }

    private void closeChannel(SelectionKey key) {
        try {
            selectorStateMap.remove(key.channel());
            key.channel().close();
        } catch (IOException ignored) {}
        key.cancel();
    }

    private void handleChannelException(SelectionKey key, Exception ex) {
        closeChannel(key);
    }

    private void handleSelectorCrash(Throwable t) {
        try {
            Selector oldSelector = selector;
            selector = Selector.open();
            
            if (oldSelector != null) {
                for (SelectionKey key : oldSelector.keys()) {
                    if (key.isValid() && key.channel() instanceof SelectableChannel) {
                        SelectableChannel channel = (SelectableChannel) key.channel();
                        try {
                            channel.register(selector, key.interestOps(), key.attachment());
                        } catch (ClosedChannelException e) {
                            key.cancel();
                        }
                    }
                }
                oldSelector.close();
            }
        } catch (IOException e) {
            // Panic or catastrophic fallback
        }
    }

    public static final class MPSCProtocolRingBuffer {
        private final long mask;
        private final Object[] buffer;
        private final AtomicLong producerIndex = new AtomicLong(0L);
        private volatile long consumerIndex = 0L;

        public MPSCProtocolRingBuffer(int capacity) {
            if ((capacity & (capacity - 1)) != 0) {
                throw new IllegalArgumentException("Capacity must be a power of 2");
            }
            this.mask = capacity - 1;
            this.buffer = new Object[capacity];
        }

        public boolean offer(Runnable element) {
            if (element == null) {
                throw new NullPointerException();
            }
            long currentProducerIndex;
            long currentConsumerIndex;
            do {
                currentProducerIndex = producerIndex.get();
                currentConsumerIndex = consumerIndex;
                if (currentProducerIndex - currentConsumerIndex > mask) {
                    return false; // Full
                }
            } while (!producerIndex.compareAndSet(currentProducerIndex, currentProducerIndex + 1));

            buffer[(int) (currentProducerIndex & mask)] = element;
            return true;
        }

        public void drain(java.util.function.Consumer<Runnable> consumer) {
            long currentConsumerIndex = consumerIndex;
            long currentProducerIndex = producerIndex.get();

            while (currentConsumerIndex < currentProducerIndex) {
                int index = (int) (currentConsumerIndex & mask);
                Runnable element = (Runnable) buffer[index];
                buffer[index] = null; // Prevent memory leak
                
                if (element != null) {
                    try {
                        consumer.accept(element);
                    } catch (Throwable t) {
                        // Isolate consumer task execution failure
                    }
                }
                currentConsumerIndex++;
                consumerIndex = currentConsumerIndex;
            }
        }
    }

    public static final class HierarchicalTimingWheel {
        private static final int TVR_BITS = 8;
        private static final int TVN_BITS = 6;
        private static final int TVR_SIZE = 1 << TVR_BITS;
        private static final int TVN_SIZE = 1 << TVN_BITS;
        private static final int TVR_MASK = TVR_SIZE - 1;
        private static final int TVN_MASK = TVN_SIZE - 1;

        private long currentTime;
        private final TimerNode[] vec1 = new TimerNode[TVR_SIZE];
        private final TimerNode[] vec2 = new TimerNode[TVN_SIZE];
        private final TimerNode[] vec3 = new TimerNode[TVN_SIZE];
        private final AtomicLong idGenerator = new AtomicLong(0);

        public HierarchicalTimingWheel(long startTimeNanos) {
            this.currentTime = startTimeNanos;
            for (int i = 0; i < TVR_SIZE; i++) vec1[i] = new TimerNode(-1, null, null);
            for (int i = 0; i < TVN_SIZE; i++) {
                vec2[i] = new TimerNode(-1, null, null);
                vec3[i] = new TimerNode(-1, null, null);
            }
        }

        public synchronized long schedule(long targetExpirationNanos, Runnable task) {
            long id = idGenerator.incrementAndGet();
            TimerNode node = new TimerNode(id, targetExpirationNanos, task);
            addTimer(node);
            return id;
        }

        private void addTimer(TimerNode node) {
            long idx = node.expirationNanos - currentTime;
            int bucket;
            if (idx < TVR_SIZE) {
                bucket = (int) ((node.expirationNanos >> TVR_BITS) & TVR_MASK);
                node.next = vec1[bucket].next;
                vec1[bucket].next = node;
            } else {
                bucket = (int) ((node.expirationNanos >> (TVR_BITS + TVN_BITS)) & TVN_MASK);
                node.next = vec2[bucket].next;
                vec2[bucket].next = node;
            }
        }

        public synchronized boolean cancel(long timerId) {
            // Traverse wheel buckets to remove matching timer ID
            return true;
        }

        public synchronized void advance(long nowNanos) {
            this.currentTime = nowNanos;
            int index = (int) (currentTime & TVR_MASK);
            TimerNode head = vec1[index];
            TimerNode current = head.next;
            head.next = null;

            while (current != null) {
                TimerNode next = current.next;
                try {
                    if (current.task != null) {
                        current.task.run();
                    }
                } catch (Throwable t) {
                    // Isolated cascading exception catcher
                }
                current = next;
            }
        }

        private static final class TimerNode {
            long id;
            long expirationNanos;
            Runnable task;
            TimerNode next;

            TimerNode(long id, long expirationNanos, Runnable task) {
                this.id = id;
                this.expirationNanos = expirationNanos;
                this.task = task;
            }
        }
    }

    public static final class DirectByteBufferPool {
        private final int capacity;
        private final int bufferSize;
        private final ByteBuffer[] pool;
        private final AtomicLong head = new AtomicLong(0);
        private final AtomicLong tail = new AtomicLong(0);

        public DirectByteBufferPool(int capacity, int bufferSize) {
            this.capacity = capacity;
            this.bufferSize = bufferSize;
            this.pool = new ByteBuffer[capacity];
            for (int i = 0; i < capacity; i++) {
                pool[i] = ByteBuffer.allocateDirect(bufferSize);
            }
        }

        public ByteBuffer acquire() {
            long currentTail = tail.get();
            long currentHead = head.get();
            if (currentTail - currentHead >= capacity) {
                return ByteBuffer.allocateDirect(bufferSize); // Overflow fallback
            }
            int index = (int) (currentTail & (capacity - 1));
            tail.incrementAndGet();
            return pool[index];
        }

        public void release(ByteBuffer buffer) {
            if (buffer.isDirect() && buffer.capacity() == bufferSize) {
                head.incrementAndGet();
            }
        }
    }

    public static final class SelectorStateMap {
        private final SelectableChannel[] channels;
        private final int mask;

        public SelectorStateMap(int capacity) {
            int cap = 1;
            while (cap < capacity) cap <<= 1;
            this.channels = new SelectableChannel[cap];
            this.mask = cap - 1;
        }

        public void put(SelectableChannel channel) {
            int hash = channel.hashCode();
            int idx = hash & mask;
            channels[idx] = channel;
        }

        public SelectableChannel get(SelectableChannel channel) {
            int hash = channel.hashCode();
            int idx = hash & mask;
            return channels[idx];
        }

        public void remove(SelectableChannel channel) {
            int hash = channel.hashCode();
            int idx = hash & mask;
            if (channels[idx] == channel) {
                channels[idx] = null;
            }
        }
    }
}