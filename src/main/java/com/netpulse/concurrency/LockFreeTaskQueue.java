package com.netpulse.concurrency;

import sun.misc.Unsafe;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

@sun.misc.Contended
public final class LockFreeTaskQueue<T> {

    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new AssertionError("Unable to initialize Unsafe", e);
        }
    }

    private static final int CACHE_LINE_SIZE = 64;
    private static final long ARRAY_BASE;
    private static final int ARRAY_SHIFT;

    static {
        Class<?> cls = Object[].class;
        ARRAY_BASE = UNSAFE.arrayBaseOffset(cls);
        int scale = UNSAFE.arrayIndexScale(cls);
        if ((scale & (scale - 1)) != 0) {
            throw new AssertionError("Data scale not a power of two");
        }
        ARRAY_SHIFT = 31 - Integer.numberOfLeadingZeros(scale);
    }

    private final int capacity;
    private final int mask;
    private final Object[] buffer;

    @sun.misc.Contended
    private volatile long head = 0L;

    @sun.misc.Contended
    private volatile long tail = 0L;

    @sun.misc.Contended
    private long consumerCursor = 0L;

    private static final AtomicLongFieldUpdater<LockFreeTaskQueue> HEAD_UPDATER =
            AtomicLongFieldUpdater.newUpdater(LockFreeTaskQueue.class, "head");

    private static final AtomicLongFieldUpdater<LockFreeTaskQueue> TAIL_UPDATER =
            AtomicLongFieldUpdater.newUpdater(LockFreeTaskQueue.class, "tail");

    public LockFreeTaskQueue(int requestedCapacity) {
        if (requestedCapacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        int p2 = 1;
        while (p2 < requestedCapacity) {
            p2 <<= 1;
        }
        this.capacity = p2;
        this.mask = p2 - 1;
        this.buffer = new Object[p2];
    }

    public boolean offer(T e) {
        if (e == null) {
            throw new NullPointerException("Element cannot be null");
        }

        long currentTail;
        long currentHead;
        
        do {
            currentTail = tail;
            currentHead = head;
            
            if (currentTail - currentHead >= capacity) {
                return false; 
            }
        } while (!TAIL_UPDATER.compareAndSet(this, currentTail, currentTail + 1));

        int index = (int) currentTail & mask;
        long offset = ARRAY_BASE + ((long) index << ARRAY_SHIFT);
        
        UNSAFE.putOrderedObject(buffer, offset, e);
        return true;
    }

    public T poll() {
        long currentConsumerCursor = consumerCursor;
        long currentHead = head;

        if (currentConsumerCursor >= currentHead) {
            currentHead = head;
            if (currentConsumerCursor >= currentHead) {
                return null;
            }
        }

        int index = (int) currentConsumerCursor & mask;
        long offset = ARRAY_BASE + ((long) index << ARRAY_SHIFT);
        
        @SuppressWarnings("unchecked")
        T e = (T) UNSAFE.getObjectVolatile(buffer, offset);
        if (e == null) {
            return null;
        }

        UNSAFE.putObject(buffer, offset, null);
        consumerCursor = currentConsumerCursor + 1;
        return e;
    }

    public T peek() {
        long currentConsumerCursor = consumerCursor;
        long currentHead = head;

        if (currentConsumerCursor >= currentHead) {
            currentHead = head;
            if (currentConsumerCursor >= currentHead) {
                return null;
            }
        }

        int index = (int) currentConsumerCursor & mask;
        long offset = ARRAY_BASE + ((long) index << ARRAY_SHIFT);
        
        @SuppressWarnings("unchecked")
        T e = (T) UNSAFE.getObjectVolatile(buffer, offset);
        return e;
    }

    public void publish() {
        long currentTail = tail;
        long currentHead;
        do {
            currentHead = head;
            if (currentHead >= currentTail) {
                break;
            }
        } while (!HEAD_UPDATER.compareAndSet(this, currentHead, currentTail));
    }

    public int size() {
        long currentHead = head;
        long currentTail = tail;
        long size = currentTail - currentHead;
        if (size < 0) {
            return 0;
        }
        return size > capacity ? capacity : (int) size;
    }

    public boolean isEmpty() {
        return consumerCursor >= head && head == tail;
    }

    public int capacity() {
        return capacity;
    }
}