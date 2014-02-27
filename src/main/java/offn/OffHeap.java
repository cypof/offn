package offn;

import java.lang.reflect.Field;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import sun.misc.Unsafe;

/**
 * Adaptation of Doug Lea's ConcurrentSkipListMap to run on native memory.
 * <nl>
 * http://gee.cs.oswego.edu/cgi-bin/viewcvs.cgi/jsr166/src/main/java/util/concurrent/
 * ConcurrentSkipListMap.java?revision=1.81
 */
@SuppressWarnings({ "unchecked", "restriction" })
public final class OffHeap {

    /**
     * Generates the initial random seed for the cheaper per-instance random number
     * generators used in randomLevel.
     */
    private static final Random seedGenerator = new Random();

    /**
     * Special value used to identify base-level header
     */
    private static final long BASE_HEADER;

    /**
     * The topmost head index of the skiplist.
     */
    private volatile long head;

    /**
     * Seed for simple random number generator. Not volatile since it doesn't matter too
     * much if different threads don't see updates.
     */
    private int randomSeed;

    // ADDED

    /**
     * This class cannot rely on GC to ensure it is safe to reclaim memory after a node
     * has been removed. Instead, when a node is freed, its memory is reclaimed after a
     * delay. TODO Tim Harris's time stamp + thread list.
     */
    static final int FREE_MEMORY_DELAY_MS = 60 * 1000;

    static final boolean STATS = false;
    static final AtomicLong _allocations = STATS ? new AtomicLong() : null;

    // END ADDED

    /**
     * Initializes or resets state. Needed by constructors, clone, clear, readObject. and
     * ConcurrentSkipListSet.clone. (Note that comparator must be separately initialized.)
     */
    final void initialize() {
        randomSeed = seedGenerator.nextInt() | 0x0100; // ensure nonzero
        head = newHeadIndex(newNode(), 0, 0, 1);
    }

    /**
     * compareAndSet head node
     */
    private boolean casHead(long expect, long update) {
        return UNSAFE.compareAndSwapLong(this, headOffset, expect, update);
    }

    /* ---------------- Nodes -------------- */

    private static final int KEY = 0, KEY_LENGTH = 20;

    private static final int VALUE = 24; // 4 padding for alignment

    private static final int NEXT = 32;

    private static final int LENGTH = 40;

    /**
     * Creates a new regular node.
     */
    private static long newNode(byte[] key, long value, long next) {
        long node = UNSAFE.allocateMemory(LENGTH);

        if (STATS)
            _allocations.incrementAndGet();

        /*
         * Apparently there is no way to emulate final field initialization, but nodes and
         * indexes are always added through either volatile writes or CAS, so data will be
         * visible to all threads.
         */

        assert key.length == KEY_LENGTH;

        for (int i = 0; i < KEY_LENGTH; i++)
            UNSAFE.putByte(node + KEY + i, key[i]);

        UNSAFE.putLong(node + VALUE, value);
        UNSAFE.putLong(node + NEXT, next);
        return node;
    }

    /**
     * Head node.
     */
    private static long newNode() {
        long node = UNSAFE.allocateMemory(LENGTH);

        if (STATS)
            _allocations.incrementAndGet();

        UNSAFE.setMemory(node + KEY, KEY_LENGTH, (byte) 0);
        UNSAFE.putLong(node + VALUE, BASE_HEADER);
        UNSAFE.putLong(node + NEXT, 0);
        return node;
    }

    private static byte[] key(long node) {
        byte[] key = new byte[KEY_LENGTH];

        for (int i = 0; i < KEY_LENGTH; i++)
            key[i] = UNSAFE.getByte(node + KEY + i);

        return key;
    }

    private static int compare(byte[] key, long node) {
        assert key.length == KEY_LENGTH;

        for (int i = 0; i < KEY_LENGTH; i++) {
            int x = key[i] & 0xff;
            int y = UNSAFE.getByte(node + KEY + i) & 0xff;

            if (x != y)
                return x < y ? -1 : 1;
        }

        return 0;
    }

    private static long value(long node) {
        assert !marked(node);
        return UNSAFE.getLongVolatile(null, node + VALUE);
    }

    private static long next(long node) {
        assert !marked(node);
        return UNSAFE.getLongVolatile(null, node + NEXT);
    }

    /**
     * compareAndSet value field
     */
    private static boolean casValue(long node, long expect, long update) {
        assert !marked(node) && update != node;
        return UNSAFE.compareAndSwapLong(null, node + VALUE, expect, update);
    }

    /**
     * compareAndSet next field
     */
    private static boolean casNext(long node, long expect, long update) {
        assert !marked(node) && update != node;
        return UNSAFE.compareAndSwapLong(null, node + NEXT, expect, update);
    }

    /**
     * Returns true if this node is the header of base-level list.
     * 
     * @return true if this node is header node
     */
    private static boolean isBaseHeader(long node) {
        return value(node) == BASE_HEADER;
    }

    /**
     * Uses Tim Harris's original design of marking the pointer instead of the marker
     * node. It is possible to mask with 1 as alignment is at least 4.
     * 
     * @param f
     *            the assumed current successor of this node
     * @return true if successful
     */
    private static boolean mark(long node, long f) {
        return casNext(node, f, f | 1);
    }

    private static long unmarked(long node) {
        return node & ~1;
    }

    private static boolean marked(long next) {
        return (next & 1) != 0;
    }

    /**
     * Helps out a deletion by appending marker or unlinking from predecessor. This is
     * called during traversals when value field seen to be null.
     * 
     * @param b
     *            predecessor
     * @param f
     *            successor
     */
    private static void helpDelete(long n, long nu, long b, long f) {
        /*
         * Rechecking links and then doing only one of the help-out stages per call tends
         * to minimize CAS interference among helping threads.
         */
        if (f == next(nu) && n == next(b)) {
            if (!marked(f)) // not already marked
                mark(nu, f);
            else {
                if (casNext(b, n, unmarked(f)))
                    onDelete(nu);
            }
        }
    }

    private static void onDelete(final long block) {
        assert !marked(block);
        int todo;
        // Wait until all threads unlikely to access this location
        // ThreadPool.scheduleOnce(new Runnable() {
        //
        // @Override
        // public void run() {
        // UNSAFE.freeMemory(block);
        //
        // if (Stats.ENABLED)
        // Stats.getInstance().NativeAllocations.decrementAndGet();
        // }
        //
        // }, FREE_MEMORY_DELAY_MS);
    }

    /**
     * Returns value if this node contains a valid key-value pair, else null.
     * 
     * @return this node's value if it isn't a marker or header or is deleted, else null.
     */
    private static long getValidValue(long node) {
        long v = value(node);
        if (v == 0 || v == BASE_HEADER)
            return 0;
        return v;
    }

    /* ---------------- Indexing -------------- */

    private static final int NODE = 0;

    private static final int DOWN = NODE + 8;

    private static final int RIGHT = DOWN + 8;

    private static long node(long index) {
        return UNSAFE.getLong(null, index + NODE);
    }

    private static long down(long index) {
        return UNSAFE.getLong(null, index + DOWN);
    }

    private static long right(long index) {
        return UNSAFE.getLongVolatile(null, index + RIGHT);
    }

    /**
     * Creates index node with given values.
     */
    private static long newIndex(long node, long down, long right) {
        return newIndex(node, down, right, RIGHT + 8);
    }

    private static long newIndex(long node, long down, long right, int length) {
        long index = UNSAFE.allocateMemory(length);

        if (STATS)
            _allocations.incrementAndGet();

        UNSAFE.putLong(index + NODE, node);
        UNSAFE.putLong(index + DOWN, down);
        UNSAFE.putLong(index + RIGHT, right);
        return index;
    }

    /**
     * compareAndSet right field
     */
    private static boolean casRight(long index, long expect, long update) {
        return UNSAFE.compareAndSwapLong(null, index + RIGHT, expect, update);
    }

    /**
     * Returns true if the node this indexes has been deleted.
     * 
     * @return true if indexed node is known to be deleted
     */
    private static boolean indexesDeletedNode(long index) {
        return value(node(index)) == 0;
    }

    /**
     * Tries to CAS newSucc as successor. To minimize races with unlink that may lose this
     * index node, if the node being indexed is known to be deleted, it doesn't try to
     * link in.
     * 
     * @param succ
     *            the expected current successor
     * @param newSucc
     *            the new successor
     * @return true if successful
     */
    private static boolean link(long index, long succIndex, long newSuccIndex) {
        long n = node(index);
        UNSAFE.putLong(newSuccIndex + RIGHT, succIndex);
        return value(n) != 0 && casRight(index, succIndex, newSuccIndex);
    }

    /**
     * Tries to CAS right field to skip over apparent successor succ. Fails (forcing a
     * retraversal by caller) if this node is known to be deleted.
     * 
     * @param succ
     *            the expected current successor
     * @return true if successful
     */
    private static boolean unlink(long index, long succIndex) {
        boolean result = !indexesDeletedNode(index) && casRight(index, succIndex, right(succIndex));

        if (result)
            onDelete(succIndex);

        return result;
    }

    /* ---------------- Head nodes -------------- */

    private static final int LEVEL = RIGHT + 8;

    private static long newHeadIndex(long node, long down, long right, int level) {
        long index = newIndex(node, down, right, LEVEL + 4);
        UNSAFE.putInt(index + LEVEL, level);
        return index;
    }

    private static int level(long index) {
        return UNSAFE.getInt(index + LEVEL);
    }

    /* ---------------- Traversal -------------- */

    /**
     * Returns a base-level node with key strictly less than given key, or the base-level
     * header if there is no such node. Also unlinks indexes to deleted nodes found along
     * the way. Callers rely on this side-effect of clearing indices to deleted nodes.
     * 
     * @param key
     *            the key
     * @return a predecessor of key
     */
    private final long findPredecessor(byte[] key) {
        for (;;) {
            long q = head;
            long r = right(q);
            for (;;) {
                if (r != 0) {
                    long n = node(r);
                    if (value(n) == 0) {
                        if (!unlink(q, r))
                            break; // restart
                        r = right(q); // reread r
                        continue;
                    }
                    if (compare(key, n) > 0) {
                        q = r;
                        r = right(r);
                        continue;
                    }
                }
                long d = down(q);
                if (d != 0) {
                    q = d;
                    r = right(d);
                } else
                    return node(q);
            }
        }
    }

    /**
     * Returns node holding key or null if no such, clearing out any deleted nodes seen
     * along the way. Repeatedly traverses at base-level looking for key starting at
     * predecessor returned from findPredecessor, processing base-level deletions as
     * encountered. Some callers rely on this side-effect of clearing deleted nodes.
     * Restarts occur, at traversal step centered on node n, if: (1) After reading n's
     * next field, n is no longer assumed predecessor b's current successor, which means
     * that we don't have a consistent 3-node snapshot and so cannot unlink any subsequent
     * deleted nodes encountered. (2) n's value field is null, indicating n is deleted, in
     * which case we help out an ongoing structural deletion before retrying. Even though
     * there are cases where such unlinking doesn't require restart, they aren't sorted
     * out here because doing so would not usually outweigh cost of restarting. (3) n is a
     * marker or n's predecessor's value field is null, indicating (among other
     * possibilities) that findPredecessor returned a deleted node. We can't unlink the
     * node because we don't know its predecessor, so rely on another call to
     * findPredecessor to notice and return some earlier predecessor, which it will do.
     * This check is only strictly needed at beginning of loop, (and the b.value check
     * isn't strictly needed at all) but is done each iteration to help avoid contention
     * with other threads by callers that will fail to be able to change links, and so
     * will retry anyway. The traversal loops in doPut, doRemove, and findNear all include
     * the same three kinds of checks. And specialized versions appear in findFirst, and
     * findLast and their variants. They can't easily share code because each uses the
     * reads of fields held in locals occurring in the orders they were performed.
     * 
     * @param key
     *            the key
     * @return node holding key, or null if no such
     */
    private final long findNode(byte[] key) {
        for (;;) {
            long b = findPredecessor(key);
            long n = next(b);
            for (;;) {
                if (n == 0)
                    return 0;
                long nu = unmarked(n);
                long f = next(nu);
                if (n != next(b)) // inconsistent read
                    break;
                long v = value(nu);
                if (v == 0) { // n is deleted
                    helpDelete(n, nu, b, f);
                    break;
                }
                if (n != nu || value(b) == 0) // b is deleted
                    break;
                int c = compare(key, n);
                if (c == 0)
                    return n;
                if (c < 0)
                    return 0;
                b = n;
                n = f;
            }
        }
    }

    /**
     * Gets value for key using findNode.
     * 
     * @param okey
     *            the key
     * @return the value, or null if absent
     */
    private final long doGet(byte[] key) {
        /*
         * Loop needed here and elsewhere in case value field goes null just as it is
         * about to be returned, in which case we lost a race with a deletion, so must
         * retry.
         */
        for (;;) {
            long n = findNode(key);
            if (n == 0)
                return 0;
            long v = value(n);
            if (v != 0)
                return v;
        }
    }

    /* ---------------- Insertion -------------- */

    /**
     * Main insertion method. Adds element if not present, or replaces value if present
     * and onlyIfAbsent is false.
     * 
     * @param kkey
     *            the key
     * @param value
     *            the value that must be associated with key
     * @param onlyIfAbsent
     *            if should not insert if already present
     * @return the old value, or null if newly inserted
     */
    private final long doPut(byte[] key, long value, boolean onlyIfAbsent) {
        for (;;) {
            long b = findPredecessor(key);
            long n = next(b);
            for (;;) {
                if (n != 0) {
                    long nu = unmarked(n);
                    long f = next(nu);
                    if (n != next(b)) // inconsistent read
                        break;
                    long v = value(nu);
                    if (v == 0) { // n is deleted
                        helpDelete(n, nu, b, f);
                        break;
                    }
                    if (n != nu || value(b) == 0) // b is deleted
                        break;
                    int c = compare(key, n);
                    if (c > 0) {
                        b = n;
                        n = f;
                        continue;
                    }
                    if (c == 0) {
                        if (onlyIfAbsent || casValue(n, v, value))
                            return v;
                        else
                            break; // restart if lost race to replace value
                    }
                    // else c < 0; fall through
                }

                long z = newNode(key, value, n);
                if (!casNext(b, n, z))
                    break; // restart if lost race to append to b
                int level = randomLevel();
                if (level > 0)
                    insertIndex(z, level);
                return 0;
            }
        }
    }

    /**
     * Returns a random level for inserting a new node. Hardwired to k=1, p=0.5, max 31
     * (see above and Pugh's "Skip List Cookbook", sec 3.4). This uses the simplest of the
     * generators described in George Marsaglia's "Xorshift RNGs" paper. This is not a
     * high-quality generator but is acceptable here.
     */
    private final int randomLevel() {
        int x = randomSeed;
        x ^= x << 13;
        x ^= x >>> 17;
        randomSeed = x ^= x << 5;
        if ((x & 0x80000001) != 0) // test highest and lowest bits
            return 0;
        int level = 1;
        while (((x >>>= 1) & 1) != 0)
            ++level;
        return level;
    }

    /**
     * Creates and adds index nodes for the given node.
     * 
     * @param z
     *            the node
     * @param level
     *            the level of the index
     */
    private final void insertIndex(long z, int level) {
        long h = head;
        int max = level(h);

        if (level <= max) {
            long idx = 0;
            for (int i = 1; i <= level; ++i)
                idx = newIndex(z, idx, 0);
            addIndex(idx, h, level);

        } else { // Add a new level
            /*
             * To reduce interference by other threads checking for empty levels in
             * tryReduceLevel, new levels are added with initialized right pointers. Which
             * in turn requires keeping levels in an array to access them while creating
             * new head index nodes from the opposite direction.
             */
            level = max + 1;
            long[] idxs = new long[level + 1];
            long idx = 0;
            for (int i = 1; i <= level; ++i)
                idxs[i] = idx = newIndex(z, idx, 0);

            long oldh;
            int k;
            for (;;) {
                oldh = head;
                int oldLevel = level(oldh);
                if (level <= oldLevel) { // lost race to add level
                    k = level;
                    break;
                }
                long newh = oldh;
                long oldbase = node(oldh);
                for (int j = oldLevel + 1; j <= level; ++j)
                    newh = newHeadIndex(oldbase, newh, idxs[j], j);
                if (casHead(oldh, newh)) {
                    k = oldLevel;
                    break;
                }
            }
            addIndex(idxs[k], oldh, k);
        }
    }

    /**
     * Adds given index nodes from given level down to 1.
     * 
     * @param idx
     *            the topmost index node being inserted
     * @param h
     *            the value of head to use to insert. This must be snapshotted by callers
     *            to provide correct insertion level
     * @param indexLevel
     *            the level of the index
     */
    private final void addIndex(long idx, long h, int indexLevel) {
        // Track next level to insert in case of retries
        int insertionLevel = indexLevel;
        byte[] key = key(node(idx));
        // Similar to findPredecessor, but adding index nodes along
        // path to key.
        for (;;) {
            int j = level(h);
            long q = h;
            long r = right(q);
            long t = idx;
            for (;;) {
                if (r != 0) {
                    long n = node(r);
                    // compare before deletion check avoids needing recheck
                    int c = compare(key, n);
                    if (value(n) == 0) {
                        if (!unlink(q, r))
                            break;
                        r = right(q);
                        continue;
                    }
                    if (c > 0) {
                        q = r;
                        r = right(r);
                        continue;
                    }
                }

                if (j == insertionLevel) {
                    // Don't insert index if node already deleted
                    if (indexesDeletedNode(t)) {
                        findNode(key); // cleans up
                        return;
                    }
                    if (!link(q, r, t))
                        break; // restart
                    if (--insertionLevel == 0) {
                        // need final deletion check before return
                        if (indexesDeletedNode(t))
                            findNode(key);
                        return;
                    }
                }

                if (--j >= insertionLevel && j < indexLevel)
                    t = down(t);
                q = down(q);
                r = right(q);
            }
        }
    }

    /* ---------------- Deletion -------------- */

    /**
     * Main deletion method. Locates node, nulls value, appends a deletion marker, unlinks
     * predecessor, removes associated index nodes, and possibly reduces head index level.
     * Index nodes are cleared out simply by calling findPredecessor. which unlinks
     * indexes to deleted nodes found along path to key, which will include the indexes to
     * this node. This is done unconditionally. We can't check beforehand whether there
     * are index nodes because it might be the case that some or all indexes hadn't been
     * inserted yet for this node during initial search for it, and we'd like to ensure
     * lack of garbage retention, so must call to be sure.
     * 
     * @param okey
     *            the key
     * @param value
     *            if non-null, the value that must be associated with key
     * @return the node, or null if not found
     */
    private final long doRemove(byte[] key, long value) {
        for (;;) {
            long b = findPredecessor(key);
            long n = next(b);
            for (;;) {
                if (n == 0)
                    return 0;
                long nu = unmarked(n);
                long f = next(nu);
                if (n != next(b)) // inconsistent read
                    break;
                long v = value(nu);
                if (v == 0) { // n is deleted
                    helpDelete(n, nu, b, f);
                    break;
                }
                if (n != nu || value(b) == 0) // b is deleted
                    break;
                int c = compare(key, n);
                if (c < 0)
                    return 0;
                if (c > 0) {
                    b = n;
                    n = f;
                    continue;
                }
                if (value != 0 && value != v)
                    return 0;
                if (!casValue(n, v, 0))
                    break;
                if (!mark(n, f) || !casNext(b, n, f))
                    findNode(key); // Retry via findNode
                else {
                    onDelete(n);
                    findPredecessor(key); // Clean index
                    if (right(head) == 0)
                        tryReduceLevel();
                }
                return v;
            }
        }
    }

    /**
     * Possibly reduce head level if it has no nodes. This method can (rarely) make
     * mistakes, in which case levels can disappear even though they are about to contain
     * index nodes. This impacts performance, not correctness. To minimize mistakes as
     * well as to reduce hysteresis, the level is reduced by one only if the topmost three
     * levels look empty. Also, if the removed level looks non-empty after CAS, we try to
     * change it back quick before anyone notices our mistake! (This trick works pretty
     * well because this method will practically never make mistakes unless current thread
     * stalls immediately before first CAS, in which case it is very unlikely to stall
     * again immediately afterwards, so will recover.) We put up with all this rather than
     * just let levels grow because otherwise, even a small map that has undergone a large
     * number of insertions and removals will have a lot of levels, slowing down access
     * more than would an occasional unwanted reduction.
     */
    private final void tryReduceLevel() {
        long h = head;
        long d;
        long e;
        if (level(h) > 3 && //
                (d = down(h)) != 0 && //
                (e = down(d)) != 0 && //
                right(e) == 0 && //
                right(d) == 0 && //
                right(h) == 0 && //
                casHead(h, d) && // try to set
                right(h) != 0) // recheck
            casHead(d, h); // try to backout
    }

    /* ---------------- Constructors -------------- */

    /**
     * Constructs a new, empty map, sorted according to the {@linkplain Comparable natural
     * ordering} of the keys.
     */
    public OffHeap() {
        initialize();
    }

    /**
     * Returns the value to which the specified key is mapped, or {@code null} if this map
     * contains no mapping for the key.
     * <p>
     * More formally, if this map contains a mapping from a key {@code k} to a value
     * {@code v} such that {@code key} compares equal to {@code k} according to the map's
     * ordering, then this method returns {@code v}; otherwise it returns {@code null}.
     * (There can be at most one such mapping.)
     * 
     * @throws ClassCastException
     *             if the specified key cannot be compared with the keys currently in the
     *             map
     * @throws NullPointerException
     *             if the specified key is null
     */
    public long get(byte[] key) {
        return doGet(key);
    }

    /**
     * Associates the specified value with the specified key in this map. If the map
     * previously contained a mapping for the key, the old value is replaced.
     * 
     * @param key
     *            key with which the specified value is to be associated
     * @param value
     *            value to be associated with the specified key
     * @return the previous value associated with the specified key, or <tt>null</tt> if
     *         there was no mapping for the key
     * @throws ClassCastException
     *             if the specified key cannot be compared with the keys currently in the
     *             map
     * @throws NullPointerException
     *             if the specified key or value is null
     */
    public long put(byte[] key, long value) {
        if (value == 0)
            throw new IllegalArgumentException();

        return doPut(key, value, false);
    }

    /**
     * Removes the mapping for the specified key from this map if present.
     * 
     * @param key
     *            key for which mapping should be removed
     * @return the previous value associated with the specified key, or <tt>null</tt> if
     *         there was no mapping for the key
     * @throws ClassCastException
     *             if the specified key cannot be compared with the keys currently in the
     *             map
     * @throws NullPointerException
     *             if the specified key is null
     */
    public long remove(byte[] key) {
        return doRemove(key, 0);
    }

    /*
     *
     */
    static final Unsafe UNSAFE;

    private static final long headOffset;

    static {
        UNSAFE = getUnsafe();
        BASE_HEADER = UNSAFE.allocateMemory(1);

        try {
            Class<?> k = OffHeap.class;
            headOffset = UNSAFE.objectFieldOffset(k.getDeclaredField("head"));
        } catch (NoSuchFieldException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static Unsafe getUnsafe() {
        try {
            return Unsafe.getUnsafe();
        } catch (SecurityException se) {
            try {
                return java.security.AccessController.doPrivileged(new java.security.PrivilegedExceptionAction<Unsafe>() {

                    public Unsafe run() throws Exception {
                        return getUnsafePrivileged();
                    }

                });
            } catch (java.security.PrivilegedActionException e) {
                throw new RuntimeException(e.getCause());
            }
        }
    }

    private static Unsafe getUnsafePrivileged() throws NoSuchFieldException, IllegalAccessException {
        Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        return (Unsafe) f.get(null);
    }

}
