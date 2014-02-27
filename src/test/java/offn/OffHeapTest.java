/**
 * This file is part of ObjectFabric (http://objectfabric.org).
 *
 * ObjectFabric is licensed under the Apache License, Version 2.0, the terms
 * of which may be found at http://www.apache.org/licenses/LICENSE-2.0.html.
 * 
 * Copyright ObjectFabric Inc.
 * 
 * This file is provided AS IS with NO WARRANTY OF ANY KIND, INCLUDING THE
 * WARRANTY OF DESIGN, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.
 */

package offn;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CyclicBarrier;

import org.junit.Assert;
import org.junit.Test;

public class OffHeapTest {
  interface Map {
    long get(byte[] key);

    long put(byte[] key, long value);

    long remove(byte[] key);
  }

  @Test
  public void testRef() throws Exception {
    final ConcurrentSkipListMap<ArrayWrapper, Long> ref = new ConcurrentSkipListMap<ArrayWrapper, Long>();

    Map map = new Map() {

      @Override
      public long get(byte[] key) {
        return ref.get(new ArrayWrapper(key));
      }

      @Override
      public long put(byte[] key, long value) {
        Long r = ref.put(new ArrayWrapper(key), value);
        return r != null ? r : 0;
      }

      @Override
      public long remove(byte[] key) {
        return ref.remove(new ArrayWrapper(key));
      }
    };

    for( int i = 0; i < 10; i++ )
      run(4, 1000, map);
  }

  @Test
  public void testOffHeap() throws Exception {
    final OffHeap test = new OffHeap();

    Map map = new Map() {

      @Override
      public long get(byte[] key) {
        return test.get(key);
      }

      @Override
      public long put(byte[] key, long value) {
        return test.put(key, value);
      }

      @Override
      public long remove(byte[] key) {
        return test.remove(key);
      }
    };

    for( int i = 0; i < 10; i++ )
      run(4, 1000, map);
  }

  private void run(int threads, final int writes, final Map map) throws Exception {
    final CyclicBarrier barrier = new CyclicBarrier(threads);
    ArrayList<Thread> joins = new ArrayList<Thread>();
    final SecureRandom rand = new SecureRandom();

    for( int t = 0; t < threads; t++ ) {
      Thread thread = new Thread() {

        @Override
        public void run() {
          ArrayList<Tuple> tuples = new ArrayList<Tuple>();

          try {
            barrier.await();
          } catch( Exception e ) {
            throw new RuntimeException(e);
          }

          for( int i = 1; i < writes; i++ ) {
            byte[] array = new byte[20];
            rand.nextBytes(array);
            tuples.add(new Tuple(array, i));
            map.put(array, i);
          }

          for( int i = tuples.size() - 1; i >= 0; i-- ) {
            if( rand.nextInt(10) == 0 ) {
              Tuple tuple = tuples.remove(i);
              long value = map.remove(tuple.Key);
              Assert.assertEquals(tuple.Value, value);
            }
          }

          if( OffHeap.STATS )
            System.out.println(OffHeap._allocations.get());

          for( int i = tuples.size() - 1; i >= 0; i-- ) {
            Tuple tuple = tuples.get(i);
            Assert.assertEquals(tuple.Value, map.get(tuple.Key));
          }

          for( int i = tuples.size() - 1; i >= 0; i-- ) {
            Tuple tuple = tuples.remove(i);
            Assert.assertEquals(tuple.Value, map.remove(tuple.Key));
          }

          if( OffHeap.STATS )
            System.out.println(OffHeap._allocations.get());
        }
      };

      thread.start();
      joins.add(thread);
    }

    for( Thread thread : joins )
      thread.join();

    // Thread.sleep(SkipList.FREE_MEMORY_DELAY_MS);
    Thread.sleep(100);

    if( OffHeap.STATS ) {
      long remaining = OffHeap._allocations.get();
      Assert.assertTrue(remaining < 100); // Head and a few indexes remain
    }
  }

  private static final class Tuple {

    final byte[] Key;

    final long Value;

    public Tuple(byte[] key, long value) {
      Key = key;
      Value = value;
    }
  }

  private static final class ArrayWrapper implements Comparable {

    final byte[] Data;

    public ArrayWrapper(byte[] data) {
      if( data == null )
        throw new NullPointerException();

      Data = data;
    }

    @Override
    public boolean equals(Object other) {
      if( !(other instanceof ArrayWrapper) )
        return false;

      return Arrays.equals(Data, ((ArrayWrapper) other).Data);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(Data);
    }

    @Override
    public int compareTo(Object o) {
      ArrayWrapper other = (ArrayWrapper) o;

      for( int i = 0; i < Data.length; i++ ) {
        byte a = this.Data[i];
        byte b = other.Data[i];

        if( a != b )
          return a < b ? -1 : 1;
      }

      return 0;
    }
  }
}
