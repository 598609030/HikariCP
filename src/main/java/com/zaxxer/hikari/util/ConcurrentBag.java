/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
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
package com.zaxxer.hikari.util;

import static java.lang.Thread.yield;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

import static com.zaxxer.hikari.util.ClockSource.currentTime;
import static com.zaxxer.hikari.util.ClockSource.elapsedNanos;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_IN_USE;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_NOT_IN_USE;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_REMOVED;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_RESERVED;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry;

/**
 * 这是一个专门的并发包，可以为连接池实现LinkedBlockingQueue和LinkedTransferQueue的卓越性能。
 * 它尽可能使用ThreadLocal存储来避免锁定，但如果ThreadLocal列表中没有可用的项目，则会使用它来扫描公共集合。
 * 当借用线程没有自己的东西时，ThreadLocal列表中的未使用项可能被“窃取”。
 * 它是一个“无锁”实现，使用专门的AbstractQueuedLongSynchronizer来管理跨线程信令。
 *
 * 请注意，从包中“借用”的项目实际上并未从任何集合中删除，因此即使放弃引用也不会发生垃圾收集。
 * 因此，必须小心“回收”“借用”借来的对象，否则将导致内存泄漏。 只有“删除”方法才能从包中完全删除对象。
 *
 * This is a specialized concurrent bag that achieves superior performance
 * to LinkedBlockingQueue and LinkedTransferQueue for the purposes of a
 * connection pool.  It uses ThreadLocal storage when possible to avoid
 * locks, but resorts to scanning a common collection if there are no
 * available items in the ThreadLocal list.  Not-in-use items in the
 * ThreadLocal lists can be "stolen" when the borrowing thread has none
 * of its own.  It is a "lock-less" implementation using a specialized
 * AbstractQueuedLongSynchronizer to manage cross-thread signaling.
 *
 * Note that items that are "borrowed" from the bag are not actually
 * removed from any collection, so garbage collection will not occur
 * even if the reference is abandoned.  Thus care must be taken to
 * "requite" borrowed objects otherwise a memory leak will result.  Only
 * the "remove" method can completely remove an object from the bag.
 *
 * @author Brett Wooldridge
 *
 * @param <T> the templated type to store in the bag
 */
public class ConcurrentBag<T extends IConcurrentBagEntry> implements AutoCloseable
{
   private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentBag.class);

   /**共享元素的集合,就是连接的集合**/
   private final CopyOnWriteArrayList<T> sharedList;
   /**是否是弱引用ThreadLocal**/
   private final boolean weakThreadLocals;

   /**线程持有的成员变量是一个List的借用元素集合**/
   private final ThreadLocal<List<Object>> threadList;
   /**Bag状态监听器接口**/
   private final IBagStateListener listener;
   /**原子类，等待者数量**/
   private final AtomicInteger waiters;
   /**volatile类型，并发包是否关闭**/
   private volatile boolean closed;

   private final SynchronousQueue<T> handoffQueue;

   public interface IConcurrentBagEntry
   {
      int STATE_NOT_IN_USE = 0;
      int STATE_IN_USE = 1;
      int STATE_REMOVED = -1;
      int STATE_RESERVED = -2;

      boolean compareAndSet(int expectState, int newState);
      void setState(int newState);
      int getState();
   }

   public interface IBagStateListener
   {
      void addBagItem(int waiting);
   }

   /**
    * Construct a ConcurrentBag with the specified listener.
    *
    * @param listener the IBagStateListener to attach to this bag
    */
   public ConcurrentBag(final IBagStateListener listener)
   {
      this.listener = listener;
      this.weakThreadLocals = useWeakThreadLocals();

      this.handoffQueue = new SynchronousQueue<>(true);
      this.waiters = new AtomicInteger();
      this.sharedList = new CopyOnWriteArrayList<>();
      if (weakThreadLocals) {
         this.threadList = ThreadLocal.withInitial(() -> new ArrayList<>(16));
      }
      else {
         this.threadList = ThreadLocal.withInitial(() -> new FastList<>(IConcurrentBagEntry.class, 16));
      }
   }

   /**
    * 方法调用传入的两个参数，一个是等待时间的长度，一个是等待时间的单位，如果想要“借”的时间超时，就直接返回null。
    * 首先在threadlocal的list从后往前遍历，通过list.remove()方法取出连接后，检查是不是弱引用，如果是弱引用就强转成弱引用。
    * 然后检查这个连接是不是空，或者状态是不是未使用状态，如果是的，将该连接的状态改成正在使用，并且返回连接。
    *
    * 如果在threadlocal的list中没有找到，那么先把等待者的计数器+1，然后sharedList进行遍历查找连接，
    * 如果里面有未使用的连接，那么将这个连接状态改成正在使用，然后将等待者的计数器-1。
    *
    * 如果还是得不到连接，那么开始循环，直到时间小于10秒钟。循环中一直从handoffQueue中获取连接。
    *
    * The method will borrow a BagEntry from the bag, blocking for the
    * specified timeout if none are available.
    *
    * @param timeout how long to wait before giving up, in units of unit
    * @param timeUnit a <code>TimeUnit</code> determining how to interpret the timeout parameter
    * @return a borrowed instance from the bag or null if a timeout occurs
    * @throws InterruptedException if interrupted while waiting
    */
   public T borrow(long timeout, final TimeUnit timeUnit) throws InterruptedException
   {
      // Try the thread-local list first
      final List<Object> list = threadList.get();
      for (int i = list.size() - 1; i >= 0; i--) {
         final Object entry = list.remove(i);
         @SuppressWarnings("unchecked")
         final T bagEntry = weakThreadLocals ? ((WeakReference<T>) entry).get() : (T) entry;
         if (bagEntry != null && bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
            return bagEntry;
         }
      }

      // Otherwise, scan the shared list ... then poll the handoff queue
      final int waiting = waiters.incrementAndGet();
      try {
         for (T bagEntry : sharedList) {
            if (bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
               // If we may have stolen another waiter's connection, request another bag add.
               if (waiting > 1) {
                  listener.addBagItem(waiting - 1);
               }
               return bagEntry;
            }
         }

         listener.addBagItem(waiting);

         timeout = timeUnit.toNanos(timeout);
         do {
            final long start = currentTime();
            final T bagEntry = handoffQueue.poll(timeout, NANOSECONDS);
            if (bagEntry == null || bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
               return bagEntry;
            }

            timeout -= elapsedNanos(start);
         } while (timeout > 10_000);

         return null;
      }
      finally {
         waiters.decrementAndGet();
      }
   }

   /**
    * 回收（归还）连接
    * 首先将连接的状态改成未使用，然后遍历等待者，如果存在需要使用连接的等待者，那么直接放到handoffQueue中给借用者使用。
    * 如果没有，将该线程阻塞10ms。最后是如果当前线程的连接数少于50条，那么将这个线程加入到该线程的threadlocal中
    *
    * This method will return a borrowed object to the bag.  Objects
    * that are borrowed from the bag but never "requited" will result
    * in a memory leak.
    *
    * @param bagEntry the value to return to the bag
    * @throws NullPointerException if value is null
    * @throws IllegalStateException if the bagEntry was not borrowed from the bag
    */
   public void requite(final T bagEntry)
   {
      bagEntry.setState(STATE_NOT_IN_USE);

      for (int i = 0; waiters.get() > 0; i++) {
         if (bagEntry.getState() != STATE_NOT_IN_USE || handoffQueue.offer(bagEntry)) {
            return;
         }
         else if ((i & 0xff) == 0xff) {
            parkNanos(MICROSECONDS.toNanos(10));
         }
         else {
            yield();
         }
      }

      final List<Object> threadLocalList = threadList.get();
      if (threadLocalList.size() < 50) {
         threadLocalList.add(weakThreadLocals ? new WeakReference<>(bagEntry) : bagEntry);
      }
   }

   /**
    * 添加连接方法
    * 首先要检查是不是已经关闭了，如果被关闭了就不能添加。如果没有关闭，可以在sharedList中增加此连接。
    * 如果有消费者在等待并且该连接的状态是未使用并且handoffQueue中不能添加这个连接，那么这个添加线程先休息
    *
    * Add a new object to the bag for others to borrow.
    *
    * @param bagEntry an object to add to the bag
    */
   public void add(final T bagEntry)
   {
      if (closed) {
         LOGGER.info("ConcurrentBag has been closed, ignoring add()");
         throw new IllegalStateException("ConcurrentBag has been closed, ignoring add()");
      }

      sharedList.add(bagEntry);

      // spin until a thread takes it or none are waiting
      while (waiters.get() > 0 && bagEntry.getState() == STATE_NOT_IN_USE && !handoffQueue.offer(bagEntry)) {
         yield();
      }
   }

   /**
    * 移除连接方法
    * 如果这个连接的状态不能从正在使用状态改成移除状态，并且不能从保留状态改成移除状态，并且没有关闭，那么移除失败。
    * 然后尝试从sharedList移除这个连接，最后返回list.remove()方法的结果
    *
    * Remove a value from the bag.  This method should only be called
    * with objects obtained by <code>borrow(long, TimeUnit)</code> or <code>reserve(T)</code>
    *
    * @param bagEntry the value to remove
    * @return true if the entry was removed, false otherwise
    * @throws IllegalStateException if an attempt is made to remove an object
    *         from the bag that was not borrowed or reserved first
    */
   public boolean remove(final T bagEntry)
   {
      if (!bagEntry.compareAndSet(STATE_IN_USE, STATE_REMOVED) && !bagEntry.compareAndSet(STATE_RESERVED, STATE_REMOVED) && !closed) {
         LOGGER.warn("Attempt to remove an object from the bag that was not borrowed or reserved: {}", bagEntry);
         return false;
      }

      final boolean removed = sharedList.remove(bagEntry);
      if (!removed && !closed) {
         LOGGER.warn("Attempt to remove an object from the bag that does not exist: {}", bagEntry);
      }

      return removed;
   }

   /**
    * 关闭方法就是将closed的值改成true
    *
    * Close the bag to further adds.
    */
   @Override
   public void close()
   {
      closed = true;
   }

   /**
    * 找出sharedList中指定状态的所有连接
    *
    * This method provides a "snapshot" in time of the BagEntry
    * items in the bag in the specified state.  It does not "lock"
    * or reserve items in any way.  Call <code>reserve(T)</code>
    * on items in list before performing any action on them.
    *
    * @param state one of the {@link IConcurrentBagEntry} states
    * @return a possibly empty list of objects having the state specified
    */
   public List<T> values(final int state)
   {
      final List<T> list = sharedList.stream().filter(e -> e.getState() == state).collect(Collectors.toList());
      Collections.reverse(list);
      return list;
   }

   /**
    * 没有指定状态，那么将sharedList的复制返回
    *
    * This method provides a "snapshot" in time of the bag items.  It
    * does not "lock" or reserve items in any way.  Call <code>reserve(T)</code>
    * on items in the list, or understand the concurrency implications of
    * modifying items, before performing any action on them.
    *
    * @return a possibly empty list of (all) bag items
    */
   @SuppressWarnings("unchecked")
   public List<T> values()
   {
      return (List<T>) sharedList.clone();
   }

   /**
    * 保留
    * 这个方法是将连接的状态从未使用改成保留，这样就不会被借出去。这个方法主要用于使用values(int)方法，保留的连接可以被删除
    *
    * The method is used to make an item in the bag "unavailable" for
    * borrowing.  It is primarily used when wanting to operate on items
    * returned by the <code>values(int)</code> method.  Items that are
    * reserved can be removed from the bag via <code>remove(T)</code>
    * without the need to unreserve them.  Items that are not removed
    * from the bag can be make available for borrowing again by calling
    * the <code>unreserve(T)</code> method.
    *
    * @param bagEntry the item to reserve
    * @return true if the item was able to be reserved, false otherwise
    */
   public boolean reserve(final T bagEntry)
   {
      return bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_RESERVED);
   }

   /**
    * 将连接的状态从保留改成未使用，这样就变得可用，可以让别的线程“借用”
    *
    * This method is used to make an item reserved via <code>reserve(T)</code>
    * available again for borrowing.
    *
    * @param bagEntry the item to unreserve
    */
   public void unreserve(final T bagEntry)
   {
      if (bagEntry.compareAndSet(STATE_RESERVED, STATE_NOT_IN_USE)) {
         // spin until a thread takes it or none are waiting
         while (waiters.get() > 0 && !handoffQueue.offer(bagEntry)) {
            yield();
         }
      }
      else {
         LOGGER.warn("Attempt to relinquish an object to the bag that was not reserved: {}", bagEntry);
      }
   }

   /**
    * 获取从等待到借用到连接的花费时间
    *
    * Get the number of threads pending (waiting) for an item from the
    * bag to become available.
    *
    * @return the number of threads waiting for items from the bag
    */
   public int getWaitingThreadCount()
   {
      return waiters.get();
   }

   /**
    * 统计这次调用时指定状态的连接数
    *
    * Get a count of the number of items in the specified state at the time of this call.
    *
    * @param state the state of the items to count
    * @return a count of how many items in the bag are in the specified state
    */
   public int getCount(final int state)
   {
      int count = 0;
      for (IConcurrentBagEntry e : sharedList) {
         if (e.getState() == state) {
            count++;
         }
      }
      return count;
   }

   public int[] getStateCounts()
   {
      final int[] states = new int[6];
      for (IConcurrentBagEntry e : sharedList) {
         ++states[e.getState()];
      }
      states[4] = sharedList.size();
      states[5] = waiters.get();

      return states;
   }

   /**
    * 获取共享连接数
    *
    * Get the total number of items in the bag.
    *
    * @return the number of items in the bag
    */
   public int size()
   {
      return sharedList.size();
   }

   /**输出共享连接集合中的所有连接到日志**/
   public void dumpState()
   {
      sharedList.forEach(entry -> LOGGER.info(entry.toString()));
   }

   /**
    * Determine whether to use WeakReferences based on whether there is a
    * custom ClassLoader implementation sitting between this class and the
    * System ClassLoader.
    *
    * @return true if we should use WeakReferences in our ThreadLocals, false otherwise
    */
   private boolean useWeakThreadLocals()
   {
      try {
         if (System.getProperty("com.zaxxer.hikari.useWeakReferences") != null) {   // undocumented manual override of WeakReference behavior
            return Boolean.getBoolean("com.zaxxer.hikari.useWeakReferences");
         }

         return getClass().getClassLoader() != ClassLoader.getSystemClassLoader();
      }
      catch (SecurityException se) {
         return true;
      }
   }
}
