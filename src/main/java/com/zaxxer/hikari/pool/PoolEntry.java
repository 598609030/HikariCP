/*
 * Copyright (C) 2014 Brett Wooldridge
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
package com.zaxxer.hikari.pool;

import com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry;
import com.zaxxer.hikari.util.FastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.zaxxer.hikari.util.ClockSource.*;

/**
 * Entry used in the ConcurrentBag to track Connection instances.
 *
 * @author Brett Wooldridge
 */
final class PoolEntry implements IConcurrentBagEntry
{
   private static final Logger LOGGER = LoggerFactory.getLogger(PoolEntry.class);
   private static final AtomicIntegerFieldUpdater<PoolEntry> stateUpdater;

   Connection connection;
   /**上次访问毫秒数**/
   long lastAccessed;
   /**上次借用毫秒数**/
   long lastBorrowed;

   @SuppressWarnings("FieldCanBeLocal")
   private volatile int state = 0;
   /**是否已经移除出连接池**/
   private volatile boolean evict;

   /**连接结束的最后回调**/
   private volatile ScheduledFuture<?> endOfLife;

   private final FastList<Statement> openStatements;
   private final HikariPool hikariPool;

   private final boolean isReadOnly;
   private final boolean isAutoCommit;

   /**静态块中就是对stateUpdater进行实例化，对state字段进行顺序更新**/
   static
   {
      stateUpdater = AtomicIntegerFieldUpdater.newUpdater(PoolEntry.class, "state");
   }

   PoolEntry(final Connection connection, final PoolBase pool, final boolean isReadOnly, final boolean isAutoCommit)
   {
      this.connection = connection;
      this.hikariPool = (HikariPool) pool;
      this.isReadOnly = isReadOnly;
      this.isAutoCommit = isAutoCommit;
      this.lastAccessed = currentTime();
      this.openStatements = new FastList<>(Statement.class, 16);
   }

   /**
    * 回收连接：如果连接存在，记录上次方法的时间戳，然后调用连接池的方法回收连接
    * Release this entry back to the pool.
    *
    * @param lastAccessed last access time-stamp
    */
   void recycle(final long lastAccessed)
   {
      if (connection != null) {
         this.lastAccessed = lastAccessed;
         hikariPool.recycle(this);
      }
   }

   /**
    * 设置调度任务的回调
    * Set the end of life {@link ScheduledFuture}.
    *
    * @param endOfLife this PoolEntry/Connection's end of life {@link ScheduledFuture}
    */
   void setFutureEol(final ScheduledFuture<?> endOfLife)
   {
      this.endOfLife = endOfLife;
   }

   /**传入普通的连接创建一个代理连接**/
   Connection createProxyConnection(final ProxyLeakTask leakTask, final long now)
   {
      return ProxyFactory.getProxyConnection(this, connection, openStatements, leakTask, now, isReadOnly, isAutoCommit);
   }

   /**重置连接的状态，里面的方法涉及到位运算，为了提高运算效率**/
   void resetConnectionState(final ProxyConnection proxyConnection, final int dirtyBits) throws SQLException
   {
      hikariPool.resetConnectionState(connection, proxyConnection, dirtyBits);
   }

   String getPoolName()
   {
      return hikariPool.toString();
   }

   /**判断该连接是否已经被连接池移出**/
   boolean isMarkedEvicted()
   {
      return evict;
   }

   /**标记成已移除，将evict标成true**/
   void markEvicted()
   {
      this.evict = true;
   }

   /**移除连接，调用的是连接池的closeConnection()方法，同时记录了关闭原因**/
   void evict(final String closureReason)
   {
      hikariPool.closeConnection(this, closureReason);
   }

   /**计算上次使用该连接到现在共花费了多少毫秒**/
   /** Returns millis since lastBorrowed */
   long getMillisSinceBorrowed()
   {
      return elapsedMillis(lastBorrowed);
   }

   /** {@inheritDoc} */
   @Override
   public String toString()
   {
      final long now = currentTime();
      return connection
         + ", accessed " + elapsedDisplayString(lastAccessed, now) + " ago, "
         + stateToString();
   }

   // ***********************************************************************
   //                      IConcurrentBagEntry methods
   // ***********************************************************************

   /** {@inheritDoc} */
   @Override
   public int getState()
   {
      return stateUpdater.get(this);
   }

   /** {@inheritDoc} */
   @Override
   public boolean compareAndSet(int expect, int update)
   {
      return stateUpdater.compareAndSet(this, expect, update);
   }

   /** {@inheritDoc} */
   @Override
   public void setState(int update)
   {
      stateUpdater.set(this, update);
   }

   /**关闭连接，检查连接的最后回调是否完成，如果没有完成那么输出日志，然后将连接和回调置空后返回连接的引用**/
   Connection close()
   {
      ScheduledFuture<?> eol = endOfLife;
      if (eol != null && !eol.isDone() && !eol.cancel(false)) {
         LOGGER.warn("{} - maxLifeTime expiration task cancellation unexpectedly returned false for connection {}", getPoolName(), connection);
      }

      Connection con = connection;
      connection = null;
      endOfLife = null;
      return con;
   }

   private String stateToString()
   {
      switch (state) {
      case STATE_IN_USE:
         return "IN_USE";
      case STATE_NOT_IN_USE:
         return "NOT_IN_USE";
      case STATE_REMOVED:
         return "REMOVED";
      case STATE_RESERVED:
         return "RESERVED";
      default:
         return "Invalid";
      }
   }
}
