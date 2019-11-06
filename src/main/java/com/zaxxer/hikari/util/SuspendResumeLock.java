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

import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.concurrent.Semaphore;

/**
 * 数据库连接池暂停恢复锁
 * This class implements a lock that can be used to suspend and resume the pool.  It
 * also provides a faux implementation that is used when the feature is disabled that
 * hopefully gets fully "optimized away" by the JIT.
 *
 * @author Brett Wooldridge
 */
public class SuspendResumeLock
{
   public static final SuspendResumeLock FAUX_LOCK = new SuspendResumeLock(false) {
      @Override
      public void acquire() {}

      @Override
      public void release() {}

      @Override
      public void suspend() {}

      @Override
      public void resume() {}
   };

   private static final int MAX_PERMITS = 10000;
   /**限制连接的数量，如果连接超过10K，那么就会阻塞直到有别的连接断开后释放了信号量**/
   private final Semaphore acquisitionSemaphore;

   /**
    * Default constructor
    */
   public SuspendResumeLock()
   {
      this(true);
   }

   /**默认构造器是带锁的，信号量的值为10K**/
   private SuspendResumeLock(final boolean createSemaphore)
   {
      acquisitionSemaphore = (createSemaphore ? new Semaphore(MAX_PERMITS, true) : null);
   }

   /**获取信号量方法，首先会尝试获取信号量，如果没有获取到并且配置文件配置了获取不到抛出异常，那么就会抛出sql异常。
    * 如果没有配置，那么就调用acquireUninterruptibly()方法继续等待获取信号量**/
   public void acquire() throws SQLException
   {
      if (acquisitionSemaphore.tryAcquire()) {
         return;
      }
      else if (Boolean.getBoolean("com.zaxxer.hikari.throwIfSuspended")) {
         throw new SQLTransientException("The pool is currently suspended and configured to throw exceptions upon acquisition");
      }

      acquisitionSemaphore.acquireUninterruptibly();
   }

   /**释放线程持有的信号量**/
   public void release()
   {
      acquisitionSemaphore.release();
   }

   /**获取所有信号量导致暂停**/
   public void suspend()
   {
      acquisitionSemaphore.acquireUninterruptibly(MAX_PERMITS);
   }

   /**释放所有信号量恢复连接池**/
   public void resume()
   {
      acquisitionSemaphore.release(MAX_PERMITS);
   }
}
