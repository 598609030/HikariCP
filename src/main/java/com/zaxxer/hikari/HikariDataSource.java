/*
 * Copyright (C) 2013 Brett Wooldridge
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

package com.zaxxer.hikari;

import com.zaxxer.hikari.metrics.MetricsTrackerFactory;
import com.zaxxer.hikari.pool.HikariPool;
import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.zaxxer.hikari.pool.HikariPool.POOL_NORMAL;

/**
 * The HikariCP pooled DataSource.
 *
 * @author Brett Wooldridge
 */
public class HikariDataSource extends HikariConfig implements DataSource, Closeable
{
   private static final Logger LOGGER = LoggerFactory.getLogger(HikariDataSource.class);

   /**
    * 线程安全的连接池是否关闭的标识符
    */
   private final AtomicBoolean isShutdown = new AtomicBoolean();

   /**
    * 通过传入的配置创建的实际连接池的对象引用
    */
   private final HikariPool fastPathPool;
   /**
    * 通过自身的配置创建的实际连接池的对象引用，注意这里的volatile，这个是为双重检查锁定模式使用的
    */
   private volatile HikariPool pool;

   /**
    * Default constructor.  Setters are used to configure the pool.  Using
    * this constructor vs. {@link #HikariDataSource(HikariConfig)} will
    * result in {@link #getConnection()} performance that is slightly lower
    * due to lazy initialization checks.
    *
    * The first call to {@link #getConnection()} starts the pool.  Once the pool
    * is started, the configuration is "sealed" and no further configuration
    * changes are possible -- except via {@link HikariConfigMXBean} methods.
    */
   public HikariDataSource()
   {
      super();
      fastPathPool = null;
   }

   /**
    * 首先是对传入的配置参数进行检查是否符合要求，如果符合要求将参数复制到本对象中，
    * 然后开始创建线程池，将创建的线程池的引用赋给fastPathPool，
    * 然后将是否封闭的值改成true防止被恶意修改
    *
    * Construct a HikariDataSource with the specified configuration.  The
    * {@link HikariConfig} is copied and the pool is started by invoking this
    * constructor.
    *
    * The {@link HikariConfig} can be modified without affecting the HikariDataSource
    * and used to initialize another HikariDataSource instance.
    *
    * @param configuration a HikariConfig instance
    */
   public HikariDataSource(HikariConfig configuration)
   {
      configuration.validate();
      configuration.copyStateTo(this);

      LOGGER.info("{} - Starting...", configuration.getPoolName());
      pool = fastPathPool = new HikariPool(this);
      LOGGER.info("{} - Start completed.", configuration.getPoolName());

      this.seal();
   }

   // ***********************************************************************
   //                          DataSource methods
   // ***********************************************************************

   /**
    * 首先检查该连接池是否已经关闭，就是成员变量的isShutdown的值。如果已经关闭，抛出已经关闭的异常。
    * 如果连接池初始化成功的话，直接调用连接池的getConnection()方法返回。如果没有初始化，
    * 那么通过自身的配置信息初始化一个数据库连接池赋给pool字段。然后用这个新创建的连接池返回连接。
    * 这里代码那么长主要是因为用了双重检查锁定模式，防止在多线程的情况中初始化一个不完整的数据库连接池
    */
   /** {@inheritDoc} */
   @Override
   public Connection getConnection() throws SQLException
   {
      if (isClosed()) {
         throw new SQLException("HikariDataSource " + this + " has been closed.");
      }

      if (fastPathPool != null) {
         return fastPathPool.getConnection();
      }

      /** 双重检查锁 **/
      // See http://en.wikipedia.org/wiki/Double-checked_locking#Usage_in_Java
      HikariPool result = pool;
      if (result == null) {
         synchronized (this) {
            result = pool;
            if (result == null) {
               validate();
               LOGGER.info("{} - Starting...", getPoolName());
               try {
                  pool = result = new HikariPool(this);
                  this.seal();
               }
               catch (PoolInitializationException pie) {
                  if (pie.getCause() instanceof SQLException) {
                     throw (SQLException) pie.getCause();
                  }
                  else {
                     throw pie;
                  }
               }
               LOGGER.info("{} - Start completed.", getPoolName());
            }
         }
      }

      return result.getConnection();
   }

   /** {@inheritDoc} */
   @Override
   public Connection getConnection(String username, String password) throws SQLException
   {
      throw new SQLFeatureNotSupportedException();
   }

   /** {@inheritDoc} */
   @Override
   public PrintWriter getLogWriter() throws SQLException
   {
      HikariPool p = pool;
      return (p != null ? p.getUnwrappedDataSource().getLogWriter() : null);
   }

   /** {@inheritDoc} */
   @Override
   public void setLogWriter(PrintWriter out) throws SQLException
   {
      HikariPool p = pool;
      if (p != null) {
         p.getUnwrappedDataSource().setLogWriter(out);
      }
   }

   /** {@inheritDoc} */
   @Override
   public void setLoginTimeout(int seconds) throws SQLException
   {
      HikariPool p = pool;
      if (p != null) {
         p.getUnwrappedDataSource().setLoginTimeout(seconds);
      }
   }

   /** {@inheritDoc} */
   @Override
   public int getLoginTimeout() throws SQLException
   {
      HikariPool p = pool;
      return (p != null ? p.getUnwrappedDataSource().getLoginTimeout() : 0);
   }

   /** {@inheritDoc} */
   @Override
   public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException
   {
      throw new SQLFeatureNotSupportedException();
   }

   /**
    * 这个方法是为了将连接池转换成指定的数据库连接池类型，如果是的，进行强制转换后返回
    */
   /** {@inheritDoc} */
   @Override
   @SuppressWarnings("unchecked")
   public <T> T unwrap(Class<T> iface) throws SQLException
   {
      if (iface.isInstance(this)) {
         return (T) this;
      }

      HikariPool p = pool;
      if (p != null) {
         final DataSource unwrappedDataSource = p.getUnwrappedDataSource();
         if (iface.isInstance(unwrappedDataSource)) {
            return (T) unwrappedDataSource;
         }

         if (unwrappedDataSource != null) {
            return unwrappedDataSource.unwrap(iface);
         }
      }

      throw new SQLException("Wrapped DataSource is not an instance of " + iface);
   }

   /**
    * 这个方法是来判断当前的对象是不是指定类的实例
    */
   /** {@inheritDoc} */
   @Override
   public boolean isWrapperFor(Class<?> iface) throws SQLException
   {
      if (iface.isInstance(this)) {
         return true;
      }

      HikariPool p = pool;
      if (p != null) {
         final DataSource unwrappedDataSource = p.getUnwrappedDataSource();
         if (iface.isInstance(unwrappedDataSource)) {
            return true;
         }

         if (unwrappedDataSource != null) {
            return unwrappedDataSource.isWrapperFor(iface);
         }
      }

      return false;
   }

   // ***********************************************************************
   //                        HikariConfigMXBean methods
   // ***********************************************************************

   /**
    * 设置度量标准注册表，先调用父类HikariConfig的set方法来检查和设置，
    * 如果没有问题、连接池也初始化完成并且没有设置过，就可以进行参数的设置
    */
   /** {@inheritDoc} */
   @Override
   public void setMetricRegistry(Object metricRegistry)
   {
      boolean isAlreadySet = getMetricRegistry() != null;
      super.setMetricRegistry(metricRegistry);

      HikariPool p = pool;
      if (p != null) {
         if (isAlreadySet) {
            throw new IllegalStateException("MetricRegistry can only be set one time");
         }
         else {
            p.setMetricRegistry(super.getMetricRegistry());
         }
      }
   }

   /** {@inheritDoc} */
   @Override
   public void setMetricsTrackerFactory(MetricsTrackerFactory metricsTrackerFactory)
   {
      boolean isAlreadySet = getMetricsTrackerFactory() != null;
      super.setMetricsTrackerFactory(metricsTrackerFactory);

      HikariPool p = pool;
      if (p != null) {
         if (isAlreadySet) {
            throw new IllegalStateException("MetricsTrackerFactory can only be set one time");
         }
         else {
            p.setMetricsTrackerFactory(super.getMetricsTrackerFactory());
         }
      }
   }

   /** {@inheritDoc} */
   @Override
   public void setHealthCheckRegistry(Object healthCheckRegistry)
   {
      boolean isAlreadySet = getHealthCheckRegistry() != null;
      super.setHealthCheckRegistry(healthCheckRegistry);

      HikariPool p = pool;
      if (p != null) {
         if (isAlreadySet) {
            throw new IllegalStateException("HealthCheckRegistry can only be set one time");
         }
         else {
            p.setHealthCheckRegistry(super.getHealthCheckRegistry());
         }
      }
   }

   // ***********************************************************************
   //                        HikariCP-specific methods
   // ***********************************************************************

   /**
    * 检查连接池是否正在运行，判断条件是连接池存在并且状态是正常状态
    *
    * Returns {@code true} if the pool as been started and is not suspended or shutdown.
    *
    * @return {@code true} if the pool as been started and is not suspended or shutdown.
    */
   public boolean isRunning()
   {
      return pool != null && pool.poolState == POOL_NORMAL;
   }

   /**
    * Get the {@code HikariPoolMXBean} for this HikariDataSource instance.  If this method is called on
    * a {@code HikariDataSource} that has been constructed without a {@code HikariConfig} instance,
    * and before an initial call to {@code #getConnection()}, the return value will be {@code null}.
    *
    * @return the {@code HikariPoolMXBean} instance, or {@code null}.
    */
   public HikariPoolMXBean getHikariPoolMXBean()
   {
      return pool;
   }

   /**
    * Get the {@code HikariConfigMXBean} for this HikariDataSource instance.
    *
    * @return the {@code HikariConfigMXBean} instance.
    */
   public HikariConfigMXBean getHikariConfigMXBean()
   {
      return this;
   }

   /**
    * 将指定的连接从连接池中去除。判断条件是连接池没有关闭并且连接池存在并且这个连接是hikari数据库连接池的连接，
    * 以上为真的情况下，调用HikariPool的方法去除该连接
    *
    * Evict a connection from the pool.  If the connection has already been closed (returned to the pool)
    * this may result in a "soft" eviction; the connection will be evicted sometime in the future if it is
    * currently in use.  If the connection has not been closed, the eviction is immediate.
    *
    * @param connection the connection to evict from the pool
    */
   public void evictConnection(Connection connection)
   {
      HikariPool p;
      if (!isClosed() && (p = pool) != null && connection.getClass().getName().startsWith("com.zaxxer.hikari")) {
         p.evictConnection(connection);
      }
   }

   /**
    * 关闭连接池，先判断是否已经关闭，如果是的就结束，如果不是，调用HikariPool的shutdown()方法进行关闭
    *
    * Shutdown the DataSource and its associated pool.
    */
   @Override
   public void close()
   {
      if (isShutdown.getAndSet(true)) {
         return;
      }

      HikariPool p = pool;
      if (p != null) {
         try {
            LOGGER.info("{} - Shutdown initiated...", getPoolName());
            p.shutdown();
            LOGGER.info("{} - Shutdown completed.", getPoolName());
         }
         catch (InterruptedException e) {
            LOGGER.warn("{} - Interrupted during closing", getPoolName(), e);
            Thread.currentThread().interrupt();
         }
      }
   }

   /**
    * Determine whether the HikariDataSource has been closed.
    *
    * @return true if the HikariDataSource has been closed, false otherwise
    */
   public boolean isClosed()
   {
      return isShutdown.get();
   }

   /** {@inheritDoc} */
   @Override
   public String toString()
   {
      return "HikariDataSource (" + pool + ")";
   }
}
