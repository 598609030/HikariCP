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

package com.zaxxer.hikari;

import com.codahale.metrics.health.HealthCheckRegistry;
import com.zaxxer.hikari.metrics.MetricsTrackerFactory;
import com.zaxxer.hikari.util.PropertyElf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.security.AccessControlException;
import java.sql.Connection;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;

import static com.zaxxer.hikari.util.UtilityElf.getNullIfEmpty;
import static com.zaxxer.hikari.util.UtilityElf.safeIsAssignableFrom;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

@SuppressWarnings({"SameParameterValue", "unused"})
public class HikariConfig implements HikariConfigMXBean {
   private static final Logger LOGGER = LoggerFactory.getLogger(HikariConfig.class);
   /**
    * 随机生成线程池名称时使用
    * @see this#generatePoolName
    */
   private static final char[] ID_CHARACTERS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
   /**连接超时，默认30秒**/
   private static final long CONNECTION_TIMEOUT = SECONDS.toMillis(30);
   /**验证超时，默认5秒**/
   private static final long VALIDATION_TIMEOUT = SECONDS.toMillis(5);
   /**空闲超时，默认10分钟**/
   private static final long IDLE_TIMEOUT = MINUTES.toMillis(10);
   /**最大生命周期，默认30分钟**/
   private static final long MAX_LIFETIME = MINUTES.toMillis(30);
   /**默认连接池大小10**/
   private static final int DEFAULT_POOL_SIZE = 10;

   /**是否是单元测试，默认false**/
   private static boolean unitTest = false;

   // Properties changeable at runtime through the HikariConfigMXBean
   //
   /**
    * 运行过程中可以通过HikariConfigMXBean进行修改，
    * HikariConfigMXBean是接口，所以应该是指用它的实现类来进行值的修改。
    * 它的修饰符都是volatile，表示对所有线程可见
    */
   private volatile String catalog;
   private volatile long connectionTimeout;
   private volatile long validationTimeout;
   private volatile long idleTimeout;
   private volatile long leakDetectionThreshold;
   private volatile long maxLifetime;
   private volatile int maxPoolSize;
   private volatile int minIdle;
   private volatile String username;
   private volatile String password;

   // Properties NOT changeable at runtime
   //
   /**运行前已经设置好，运行以后不能修改**/
   private long initializationFailTimeout;
   private String connectionInitSql;
   private String connectionTestQuery;
   private String dataSourceClassName;
   private String dataSourceJndiName;
   private String driverClassName;
   private String jdbcUrl;
   private String poolName;
   private String schema;
   private String transactionIsolationName;
   private boolean isAutoCommit;
   private boolean isReadOnly;
   private boolean isIsolateInternalQueries;
   private boolean isRegisterMbeans;
   private boolean isAllowPoolSuspension;
   private DataSource dataSource;
   private Properties dataSourceProperties;
   private ThreadFactory threadFactory;
   private ScheduledExecutorService scheduledExecutor;
   private MetricsTrackerFactory metricsTrackerFactory;
   private Object metricRegistry;
   private Object healthCheckRegistry;
   private Properties healthCheckProperties;

   /**
    * 是否可以在运行时不通过HikariConfigMXBean进行参数的修改，
    * 如果是true，那么一定要通过HikariConfigMXBean进行参数修改
    */
   private volatile boolean sealed;

   /**
    * Default constructor
    */
   public HikariConfig() {
      dataSourceProperties = new Properties();
      healthCheckProperties = new Properties();

      minIdle = -1;
      maxPoolSize = -1;
      maxLifetime = MAX_LIFETIME;
      connectionTimeout = CONNECTION_TIMEOUT;
      validationTimeout = VALIDATION_TIMEOUT;
      idleTimeout = IDLE_TIMEOUT;
      initializationFailTimeout = 1;
      isAutoCommit = true;

      String systemProp = System.getProperty("hikaricp.configurationFile");
      if (systemProp != null) {
         loadProperties(systemProp);
      }
   }

   /**
    * Construct a HikariConfig from the specified properties object.
    *
    * @param properties the name of the property file
    */
   public HikariConfig(Properties properties) {
      this();
      PropertyElf.setTargetFromProperties(this, properties);
   }

   /**
    * Construct a HikariConfig from the specified property file name.  <code>propertyFileName</code>
    * will first be treated as a path in the file-system, and if that fails the
    * Class.getResourceAsStream(propertyFileName) will be tried.
    *
    * @param propertyFileName the name of the property file
    */
   public HikariConfig(String propertyFileName) {
      this();

      loadProperties(propertyFileName);
   }

   // ***********************************************************************
   //                       HikariConfigMXBean methods
   // ***********************************************************************

   /**
    * {@inheritDoc}
    */
   @Override
   public String getCatalog() {
      return catalog;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void setCatalog(String catalog) {
      this.catalog = catalog;
   }


   /**
    * {@inheritDoc}
    */
   @Override
   public long getConnectionTimeout() {
      return connectionTimeout;
   }

   /**
    * 连接超时的时间时不能设置小于250ms，如果设置成0，
    * 那么实际的时间将被设置成Integer.MAX_VALUE，相当于不会超时
    * {@inheritDoc}
    */
   @Override
   public void setConnectionTimeout(long connectionTimeoutMs) {
      if (connectionTimeoutMs == 0) {
         this.connectionTimeout = Integer.MAX_VALUE;
      } else if (connectionTimeoutMs < 250) {
         throw new IllegalArgumentException("connectionTimeout cannot be less than 250ms");
      } else {
         this.connectionTimeout = connectionTimeoutMs;
      }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public long getIdleTimeout() {
      return idleTimeout;
   }

   /**
    * 空闲超时的配置只检查一个，不能是负数
    * {@inheritDoc}
    */
   @Override
   public void setIdleTimeout(long idleTimeoutMs) {
      if (idleTimeoutMs < 0) {
         throw new IllegalArgumentException("idleTimeout cannot be negative");
      }
      this.idleTimeout = idleTimeoutMs;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public long getLeakDetectionThreshold() {
      return leakDetectionThreshold;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void setLeakDetectionThreshold(long leakDetectionThresholdMs) {
      this.leakDetectionThreshold = leakDetectionThresholdMs;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public long getMaxLifetime() {
      return maxLifetime;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void setMaxLifetime(long maxLifetimeMs) {
      this.maxLifetime = maxLifetimeMs;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public int getMaximumPoolSize() {
      return maxPoolSize;
   }

   /**
    * 数据库连接池最大连接数，只检查不能小于1
    * {@inheritDoc}
    */
   @Override
   public void setMaximumPoolSize(int maxPoolSize) {
      if (maxPoolSize < 1) {
         throw new IllegalArgumentException("maxPoolSize cannot be less than 1");
      }
      this.maxPoolSize = maxPoolSize;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public int getMinimumIdle() {
      return minIdle;
   }

   /**
    * 数据库连接池最小连接数，只检查不能小于0
    * {@inheritDoc}
    */
   @Override
   public void setMinimumIdle(int minIdle) {
      if (minIdle < 0) {
         throw new IllegalArgumentException("minimumIdle cannot be negative");
      }
      this.minIdle = minIdle;
   }

   /**
    * Get the default password to use for DataSource.getConnection(username, password) calls.
    *
    * @return the password
    */
   public String getPassword() {
      return password;
   }

   /**
    * Set the default password to use for DataSource.getConnection(username, password) calls.
    *
    * @param password the password
    */
   @Override
   public void setPassword(String password) {
      this.password = password;
   }

   /**
    * Get the default username used for DataSource.getConnection(username, password) calls.
    *
    * @return the username
    */
   public String getUsername() {
      return username;
   }

   /**
    * Set the default username used for DataSource.getConnection(username, password) calls.
    *
    * @param username the username
    */
   @Override
   public void setUsername(String username) {
      this.username = username;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public long getValidationTimeout() {
      return validationTimeout;
   }

   /**
    * 验证超时时间的参数不能小于250ms
    * {@inheritDoc}
    */
   @Override
   public void setValidationTimeout(long validationTimeoutMs) {
      if (validationTimeoutMs < 250) {
         throw new IllegalArgumentException("validationTimeout cannot be less than 250ms");
      }

      this.validationTimeout = validationTimeoutMs;
   }

   // ***********************************************************************
   //                     All other configuration methods
   // ***********************************************************************

   /**
    * Get the SQL query to be executed to test the validity of connections.
    *
    * @return the SQL query string, or null
    */
   public String getConnectionTestQuery() {
      return connectionTestQuery;
   }

   /**
    * Set the SQL query to be executed to test the validity of connections. Using
    * the JDBC4 <code>Connection.isValid()</code> method to test connection validity can
    * be more efficient on some databases and is recommended.
    *
    * @param connectionTestQuery a SQL query string
    */
   public void setConnectionTestQuery(String connectionTestQuery) {
      checkIfSealed();
      this.connectionTestQuery = connectionTestQuery;
   }

   /**
    * Get the SQL string that will be executed on all new connections when they are
    * created, before they are added to the pool.
    *
    * @return the SQL to execute on new connections, or null
    */
   public String getConnectionInitSql() {
      return connectionInitSql;
   }

   /**
    * Set the SQL string that will be executed on all new connections when they are
    * created, before they are added to the pool.  If this query fails, it will be
    * treated as a failed connection attempt.
    *
    * @param connectionInitSql the SQL to execute on new connections
    */
   public void setConnectionInitSql(String connectionInitSql) {
      checkIfSealed();
      this.connectionInitSql = connectionInitSql;
   }

   /**
    * Get the {@link DataSource} that has been explicitly specified to be wrapped by the
    * pool.
    *
    * @return the {@link DataSource} instance, or null
    */
   public DataSource getDataSource() {
      return dataSource;
   }

   /**
    * Set a {@link DataSource} for the pool to explicitly wrap.  This setter is not
    * available through property file based initialization.
    *
    * @param dataSource a specific {@link DataSource} to be wrapped by the pool
    */
   public void setDataSource(DataSource dataSource) {
      checkIfSealed();
      this.dataSource = dataSource;
   }

   /**
    * Get the name of the JDBC {@link DataSource} class used to create Connections.
    *
    * @return the fully qualified name of the JDBC {@link DataSource} class
    */
   public String getDataSourceClassName() {
      return dataSourceClassName;
   }

   /**
    * Set the fully qualified class name of the JDBC {@link DataSource} that will be used create Connections.
    *
    * @param className the fully qualified name of the JDBC {@link DataSource} class
    */
   public void setDataSourceClassName(String className) {
      checkIfSealed();
      this.dataSourceClassName = className;
   }

   /**
    * Add a property (name/value pair) that will be used to configure the {@link DataSource}/{@link java.sql.Driver}.
    * <p>
    * In the case of a {@link DataSource}, the property names will be translated to Java setters following the Java Bean
    * naming convention.  For example, the property {@code cachePrepStmts} will translate into {@code setCachePrepStmts()}
    * with the {@code value} passed as a parameter.
    * <p>
    * In the case of a {@link java.sql.Driver}, the property will be added to a {@link Properties} instance that will
    * be passed to the driver during {@link java.sql.Driver#connect(String, Properties)} calls.
    *
    * @param propertyName the name of the property
    * @param value        the value to be used by the DataSource/Driver
    */
   public void addDataSourceProperty(String propertyName, Object value) {
      checkIfSealed();
      dataSourceProperties.put(propertyName, value);
   }

   public String getDataSourceJNDI() {
      return this.dataSourceJndiName;
   }

   public void setDataSourceJNDI(String jndiDataSource) {
      checkIfSealed();
      this.dataSourceJndiName = jndiDataSource;
   }

   public Properties getDataSourceProperties() {
      return dataSourceProperties;
   }

   public void setDataSourceProperties(Properties dsProperties) {
      checkIfSealed();
      dataSourceProperties.putAll(dsProperties);
   }

   public String getDriverClassName() {
      return driverClassName;
   }

   /**
    * 这个方法是设置数据库的驱动类，数据库驱动类需要在启动数据库连接池时加载到内存中。
    * 这边的主要逻辑就是先用Thread.currentThread().getContextClassLoader()的classLoader来加载类，
    * 如果没有加载到，那么用this.getClass().getClassLoader()来加载驱动，
    * 如果都加载失败，那么抛出ClassNotFoundException。然后尝试创建驱动的实例，成功的话就将驱动的名称设置到参数中。
    * 至于中间的两个classLoader的区别是什么，这个是另一个问题了。
    * 简而言之就是通过当前线程去加载比较安全，避免双亲委派加载中可能出现的错误。
    */
   public void setDriverClassName(String driverClassName) {
      checkIfSealed();

      Class<?> driverClass = attemptFromContextLoader(driverClassName);
      try {
         if (driverClass == null) {
            driverClass = this.getClass().getClassLoader().loadClass(driverClassName);
            LOGGER.debug("Driver class {} found in the HikariConfig class classloader {}", driverClassName, this.getClass().getClassLoader());
         }
      } catch (ClassNotFoundException e) {
         LOGGER.error("Failed to load driver class {} from HikariConfig class classloader {}", driverClassName, this.getClass().getClassLoader());
      }

      if (driverClass == null) {
         throw new RuntimeException("Failed to load driver class " + driverClassName + " in either of HikariConfig class loader or Thread context classloader");
      }

      try {
         driverClass.getConstructor().newInstance();
         this.driverClassName = driverClassName;
      } catch (Exception e) {
         throw new RuntimeException("Failed to instantiate class " + driverClassName, e);
      }
   }

   public String getJdbcUrl() {
      return jdbcUrl;
   }

   public void setJdbcUrl(String jdbcUrl) {
      checkIfSealed();
      this.jdbcUrl = jdbcUrl;
   }

   /**
    * Get the default auto-commit behavior of connections in the pool.
    *
    * @return the default auto-commit behavior of connections
    */
   public boolean isAutoCommit() {
      return isAutoCommit;
   }

   /**
    * Set the default auto-commit behavior of connections in the pool.
    *
    * @param isAutoCommit the desired auto-commit default for connections
    */
   public void setAutoCommit(boolean isAutoCommit) {
      checkIfSealed();
      this.isAutoCommit = isAutoCommit;
   }

   /**
    * Get the pool suspension behavior (allowed or disallowed).
    *
    * @return the pool suspension behavior
    */
   public boolean isAllowPoolSuspension() {
      return isAllowPoolSuspension;
   }

   /**
    * Set whether or not pool suspension is allowed.  There is a performance
    * impact when pool suspension is enabled.  Unless you need it (for a
    * redundancy system for example) do not enable it.
    *
    * @param isAllowPoolSuspension the desired pool suspension allowance
    */
   public void setAllowPoolSuspension(boolean isAllowPoolSuspension) {
      checkIfSealed();
      this.isAllowPoolSuspension = isAllowPoolSuspension;
   }

   /**
    * Get the pool initialization failure timeout.  See {@code #setInitializationFailTimeout(long)}
    * for details.
    *
    * @return the number of milliseconds before the pool initialization fails
    * @see HikariConfig#setInitializationFailTimeout(long)
    */
   public long getInitializationFailTimeout() {
      return initializationFailTimeout;
   }

   /**
    * Set the pool initialization failure timeout.  This setting applies to pool
    * initialization when {@link HikariDataSource} is constructed with a {@link HikariConfig},
    * or when {@link HikariDataSource} is constructed using the no-arg constructor
    * and {@link HikariDataSource#getConnection()} is called.
    * <ul>
    * <li>Any value greater than zero will be treated as a timeout for pool initialization.
    * The calling thread will be blocked from continuing until a successful connection
    * to the database, or until the timeout is reached.  If the timeout is reached, then
    * a {@code PoolInitializationException} will be thrown. </li>
    * <li>A value of zero will <i>not</i>  prevent the pool from starting in the
    * case that a connection cannot be obtained. However, upon start the pool will
    * attempt to obtain a connection and validate that the {@code connectionTestQuery}
    * and {@code connectionInitSql} are valid.  If those validations fail, an exception
    * will be thrown.  If a connection cannot be obtained, the validation is skipped
    * and the the pool will start and continue to try to obtain connections in the
    * background.  This can mean that callers to {@code DataSource#getConnection()} may
    * encounter exceptions. </li>
    * <li>A value less than zero will bypass any connection attempt and validation during
    * startup, and therefore the pool will start immediately.  The pool will continue to
    * try to obtain connections in the background. This can mean that callers to
    * {@code DataSource#getConnection()} may encounter exceptions. </li>
    * </ul>
    * Note that if this timeout value is greater than or equal to zero (0), and therefore an
    * initial connection validation is performed, this timeout does not override the
    * {@code connectionTimeout} or {@code validationTimeout}; they will be honored before this
    * timeout is applied.  The default value is one millisecond.
    *
    * @param initializationFailTimeout the number of milliseconds before the
    *                                  pool initialization fails, or 0 to validate connection setup but continue with
    *                                  pool start, or less than zero to skip all initialization checks and start the
    *                                  pool without delay.
    */
   public void setInitializationFailTimeout(long initializationFailTimeout) {
      checkIfSealed();
      this.initializationFailTimeout = initializationFailTimeout;
   }

   /**
    * Determine whether internal pool queries, principally aliveness checks, will be isolated in their own transaction
    * via {@link Connection#rollback()}.  Defaults to {@code false}.
    *
    * @return {@code true} if internal pool queries are isolated, {@code false} if not
    */
   public boolean isIsolateInternalQueries() {
      return isIsolateInternalQueries;
   }

   /**
    * Configure whether internal pool queries, principally aliveness checks, will be isolated in their own transaction
    * via {@link Connection#rollback()}.  Defaults to {@code false}.
    *
    * @param isolate {@code true} if internal pool queries should be isolated, {@code false} if not
    */
   public void setIsolateInternalQueries(boolean isolate) {
      checkIfSealed();
      this.isIsolateInternalQueries = isolate;
   }

   public MetricsTrackerFactory getMetricsTrackerFactory() {
      return metricsTrackerFactory;
   }

   public void setMetricsTrackerFactory(MetricsTrackerFactory metricsTrackerFactory) {
      if (metricRegistry != null) {
         throw new IllegalStateException("cannot use setMetricsTrackerFactory() and setMetricRegistry() together");
      }

      this.metricsTrackerFactory = metricsTrackerFactory;
   }

   /**
    * Get the MetricRegistry instance to used for registration of metrics used by HikariCP.  Default is {@code null}.
    *
    * @return the MetricRegistry instance that will be used
    */
   public Object getMetricRegistry() {
      return metricRegistry;
   }

   /**
    * Set a MetricRegistry instance to use for registration of metrics used by HikariCP.
    *
    * @param metricRegistry the MetricRegistry instance to use
    */
   public void setMetricRegistry(Object metricRegistry) {
      if (metricsTrackerFactory != null) {
         throw new IllegalStateException("cannot use setMetricRegistry() and setMetricsTrackerFactory() together");
      }

      if (metricRegistry != null) {
         metricRegistry = getObjectOrPerformJndiLookup(metricRegistry);

         if (!safeIsAssignableFrom(metricRegistry, "com.codahale.metrics.MetricRegistry")
            && !(safeIsAssignableFrom(metricRegistry, "io.micrometer.core.instrument.MeterRegistry"))) {
            throw new IllegalArgumentException("Class must be instance of com.codahale.metrics.MetricRegistry or io.micrometer.core.instrument.MeterRegistry");
         }
      }

      this.metricRegistry = metricRegistry;
   }

   /**
    * Get the HealthCheckRegistry that will be used for registration of health checks by HikariCP.  Currently only
    * Codahale/DropWizard is supported for health checks.
    *
    * @return the HealthCheckRegistry instance that will be used
    */
   public Object getHealthCheckRegistry() {
      return healthCheckRegistry;
   }

   /**
    * Set the HealthCheckRegistry that will be used for registration of health checks by HikariCP.  Currently only
    * Codahale/DropWizard is supported for health checks.  Default is {@code null}.
    *
    * @param healthCheckRegistry the HealthCheckRegistry to be used
    */
   public void setHealthCheckRegistry(Object healthCheckRegistry) {
      checkIfSealed();

      if (healthCheckRegistry != null) {
         healthCheckRegistry = getObjectOrPerformJndiLookup(healthCheckRegistry);

         if (!(healthCheckRegistry instanceof HealthCheckRegistry)) {
            throw new IllegalArgumentException("Class must be an instance of com.codahale.metrics.health.HealthCheckRegistry");
         }
      }

      this.healthCheckRegistry = healthCheckRegistry;
   }

   public Properties getHealthCheckProperties() {
      return healthCheckProperties;
   }

   public void setHealthCheckProperties(Properties healthCheckProperties) {
      checkIfSealed();
      this.healthCheckProperties.putAll(healthCheckProperties);
   }

   public void addHealthCheckProperty(String key, String value) {
      checkIfSealed();
      healthCheckProperties.setProperty(key, value);
   }

   /**
    * Determine whether the Connections in the pool are in read-only mode.
    *
    * @return {@code true} if the Connections in the pool are read-only, {@code false} if not
    */
   public boolean isReadOnly() {
      return isReadOnly;
   }

   /**
    * Configures the Connections to be added to the pool as read-only Connections.
    *
    * @param readOnly {@code true} if the Connections in the pool are read-only, {@code false} if not
    */
   public void setReadOnly(boolean readOnly) {
      checkIfSealed();
      this.isReadOnly = readOnly;
   }

   /**
    * Determine whether HikariCP will self-register {@link HikariConfigMXBean} and {@link HikariPoolMXBean} instances
    * in JMX.
    *
    * @return {@code true} if HikariCP will register MXBeans, {@code false} if it will not
    */
   public boolean isRegisterMbeans() {
      return isRegisterMbeans;
   }

   /**
    * Configures whether HikariCP self-registers the {@link HikariConfigMXBean} and {@link HikariPoolMXBean} in JMX.
    *
    * @param register {@code true} if HikariCP should register MXBeans, {@code false} if it should not
    */
   public void setRegisterMbeans(boolean register) {
      checkIfSealed();
      this.isRegisterMbeans = register;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public String getPoolName() {
      return poolName;
   }

   /**
    * Set the name of the connection pool.  This is primarily used for the MBean
    * to uniquely identify the pool configuration.
    *
    * @param poolName the name of the connection pool to use
    */
   public void setPoolName(String poolName) {
      checkIfSealed();
      this.poolName = poolName;
   }

   /**
    * Get the ScheduledExecutorService used for housekeeping.
    *
    * @return the executor
    */
   public ScheduledExecutorService getScheduledExecutor() {
      return scheduledExecutor;
   }

   /**
    * Set the ScheduledExecutorService used for housekeeping.
    *
    * @param executor the ScheduledExecutorService
    */
   public void setScheduledExecutor(ScheduledExecutorService executor) {
      checkIfSealed();
      this.scheduledExecutor = executor;
   }

   public String getTransactionIsolation() {
      return transactionIsolationName;
   }

   /**
    * Get the default schema name to be set on connections.
    *
    * @return the default schema name
    */
   public String getSchema() {
      return schema;
   }

   /**
    * Set the default schema name to be set on connections.
    *
    * @param schema the name of the default schema
    */
   public void setSchema(String schema) {
      checkIfSealed();
      this.schema = schema;
   }

   /**
    * Set the default transaction isolation level.  The specified value is the
    * constant name from the <code>Connection</code> class, eg.
    * <code>TRANSACTION_REPEATABLE_READ</code>.
    *
    * @param isolationLevel the name of the isolation level
    */
   public void setTransactionIsolation(String isolationLevel) {
      checkIfSealed();
      this.transactionIsolationName = isolationLevel;
   }

   /**
    * Get the thread factory used to create threads.
    *
    * @return the thread factory (may be null, in which case the default thread factory is used)
    */
   public ThreadFactory getThreadFactory() {
      return threadFactory;
   }

   /**
    * Set the thread factory to be used to create threads.
    *
    * @param threadFactory the thread factory (setting to null causes the default thread factory to be used)
    */
   public void setThreadFactory(ThreadFactory threadFactory) {
      checkIfSealed();
      this.threadFactory = threadFactory;
   }

   void seal() {
      this.sealed = true;
   }

   /**
    * 将Hikari数据库连接池的配置复制给另一个，
    * 主要就是通过字段的反射赋值将值一对一赋给新的配置对象
    * Copies the state of {@code this} into {@code other}.
    *
    * @param other Other {@link HikariConfig} to copy the state to.
    */
   public void copyStateTo(HikariConfig other) {
      for (Field field : HikariConfig.class.getDeclaredFields()) {
         if (!Modifier.isFinal(field.getModifiers())) {
            field.setAccessible(true);
            try {
               field.set(other, field.get(this));
            } catch (Exception e) {
               throw new RuntimeException("Failed to copy HikariConfig state: " + e.getMessage(), e);
            }
         }
      }

      other.sealed = false;
   }

   // ***********************************************************************
   //                          Private methods
   // ***********************************************************************

   private Class<?> attemptFromContextLoader(final String driverClassName) {
      final ClassLoader threadContextClassLoader = Thread.currentThread().getContextClassLoader();
      if (threadContextClassLoader != null) {
         try {
            final Class<?> driverClass = threadContextClassLoader.loadClass(driverClassName);
            LOGGER.debug("Driver class {} found in Thread context class loader {}", driverClassName, threadContextClassLoader);
            return driverClass;
         } catch (ClassNotFoundException e) {
            LOGGER.debug("Driver class {} not found in Thread context class loader {}, trying classloader {}",
               driverClassName, threadContextClassLoader, this.getClass().getClassLoader());
         }
      }

      return null;
   }

   /**
    * 参数验证的方法，在创建连接池和获取连接的时候调用。
    * 主要是验证必要的字符串不能为空，并且数字形式的参数需要值在限定的范围内。
    */
   @SuppressWarnings("StatementWithEmptyBody")
   public void validate() {
      if (poolName == null) {
         poolName = generatePoolName();
      } else if (isRegisterMbeans && poolName.contains(":")) {
         throw new IllegalArgumentException("poolName cannot contain ':' when used with JMX");
      }

      // treat empty property as null
      //noinspection NonAtomicOperationOnVolatileField
      catalog = getNullIfEmpty(catalog);
      connectionInitSql = getNullIfEmpty(connectionInitSql);
      connectionTestQuery = getNullIfEmpty(connectionTestQuery);
      transactionIsolationName = getNullIfEmpty(transactionIsolationName);
      dataSourceClassName = getNullIfEmpty(dataSourceClassName);
      dataSourceJndiName = getNullIfEmpty(dataSourceJndiName);
      driverClassName = getNullIfEmpty(driverClassName);
      jdbcUrl = getNullIfEmpty(jdbcUrl);

      // Check Data Source Options
      if (dataSource != null) {
         if (dataSourceClassName != null) {
            LOGGER.warn("{} - using dataSource and ignoring dataSourceClassName.", poolName);
         }
      } else if (dataSourceClassName != null) {
         if (driverClassName != null) {
            LOGGER.error("{} - cannot use driverClassName and dataSourceClassName together.", poolName);
            // NOTE: This exception text is referenced by a Spring Boot FailureAnalyzer, it should not be
            // changed without first notifying the Spring Boot developers.
            throw new IllegalStateException("cannot use driverClassName and dataSourceClassName together.");
         } else if (jdbcUrl != null) {
            LOGGER.warn("{} - using dataSourceClassName and ignoring jdbcUrl.", poolName);
         }
      } else if (jdbcUrl != null || dataSourceJndiName != null) {
         // ok
      } else if (driverClassName != null) {
         LOGGER.error("{} - jdbcUrl is required with driverClassName.", poolName);
         throw new IllegalArgumentException("jdbcUrl is required with driverClassName.");
      } else {
         LOGGER.error("{} - dataSource or dataSourceClassName or jdbcUrl is required.", poolName);
         throw new IllegalArgumentException("dataSource or dataSourceClassName or jdbcUrl is required.");
      }

      validateNumerics();

      if (LOGGER.isDebugEnabled() || unitTest) {
         logConfiguration();
      }
   }

   /**
    * 验证数字参数的合法性
    */
   private void validateNumerics() {
      if (maxLifetime != 0 && maxLifetime < SECONDS.toMillis(30)) {
         LOGGER.warn("{} - maxLifetime is less than 30000ms, setting to default {}ms.", poolName, MAX_LIFETIME);
         maxLifetime = MAX_LIFETIME;
      }

      if (leakDetectionThreshold > 0 && !unitTest) {
         if (leakDetectionThreshold < SECONDS.toMillis(2) || (leakDetectionThreshold > maxLifetime && maxLifetime > 0)) {
            LOGGER.warn("{} - leakDetectionThreshold is less than 2000ms or more than maxLifetime, disabling it.", poolName);
            leakDetectionThreshold = 0;
         }
      }

      if (connectionTimeout < 250) {
         LOGGER.warn("{} - connectionTimeout is less than 250ms, setting to {}ms.", poolName, CONNECTION_TIMEOUT);
         connectionTimeout = CONNECTION_TIMEOUT;
      }

      if (validationTimeout < 250) {
         LOGGER.warn("{} - validationTimeout is less than 250ms, setting to {}ms.", poolName, VALIDATION_TIMEOUT);
         validationTimeout = VALIDATION_TIMEOUT;
      }

      if (maxPoolSize < 1) {
         maxPoolSize = DEFAULT_POOL_SIZE;
      }

      if (minIdle < 0 || minIdle > maxPoolSize) {
         minIdle = maxPoolSize;
      }

      if (idleTimeout + SECONDS.toMillis(1) > maxLifetime && maxLifetime > 0 && minIdle < maxPoolSize) {
         LOGGER.warn("{} - idleTimeout is close to or more than maxLifetime, disabling it.", poolName);
         idleTimeout = 0;
      } else if (idleTimeout != 0 && idleTimeout < SECONDS.toMillis(10) && minIdle < maxPoolSize) {
         LOGGER.warn("{} - idleTimeout is less than 10000ms, setting to default {}ms.", poolName, IDLE_TIMEOUT);
         idleTimeout = IDLE_TIMEOUT;
      } else if (idleTimeout != IDLE_TIMEOUT && idleTimeout != 0 && minIdle == maxPoolSize) {
         LOGGER.warn("{} - idleTimeout has been set but has no effect because the pool is operating as a fixed size pool.", poolName);
      }
   }

   /**
    *在很多的setter方法中最先执行的就是这个检查关闭的方法，
    * 因为有一些参数是运行过程中不能修改的，
    * 所以需要加一个这样的限制。
    * 这个成员变量是一个volatile类型
    */
   private void checkIfSealed() {
      if (sealed)
         throw new IllegalStateException("The configuration of the pool is sealed once started. Use HikariConfigMXBean for runtime changes.");
   }

   /**
    * 这个方法是在验证参数的时候，验证通过后如果是单元测试或者是debug时候可以打印出连接池参数
    */
   private void logConfiguration() {
      LOGGER.debug("{} - configuration:", poolName);
      final Set<String> propertyNames = new TreeSet<>(PropertyElf.getPropertyNames(HikariConfig.class));
      for (String prop : propertyNames) {
         try {
            Object value = PropertyElf.getProperty(prop, this);
            if ("dataSourceProperties".equals(prop)) {
               Properties dsProps = PropertyElf.copyProperties(dataSourceProperties);
               dsProps.setProperty("password", "<masked>");
               value = dsProps;
            }

            if ("initializationFailTimeout".equals(prop) && initializationFailTimeout == Long.MAX_VALUE) {
               value = "infinite";
            } else if ("transactionIsolation".equals(prop) && transactionIsolationName == null) {
               value = "default";
            } else if (prop.matches("scheduledExecutorService|threadFactory") && value == null) {
               value = "internal";
            } else if (prop.contains("jdbcUrl") && value instanceof String) {
               value = ((String) value).replaceAll("([?&;]password=)[^&#;]*(.*)", "$1<masked>$2");
            } else if (prop.contains("password")) {
               value = "<masked>";
            } else if (value instanceof String) {
               value = "\"" + value + "\""; // quote to see lead/trailing spaces is any
            } else if (value == null) {
               value = "none";
            }
            LOGGER.debug((prop + "................................................").substring(0, 32) + value);
         } catch (Exception e) {
            // continue
         }
      }
   }

   /**
    * 加载配置文件的参数到成员变量中
    * @param propertyFileName
    */
   private void loadProperties(String propertyFileName) {
      final File propFile = new File(propertyFileName);
      try (final InputStream is = propFile.isFile() ? new FileInputStream(propFile) : this.getClass().getResourceAsStream(propertyFileName)) {
         if (is != null) {
            Properties props = new Properties();
            props.load(is);
            PropertyElf.setTargetFromProperties(this, props);
         } else {
            throw new IllegalArgumentException("Cannot find property file: " + propertyFileName);
         }
      } catch (IOException io) {
         throw new RuntimeException("Failed to read property file", io);
      }
   }

   /**
    * 自动生成线程池名称的方法，检查是否有设定的线程池数量值，有的话+1，
    * 否则就是通过随机数产生名称，前缀都是HikaraPool-
    */
   private String generatePoolName() {
      final String prefix = "HikariPool-";
      try {
         // Pool number is global to the VM to avoid overlapping pool numbers in classloader scoped environments
         synchronized (System.getProperties()) {
            final String next = String.valueOf(Integer.getInteger("com.zaxxer.hikari.pool_number", 0) + 1);
            System.setProperty("com.zaxxer.hikari.pool_number", next);
            return prefix + next;
         }
      } catch (AccessControlException e) {
         // The SecurityManager didn't allow us to read/write system properties
         // so just generate a random pool number instead
         final ThreadLocalRandom random = ThreadLocalRandom.current();
         final StringBuilder buf = new StringBuilder(prefix);

         for (int i = 0; i < 4; i++) {
            buf.append(ID_CHARACTERS[random.nextInt(62)]);
         }

         LOGGER.info("assigned random pool name '{}' (security manager prevented access to system properties)", buf);

         return buf.toString();
      }
   }

   /**
    * 通过JNDI查找资源的方法
    * @param object
    * @return
    */
   private Object getObjectOrPerformJndiLookup(Object object) {
      if (object instanceof String) {
         try {
            InitialContext initCtx = new InitialContext();
            return initCtx.lookup((String) object);
         } catch (NamingException e) {
            throw new IllegalArgumentException(e);
         }
      }
      return object;
   }
}
