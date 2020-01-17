package com.atguigu.streaming.project.utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object RedisUtil {
  private val poolConfig = new JedisPoolConfig
      poolConfig.setMaxIdle(40)
      poolConfig.setMinIdle(20)
      poolConfig.setMaxTotal(60)
      poolConfig.setBlockWhenExhausted(true)
      poolConfig.setMaxWaitMillis(5000)
      poolConfig.setTestOnBorrow(true)
      poolConfig.setTestOnReturn(true)
  private val jedisPool = new JedisPool(poolConfig,"hadoop103",6379)

  def getJedisClient = jedisPool.getResource



}

