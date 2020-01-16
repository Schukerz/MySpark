package com.atguigu.mock.streaming.project.utils

import redis.clients.jedis.JedisPoolConfig

object RedisUtil {
  def getJedisClient()= {


    val config: JedisPoolConfig = new JedisPoolConfig
  }
}
