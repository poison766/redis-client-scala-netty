package com.impactua.redis.commands

import com.impactua.redis.TestClient
import org.scalatest.{FlatSpec, Matchers}

/**
  *
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 13.02.2018.
  */
class ListCommandsSpec extends FlatSpec with Matchers with TestClient {

  "A lpush" should "insert all the specified values at the head of the list stored at key" in {
    client.lpush("key-lpush", "world") shouldEqual 1
    client.lpush("key-lpush", "hello") shouldEqual 2

    client.lrange[String]("key-lpush", 0, -1) shouldEqual Seq("hello", "world")
  }

  "A lpushx" should "inserts value at the head of the list stored at key, only if key already exists and holds a list" in {
    client.lpush("key-lpushx", "world") shouldEqual 1
    client.lpushx("key-lpushx", "hello") shouldEqual 2

    client.lpushx("key-lpushx-unexist", "hello") shouldEqual 0

    client.lrange[String]("key-lpushx", 0, -1) shouldEqual Seq("hello", "world")
    client.lrange[String]("key-lpushx-unexist", 0, -1) shouldEqual Nil
  }

  "A rpush" should "insert all the specified values at the tail of the list stored at key" in {
    client.rpush("key-rpush", "hello") shouldEqual 1
    client.rpush("key-rpush", "world") shouldEqual 2

    client.lrange[String]("key-lpush", 0, -1) shouldEqual Seq("hello", "world")
  }

  "A rpushx" should "inserts value at the tail of the list stored at key, only if key already exists and holds a list" in {
    client.rpush("key-rpushx", "Hello") shouldEqual 1
    client.rpushx("key-rpushx", "World") shouldEqual 2

    client.lrange[String]("key-rpushx", 0, -1) shouldEqual Seq("Hello", "World")

    client.rpushx("no-key-rpushx", "World") shouldEqual 0
    client.lrange[String]("no-key-rpushx", 0, -1) shouldEqual Nil
  }

  "A lindex" should "returns the element at index `index` in the list stored at key" in {
    client.lpush("key-lindex", "world") shouldEqual 1
    client.lpush("key-lindex", "hello") shouldEqual 2

    client.lindex[String]("key-lindex", 0) shouldEqual Some("hello")
    client.lindex[String]("key-lindex", -1) shouldEqual Some("world")
    client.lindex[String]("key-lindex", 3) shouldEqual None
  }

  "A linsert" should "inserts value in the list stored at key either before or after the reference value `pivot`" in {
    client.rpush("key-linsert", "hello") shouldEqual 1
    client.rpush("key-linsert", "world") shouldEqual 2

    client.linsert[String]("key-linsert", "world", "there") shouldEqual 3
    client.lrange[String]("key-linsert", 0, -1) shouldEqual Seq("hello", "there", "world")
  }

  "A llen" should "returns the length of the list stored at key" in {
    client.rpush("key-llen", "hello") shouldEqual 1
    client.rpush("key-llen", "world") shouldEqual 2

    client.llen("key-llen") shouldEqual 2
  }

  "A lpop" should "removes and returns the first element of the list stored at key" in {
    client.rpush("key-lpop", "hello") shouldEqual 1
    client.rpush("key-lpop", "world") shouldEqual 2

    client.lpop[String]("key-lpop") shouldEqual Some("hello")
    client.lpop[String]("key-lpop") shouldEqual Some("world")
    client.lpop[String]("key-lpop") shouldEqual None
  }

  "A lrem" should "removes the first count occurrences of elements equal to value from the list stored at key" in {
    client.rpush("key-lrem", "hello") shouldEqual 1
    client.rpush("key-lrem", "hello") shouldEqual 2
    client.rpush("key-lrem", "world") shouldEqual 3
    client.rpush("key-lrem", "hello2") shouldEqual 4

    client.lrem[String]("key-lrem", -2, "hello") shouldEqual 2
    client.lrange[String]("key-lrem", 0, -1) shouldEqual Seq("world", "hello2")
  }

  "A lset" should "sets the list element at index to value" in {
    client.rpush("key-lset", "one") shouldEqual 1
    client.rpush("key-lset", "two") shouldEqual 2
    client.rpush("key-lset", "three") shouldEqual 3

    client.lset("key-lset", 0, "four") shouldEqual true
    client.lset("key-lset", -2, "five") shouldEqual true

    client.lrange[String]("key-lset", 0, -1) shouldEqual List("four", "five", "three")
  }

  "A ltrim" should "trim an existing list so that it will contain only the specified range of elements specified" in {
    client.rpush("key-ltrim", "one") shouldEqual 1
    client.rpush("key-ltrim", "two") shouldEqual 2
    client.rpush("key-ltrim", "three") shouldEqual 3

    client.ltrim("key-ltrim", 1, -1) shouldEqual true

    client.lrange[String]("key-ltrim", 0, -1) shouldEqual List("two", "three")
  }

  "A rpop" should "removes and returns the last element of the list stored at key" in {
    client.rpush("key-rpop", "one") shouldEqual 1
    client.rpush("key-rpop", "two") shouldEqual 2
    client.rpush("key-rpop", "three") shouldEqual 3

    client.rpop[String]("key-rpop") shouldEqual Some("three")
    client.lrange[String]("key-rpop", 0, -1) shouldEqual List("one", "two")
  }

  "A rpoplpush" should "returns and removes the last element of the list stored at source, and pushes the element at the first element of the list stored at destination" in {
    client.rpush("key-rpoplpush", "one") shouldEqual 1
    client.rpush("key-rpoplpush", "two") shouldEqual 2
    client.rpush("key-rpoplpush", "three") shouldEqual 3

    client.rpoplpush[String]("key-rpoplpush", "key-rpoplpush2") shouldEqual Some("three")

    client.lrange[String]("key-rpoplpush", 0, -1) shouldEqual List("one", "two")
    client.lrange[String]("key-rpoplpush2", 0, -1) shouldEqual List("three")
  }

}
