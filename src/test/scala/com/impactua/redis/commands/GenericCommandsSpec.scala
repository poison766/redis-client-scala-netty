package com.impactua.redis.commands

import com.impactua.redis.TestClient
import org.scalatest.{FlatSpec, Matchers}
import com.impactua.redis.primitives.Redlock._

class GenericCommandsSpec extends FlatSpec with Matchers with TestClient {

  "A ping" should "just work" in {
    client.ping shouldBe true
  }

  "An exists" should "check if key exists" in {
    client.exists("foo") shouldBe false
    client.set("foo", "bar", 1) shouldBe true
    client.exists("foo") shouldBe true
    client.del("foo") shouldEqual 1
    client.exists("foo") shouldBe false
    client.del("foo") shouldEqual 0
    client.ttl("foo") shouldEqual -2
  }

  "An expire" should "expire keys" in {
    client.set("foo", "bar") shouldBe true
    client.expire("foo", 100) shouldBe true
    client.ttl("foo") shouldEqual 100
    client.persist("foo") shouldBe true
    client.ttl("foo") shouldEqual -1
    client.ttl("somekey") shouldEqual -2
  }

  "Keys" should "return all matched keys" in {
    client.set("prefix:1" -> 1, "prefix:2" -> 2) shouldBe true
    client.keys("prefix:*") shouldEqual Set("prefix:2", "prefix:1")
    client.del("prefix:1") shouldBe 1
    client.del("prefix:2") shouldBe 1
  }

  "A transaction" should "allow to set values" in {
    client.withTransaction { cli =>
      cli.setAsync("tx_key", "tx_val")
    }
  }

  "Scripting" should "accept and execute scripts" in {

    client.eval[Int]("return ARGV[1];", ("anyKey", "2")) shouldEqual Set(2)
    client.scriptLoad("return ARGV[1];") shouldEqual "4629ab89363d08ca29abd4bb0aaf5ed70e2bb228"
    client.evalsha[Int]("4629ab89363d08ca29abd4bb0aaf5ed70e2bb228", ("key", "4")) shouldEqual Set(4)

    client.setNx("lock_key", "lock_value") shouldEqual true
    client.eval[Int](UNLOCK_SCRIPT, ("lock_key", "no_lock_value")) shouldEqual Set(0)
    client.eval[Int](UNLOCK_SCRIPT, ("lock_key", "lock_value")) shouldEqual Set(1)

    client.scriptExists("4629ab89363d08ca29abd4bb0aaf5ed70e2bb228") shouldBe true
    client.scriptFlush() shouldBe true

    client.scriptExists("4629ab89363d08ca29abd4bb0aaf5ed70e2bb228") shouldBe false
  }

}
