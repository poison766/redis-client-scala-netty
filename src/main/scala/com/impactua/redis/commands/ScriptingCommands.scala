package com.impactua.redis.commands

import com.impactua.redis.commands.ClientCommands._
import com.impactua.redis.connections._
import com.impactua.redis.{BinaryConverter, ScriptSyntaxException, UnsupportedResponseException}

/**
 * http://redis.io/commands#scripting
 */
private[redis] trait ScriptingCommands extends ClientCommands {

  def evalAsync[T](script: String, kvs: (String, String)*)(implicit conv: BinaryConverter[T]) =
    r.send(Eval(script, kvs.map{kv => kv._1 -> BinaryConverter.StringConverter.write(kv._2)})).map(bulkResultToSet(conv))

  def eval[T](script: String, kvs: (String, String)*)(implicit conv: BinaryConverter[T]) = await { evalAsync(script, kvs: _*) }

  def evalshaAsync[T](script: String, kvs: (String, String)*)(implicit conv: BinaryConverter[T]) =
    r.send(EvalSha(script, kvs.map{kv => kv._1 -> BinaryConverter.StringConverter.write(kv._2)})).map(bulkResultToSet(conv))

  def evalsha[T](digest: String, kvs: (String, String)*)(implicit conv: BinaryConverter[T]) = await { evalshaAsync(digest, kvs: _*) }

  def scriptLoadAsync(script: String) = r.send(ScriptLoad(script)).map {
    case BulkDataResult(Some(data)) =>
      new String(data)
    case ErrorResult(err) =>
      throw ScriptSyntaxException(err)
    case unknown =>
      throw UnsupportedResponseException("Unsupported response type: " + unknown)
  }

  def scriptLoad(script: String) = await { scriptLoadAsync(script) }

  def scriptKillAsync() = r.send(ScriptKill()).map(okResultAsBoolean)

  def scriptKill() = await { scriptKillAsync() }

  def scriptFlushAsync() = r.send(ScriptFlush()).map(okResultAsBoolean)

  def scriptFlush() = await { scriptFlushAsync() }

  def scriptExistsAsync(script: String) = r.send(ScriptExists(script)).map {
    case BulkDataResult(Some(data)) => !"0".equals(new String(data))
    case MultiBulkDataResult(List(BulkDataResult(Some(data)))) => !"0".equals(new String(data))
    case unknown =>
      throw UnsupportedResponseException("Unsupported response type: " + unknown)
  }

  def scriptExists(script: String) = await { scriptExistsAsync(script) }


}
