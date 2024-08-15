package com.lk.setl.sql.catalyst.expressions

import com.lk.setl.Logging

import scala.util.control.NonFatal
import com.lk.setl.util.Utils

/**
 * Defines values for `SQLConf` config of fallback mode. Use for test only.
 */
object CodegenObjectFactoryMode extends Enumeration {
  val FALLBACK, CODEGEN_ONLY, NO_CODEGEN = Value
}

/**
 * A codegen object generator which creates objects with codegen path first. Once any compile
 * error happens, it can fallback to interpreted implementation. In tests, we can use a SQL config
 * `SQLConf.CODEGEN_FACTORY_MODE` to control fallback behavior.
 */
abstract class CodeGeneratorWithInterpretedFallback[IN, OUT] extends Logging {

  def createObject(in: IN): OUT = createObject(in, CodegenObjectFactoryMode.FALLBACK)

  def createObject(in: IN, fallbackMode: CodegenObjectFactoryMode.Value): OUT = {
    // We are allowed to choose codegen-only or no-codegen modes if under tests.
    fallbackMode match {
      case CodegenObjectFactoryMode.CODEGEN_ONLY if Utils.isTesting =>
        createCodeGeneratedObject(in)
      case CodegenObjectFactoryMode.NO_CODEGEN if Utils.isTesting =>
        createInterpretedObject(in)
      case _ =>
        try {
          createCodeGeneratedObject(in)
        } catch {
          case NonFatal(e) =>
            logWarning("Expr codegen error and falling back to interpreter mode", e)
            createInterpretedObject(in)
        }
    }
  }

  protected def createCodeGeneratedObject(in: IN): OUT
  protected def createInterpretedObject(in: IN): OUT
}
