package com.ms.hdi.spark.datasource.util

import com.ms.hdi.spark.datasource.model.DataGenObj

import scala.reflect.runtime.universe

/**
 * Reflection related Util functions
 */
object ReflectionUtil {

  /**
   * get type from given class name as string
   * @param name
   * @return
   */
  def getTypeFromStringClassName(name: String): universe.Type = {
    import scala.reflect.runtime.{universe => ru}
    val classInstance = Class.forName(name)
    val mirror: ru.Mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol: ru.ClassSymbol = mirror.classSymbol(classInstance)
    classSymbol.selfType
  }

  /**
   * get class instance for given class name
   * @param name
   * @return
   */
  def getClassInstance(name: String): DataGenObj = {
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val module = runtimeMirror.staticModule(name)
    val obj= runtimeMirror.reflectModule(module)
    obj.instance.asInstanceOf[DataGenObj]
  }
}
