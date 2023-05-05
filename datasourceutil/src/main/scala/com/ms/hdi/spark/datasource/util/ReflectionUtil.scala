package com.ms.hdi.spark.datasource.util

import com.ms.hdi.example.model.BaseDataGen
import com.ms.hdi.model.mockobj.DataGenObj
import net.andreinc.mockneat.MockNeat
import net.andreinc.mockneat.abstraction.MockUnit

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
    val classInstance: Class[_] = Class.forName(name)
    val mirror: ru.Mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol: ru.ClassSymbol = mirror.classSymbol(classInstance)
    classSymbol.selfType
  }

  /**
   * get mock unit to generate data from given object name
   * @param staticSchemaCompanionObject companion object full qualified name
   * @param schemaClass business model case class name
   * @param mockNeat  mockneat
   * @param index index/seed/partition#
   * @return
   */
  def getMockUnit(staticSchemaCompanionObject: String, schemaClass: Class[_], mockNeat:MockNeat, index:Int): MockUnit[BaseDataGen] = {
    val runtimeMirror: universe.Mirror = universe.runtimeMirror(getClass.getClassLoader)
    val module: universe.ModuleSymbol = runtimeMirror.staticModule(staticSchemaCompanionObject)
    val obj: universe.ModuleMirror = runtimeMirror.reflectModule(module)
    val mockNeatParameters: (MockNeat, Int) => List[Object] = obj.instance.asInstanceOf[DataGenObj].mockNeatParameters
    val params1 = mockNeatParameters.apply(mockNeat,index)
    mockNeat.constructor(schemaClass).params(params1: _*).asInstanceOf[MockUnit[BaseDataGen]]
  }
}
