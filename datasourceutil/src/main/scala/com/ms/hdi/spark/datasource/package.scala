package com.ms.hdi.spark

package object datasource {


  import java.util.function.{Function => JFunction}

  /**
   * convert scala function with one argument to java
   * val x = (a:String)=>a.toInt
   * @param f
   * @tparam A
   * @tparam B
   * @return
   */
  implicit def toJavaFunction[A, B](f: Function1[A, B]) = new JFunction[A, B] {
    override def apply(a: A): B = f(a)
  }
}
