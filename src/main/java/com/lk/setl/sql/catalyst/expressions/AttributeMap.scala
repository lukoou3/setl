package com.lk.setl.sql.catalyst.expressions

/**
 * Builds a map that is keyed by an Attribute's expression id. Using the expression id allows values
 * to be looked up even when the attributes used differ cosmetically (i.e., the capitalization
 * of the name, or the expected nullability).
 */
object AttributeMap {
  def apply[A](kvs: Seq[(Attribute, A)]): AttributeMap[A] = {
    new AttributeMap(kvs.map(kv => (kv._1.exprId, kv)).toMap)
  }

  def empty[A]: AttributeMap[A] = new AttributeMap(Map.empty)
}

class AttributeMap[A](val baseMap: Map[Long, (Attribute, A)])
  extends Map[Attribute, A] with Serializable {

  //  Note: this class supports Scala 2.12. A parallel source tree has a 2.13 implementation.

  override def get(k: Attribute): Option[A] = baseMap.get(k.exprId).map(_._2)

  override def contains(k: Attribute): Boolean = get(k).isDefined

  override def + [B1 >: A](kv: (Attribute, B1)): Map[Attribute, B1] = baseMap.values.toMap + kv

  override def iterator: Iterator[(Attribute, A)] = baseMap.valuesIterator

  override def -(key: Attribute): Map[Attribute, A] = baseMap.values.toMap - key
}
