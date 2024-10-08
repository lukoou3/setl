package com.lk.setl.sql.catalyst.trees

import com.lk.setl.sql.catalyst.errors.{TreeNodeException, attachTree}
import com.lk.setl.sql.catalyst.util.StringUtils.PlanStringConcat
import com.lk.setl.sql.catalyst.util.truncatedString
import com.lk.setl.sql.types.{DataType, StructType}
import com.lk.setl.util.Utils
import org.apache.commons.lang3.ClassUtils

import java.util.UUID
import scala.collection.{Map, mutable}
import scala.reflect.ClassTag

/** Used by [[TreeNode.getNodeNumbered]] when traversing the tree for a given number */
private class MutableInt(var i: Int)

case class Origin(
  line: Option[Int] = None,
  startPosition: Option[Int] = None)

/**
 *
 * CurrentOrigin提供树节点的位置，以询问其起源的上下文。例如，当前正在分析哪一行代码。
 * Provides a location for TreeNodes to ask about the context of their origin.  For example, which
 * line of code is currently being parsed.
 */
object CurrentOrigin {
  /**
   * Origin 表示第几行第几列, 目前 CurrentOrigin 仅在 parser 中使用，在 visit 每个节点的时候都会使用，记录当前 parse 的节点是哪行哪列
   * 从 value 是 ThreadLocal 类型可以看出，在 Spark SQL 中，parse sql 时都是在单独的 thread 里进行的（不同的 sql 不同的 thread）
   */
  private val value = new ThreadLocal[Origin]() {
    override def initialValue: Origin = Origin()
  }

  def get: Origin = value.get()
  def set(o: Origin): Unit = value.set(o)

  def reset(): Unit = value.set(Origin())

  def setPosition(line: Int, start: Int): Unit = {
    value.set(
      value.get.copy(line = Some(line), startPosition = Some(start)))
  }

  def withOrigin[A](o: Origin)(f: => A): A = {
    set(o)
    val ret = try f finally { reset() }
    ret
  }
}

// A tag of a `TreeNode`, which defines name and type
case class TreeNodeTag[T](name: String)

// scalastyle:off
abstract class TreeNode[BaseType <: TreeNode[BaseType]] extends Product {
  // scalastyle:on
  self: BaseType =>

  val origin: Origin = CurrentOrigin.get

  /**
   * A mutable map for holding auxiliary information of this tree node. It will be carried over
   * when this node is copied via `makeCopy`, or transformed via `transformUp`/`transformDown`.
   */
  private val tags: mutable.Map[TreeNodeTag[_], Any] = mutable.Map.empty

  def copyTagsFrom(other: BaseType): Unit = {
    // SPARK-32753: it only makes sense to copy tags to a new node
    // but it's too expensive to detect other cases likes node removal
    // so we make a compromise here to copy tags to node with no tags
    if (tags.isEmpty) {
      tags ++= other.tags
    }
  }

  def setTagValue[T](tag: TreeNodeTag[T], value: T): Unit = {
    tags(tag) = value
  }

  def getTagValue[T](tag: TreeNodeTag[T]): Option[T] = {
    tags.get(tag).map(_.asInstanceOf[T])
  }

  def unsetTagValue[T](tag: TreeNodeTag[T]): Unit = {
    tags -= tag
  }

  /**
   * 返回该节点的 seq of children，children 是不可变的。
   * Returns a Seq of the children of this node.
   * Children should not change. Immutability required for containsChild optimization
   */
  def children: Seq[BaseType]

  lazy val containsChild: Set[TreeNode[_]] = children.toSet

  // Copied from Scala 2.13.1
  // github.com/scala/scala/blob/v2.13.1/src/library/scala/util/hashing/MurmurHash3.scala#L56-L73
  // to prevent the issue https://github.com/scala/bug/issues/10495
  // TODO(SPARK-30848): Remove this once we drop Scala 2.12.
  private final def productHash(x: Product, seed: Int, ignorePrefix: Boolean = false): Int = {
    val arr = x.productArity
    // Case objects have the hashCode inlined directly into the
    // synthetic hashCode method, but this method should still give
    // a correct result if passed a case object.
    if (arr == 0) {
      x.productPrefix.hashCode
    } else {
      var h = seed
      if (!ignorePrefix) h = scala.util.hashing.MurmurHash3.mix(h, x.productPrefix.hashCode)
      var i = 0
      while (i < arr) {
        h = scala.util.hashing.MurmurHash3.mix(h, x.productElement(i).##)
        i += 1
      }
      scala.util.hashing.MurmurHash3.finalizeHash(h, arr)
    }
  }

  private lazy val _hashCode: Int = productHash(this, scala.util.hashing.MurmurHash3.productSeed)
  override def hashCode(): Int = _hashCode

  /**
   * Faster version of equality which short-circuits when two treeNodes are the same instance.
   * We don't just override Object.equals, as doing so prevents the scala compiler from
   * generating case class `equals` methods
   */
  def fastEquals(other: TreeNode[_]): Boolean = {
    this.eq(other) || this == other
  }

  /**
   * 找到满足f指定条件的第一个树节点。该条件递归应用于此节点及其所有子节点（先序遍历）
   * Find the first [[TreeNode]] that satisfies the condition specified by `f`.
   * The condition is recursively applied to this node and all of its children (pre-order).
   */
  def find(f: BaseType => Boolean): Option[BaseType] = if (f(this)) {
    Some(this)
  } else {
    children.foldLeft(Option.empty[BaseType]) { (l, r) => l.orElse(r.find(f)) }
  }

  /**
   * 将函数 f 递归应用于节点及其子节点
   * Runs the given function on this node and then recursively on [[children]].
   * @param f the function to be applied to each node in the tree.
   */
  def foreach(f: BaseType => Unit): Unit = {
    f(this)
    children.foreach(_.foreach(f))
  }

  /**
   * 先应用于 child 再应用与 parent
   * Runs the given function recursively on [[children]] then on this node.
   * @param f the function to be applied to each node in the tree.
   */
  def foreachUp(f: BaseType => Unit): Unit = {
    children.foreach(_.foreachUp(f))
    f(this)
  }

  /**
   * 返回一个Seq，该Seq包含将给定函数应用于此树中的每个节点的结果，先序遍历。
   * Returns a Seq containing the result of applying the given function to each
   * node in this tree in a preorder traversal.
   * @param f the function to be applied.
   */
  def map[A](f: BaseType => A): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(ret += f(_))
    ret.toSeq
  }

  /**
   * Returns a Seq by applying a function to all nodes in this tree and using the elements of the
   * resulting collections.
   */
  def flatMap[A](f: BaseType => TraversableOnce[A]): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(ret ++= f(_))
    ret.toSeq
  }

  /**
   * Returns a Seq containing the result of applying a partial function to all elements in this
   * tree on which the function is defined.
   */
  def collect[B](pf: PartialFunction[BaseType, B]): Seq[B] = {
    val ret = new collection.mutable.ArrayBuffer[B]()
    val lifted = pf.lift
    foreach(node => lifted(node).foreach(ret.+=))
    ret.toSeq
  }

  /**
   * 以 Seq 的形式返回 tree 的所有叶子节点
   * Returns a Seq containing the leaves in this tree.
   */
  def collectLeaves(): Seq[BaseType] = {
    this.collect { case p if p.children.isEmpty => p }
  }

  /**
   * Finds and returns the first [[TreeNode]] of the tree for which the given partial function
   * is defined (pre-order), and applies the partial function to it.
   */
  def collectFirst[B](pf: PartialFunction[BaseType, B]): Option[B] = {
    val lifted = pf.lift
    lifted(this).orElse {
      children.foldLeft(Option.empty[B]) { (l, r) => l.orElse(r.collectFirst(pf)) }
    }
  }

  /**
   * TreeNode 没有实现 Product 相关方法，都由其子类自行实现
   * Efficient alternative to `productIterator.map(f).toArray`.
   */
  protected def mapProductIterator[B: ClassTag](f: Any => B): Array[B] = {
    val arr = Array.ofDim[B](productArity)
    var i = 0
    while (i < arr.length) {
      arr(i) = f(productElement(i))
      i += 1
    }
    arr
  }

  /**
   * Returns a copy of this node with the children replaced.
   * TODO: Validate somewhere (in debug mode?) that children are ordered correctly.
   */
  def withNewChildren(newChildren: Seq[BaseType]): BaseType = {
    assert(newChildren.size == children.size, "Incorrect number of children")
    var changed = false
    val remainingNewChildren = newChildren.toBuffer
    val remainingOldChildren = children.toBuffer
    def mapTreeNode(node: TreeNode[_]): TreeNode[_] = {
      val newChild = remainingNewChildren.remove(0)
      val oldChild = remainingOldChildren.remove(0)
      if (newChild fastEquals oldChild) {
        oldChild
      } else {
        changed = true
        newChild
      }
    }
    def mapChild(child: Any): Any = child match {
      case arg: TreeNode[_] if containsChild(arg) => mapTreeNode(arg)
      // CaseWhen Case or any tuple type
      case (left, right) => (mapChild(left), mapChild(right))
      case nonChild: AnyRef => nonChild
      case null => null
    }
    val newArgs = mapProductIterator {
      case s: StructType => s // Don't convert struct types to some other type of Seq[StructField]
      // Handle Seq[TreeNode] in TreeNode parameters.
      case s: Stream[_] =>
        // Stream is lazy so we need to force materialization
        s.map(mapChild).force
      case s: Seq[_] =>
        s.map(mapChild)
      case m: Map[_, _] =>
        // `map.mapValues().view.force` return `Map` in Scala 2.12 but return `IndexedSeq` in Scala
        // 2.13, call `toMap` method manually to compatible with Scala 2.12 and Scala 2.13
        // `mapValues` is lazy and we need to force it to materialize
        m.mapValues(mapChild).view.force.toMap
      case arg: TreeNode[_] if containsChild(arg) => mapTreeNode(arg)
      case Some(child) => Some(mapChild(child))
      case nonChild: AnyRef => nonChild
      case null => null
    }

    if (changed) makeCopy(newArgs) else this
  }

  /**
   * 返回此节点的副本，其中规则已递归应用于树。当规则不适用于给定节点时，它保持不变。
   * 用户不应期望特定的方向性。如果需要特定的方向性，则应使用transformDown或transformUp。
   * Returns a copy of this node where `rule` has been recursively applied to the tree.
   * When `rule` does not apply to a given node it is left unchanged.
   * Users should not expect a specific directionality. If a specific directionality is needed,
   * transformDown or transformUp should be used.
   *
   * @param rule the function use to transform this nodes children
   */
  def transform(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    transformDown(rule)
  }

  /**
   * 返回此节点的副本，其中规则已递归应用于该节点及其所有子节点（预排序）。
   * 当规则不适用于给定节点时，它保持不变。
   * Returns a copy of this node where `rule` has been recursively applied to it and all of its
   * children (pre-order). When `rule` does not apply to a given node it is left unchanged.
   *
   * @param rule the function used to transform this nodes children
   */
  def transformDown(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    val afterRule = CurrentOrigin.withOrigin(origin) {
      // 当规则不适用于给定节点时，它保持不变
      rule.applyOrElse(this, identity[BaseType])
    }

    // 返回副本
    // Check if unchanged and then possibly return old copy to avoid gc churn.
    if (this fastEquals afterRule) {
      mapChildren(_.transformDown(rule))
    } else {
      // If the transform function replaces this node with a new one, carry over the tags.
      afterRule.copyTagsFrom(this)
      afterRule.mapChildren(_.transformDown(rule))
    }
  }

  /**
   * 返回此节点的副本，其中规则首先递归应用于其所有子节点，然后递归应用于其自身（后序）。
   * 当规则不适用于给定节点时，它保持不变。
   * Returns a copy of this node where `rule` has been recursively applied first to all of its
   * children and then itself (post-order). When `rule` does not apply to a given node, it is left
   * unchanged.
   *
   * @param rule the function use to transform this nodes children
   */
  def transformUp(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    val afterRuleOnChildren = mapChildren(_.transformUp(rule))
    val newNode = if (this fastEquals afterRuleOnChildren) {
      CurrentOrigin.withOrigin(origin) {
        rule.applyOrElse(this, identity[BaseType])
      }
    } else {
      CurrentOrigin.withOrigin(origin) {
        rule.applyOrElse(afterRuleOnChildren, identity[BaseType])
      }
    }
    // If the transform function replaces this node with a new one, carry over the tags.
    newNode.copyTagsFrom(this)
    newNode
  }

  /**
   * Returns a copy of this node where `f` has been applied to all the nodes in `children`.
   */
  def mapChildren(f: BaseType => BaseType): BaseType = {
    if (containsChild.nonEmpty) {
      mapChildren(f, forceCopy = false)
    } else {
      this
    }
  }

  /**
   * 返回此节点的副本，其中“f”已应用于“children”中的所有节点。
   * 返回 f 应用于所有子节点（非递归，一般将递归操作放在调用该函数的地方）后该节点的 copy。
   * 其内部的原理是调用 mapProductIterator，对每一个 productElement(i) 进行各种模式匹配，若能匹配上某个再根据一定规则进行转换，
   *
   * Returns a copy of this node where `f` has been applied to all the nodes in `children`.
   *
   * @param f The transform function to be applied on applicable `TreeNode` elements.
   * @param forceCopy Whether to force making a copy of the nodes even if no child has been changed.
   */
  private def mapChildren(
    f: BaseType => BaseType,
    forceCopy: Boolean): BaseType = {
    var changed = false

    def mapChild(child: Any): Any = child match {
      case arg: TreeNode[_] if containsChild(arg) =>
        val newChild = f(arg.asInstanceOf[BaseType])
        if (forceCopy || !(newChild fastEquals arg)) {
          changed = true
          newChild
        } else {
          arg
        }
      case tuple @ (arg1: TreeNode[_], arg2: TreeNode[_]) =>
        val newChild1 = if (containsChild(arg1)) {
          f(arg1.asInstanceOf[BaseType])
        } else {
          arg1.asInstanceOf[BaseType]
        }

        val newChild2 = if (containsChild(arg2)) {
          f(arg2.asInstanceOf[BaseType])
        } else {
          arg2.asInstanceOf[BaseType]
        }

        if (forceCopy || !(newChild1 fastEquals arg1) || !(newChild2 fastEquals arg2)) {
          changed = true
          (newChild1, newChild2)
        } else {
          tuple
        }
      case other => other
    }

    val newArgs = mapProductIterator {
      case arg: TreeNode[_] if containsChild(arg) =>
        val newChild = f(arg.asInstanceOf[BaseType])
        if (forceCopy || !(newChild fastEquals arg)) {
          changed = true
          newChild
        } else {
          arg
        }
      case Some(arg: TreeNode[_]) if containsChild(arg) =>
        val newChild = f(arg.asInstanceOf[BaseType])
        if (forceCopy || !(newChild fastEquals arg)) {
          changed = true
          Some(newChild)
        } else {
          Some(arg)
        }
      // `map.mapValues().view.force` return `Map` in Scala 2.12 but return `IndexedSeq` in Scala
      // 2.13, call `toMap` method manually to compatible with Scala 2.12 and Scala 2.13
      case m: Map[_, _] => m.mapValues {
        case arg: TreeNode[_] if containsChild(arg) =>
          val newChild = f(arg.asInstanceOf[BaseType])
          if (forceCopy || !(newChild fastEquals arg)) {
            changed = true
            newChild
          } else {
            arg
          }
        case other => other
      }.view.force.toMap // `mapValues` is lazy and we need to force it to materialize
      case d: DataType => d // Avoid unpacking Structs
      case args: Stream[_] => args.map(mapChild).force // Force materialization on stream
      case args: Iterable[_] => args.map(mapChild)
      case nonChild: AnyRef => nonChild
      case null => null
    }
    if (forceCopy || changed) makeCopy(newArgs, forceCopy) else this
  }

  /**
   * Args to the constructor that should be copied, but not transformed.
   * These are appended to the transformed args automatically by makeCopy
   * @return
   */
  protected def otherCopyArgs: Seq[AnyRef] = Nil

  /**
   * Creates a copy of this type of tree node after a transformation.
   * Must be overridden by child classes that have constructor arguments
   * that are not present in the productIterator.
   * @param newArgs the new product arguments.
   */
  def makeCopy(newArgs: Array[AnyRef]): BaseType = makeCopy(newArgs, allowEmptyArgs = false)

  /**
   * Creates a copy of this type of tree node after a transformation.
   * Must be overridden by child classes that have constructor arguments
   * that are not present in the productIterator.
   * @param newArgs the new product arguments.
   * @param allowEmptyArgs whether to allow argument list to be empty.
   */
  private def makeCopy(
    newArgs: Array[AnyRef],
    allowEmptyArgs: Boolean): BaseType = attachTree(this, "makeCopy") {
    val allCtors = getClass.getConstructors
    if (newArgs.isEmpty && allCtors.isEmpty) {
      // This is a singleton object which doesn't have any constructor. Just return `this` as we
      // can't copy it.
      return this
    }

    // Skip no-arg constructors that are just there for kryo.
    val ctors = allCtors.filter(allowEmptyArgs || _.getParameterTypes.size != 0)
    if (ctors.isEmpty) {
      sys.error(s"No valid constructor for $nodeName")
    }
    val allArgs: Array[AnyRef] = if (otherCopyArgs.isEmpty) {
      newArgs
    } else {
      newArgs ++ otherCopyArgs
    }
    val defaultCtor = ctors.find { ctor =>
      if (ctor.getParameterTypes.length != allArgs.length) {
        false
      } else if (allArgs.contains(null)) {
        // if there is a `null`, we can't figure out the class, therefore we should just fallback
        // to older heuristic
        false
      } else {
        val argsArray: Array[Class[_]] = allArgs.map(_.getClass)
        ClassUtils.isAssignable(argsArray, ctor.getParameterTypes, true /* autoboxing */)
      }
    }.getOrElse(ctors.maxBy(_.getParameterTypes.length)) // fall back to older heuristic

    try {
      CurrentOrigin.withOrigin(origin) {
        val res = defaultCtor.newInstance(allArgs.toArray: _*).asInstanceOf[BaseType]
        res.copyTagsFrom(this)
        res
      }
    } catch {
      case e: java.lang.IllegalArgumentException =>
        throw new TreeNodeException(
          this,
          s"""
             |Failed to copy node.
             |Is otherCopyArgs specified correctly for $nodeName.
             |Exception message: ${e.getMessage}
             |ctor: $defaultCtor?
             |types: ${newArgs.map(_.getClass).mkString(", ")}
             |args: ${newArgs.mkString(", ")}
           """.stripMargin)
    }
  }

  override def clone(): BaseType = {
    mapChildren(_.clone(), forceCopy = true)
  }

  private def simpleClassName: String = Utils.getSimpleName(this.getClass)

  /**
   * Returns the name of this type of TreeNode.  Defaults to the class name.
   * Note that we remove the "Exec" suffix for physical operators here.
   */
  def nodeName: String = simpleClassName.replaceAll("Exec$", "")

  /**
   * The arguments that should be included in the arg string.  Defaults to the `productIterator`.
   */
  protected def stringArgs: Iterator[Any] = productIterator

  private lazy val allChildren: Set[TreeNode[_]] = (children ++ innerChildren).toSet[TreeNode[_]]

  /** Returns a string representing the arguments to this node, minus any children */
  def argString(maxFields: Int): String = stringArgs.flatMap {
    case tn: TreeNode[_] if allChildren.contains(tn) => Nil
    case Some(tn: TreeNode[_]) if allChildren.contains(tn) => Nil
    case Some(tn: TreeNode[_]) => tn.simpleString(maxFields) :: Nil
    case tn: TreeNode[_] => tn.simpleString(maxFields) :: Nil
    case seq: Seq[Any] if seq.toSet.subsetOf(allChildren.asInstanceOf[Set[Any]]) => Nil
    case iter: Iterable[_] if iter.isEmpty => Nil
    case seq: Seq[_] => truncatedString(seq, "[", ", ", "]", maxFields) :: Nil
    case set: Set[_] => truncatedString(set.toSeq, "{", ", ", "}", maxFields) :: Nil
    case array: Array[_] if array.isEmpty => Nil
    case array: Array[_] => truncatedString(array, "[", ", ", "]", maxFields) :: Nil
    case null => Nil
    case None => Nil
    case Some(null) => Nil
    case Some(any) => any :: Nil
    case other => other :: Nil
  }.mkString(", ")

  /**
   * ONE line description of this node.
   * @param maxFields Maximum number of fields that will be converted to strings.
   *                  Any elements beyond the limit will be dropped.
   */
  def simpleString(maxFields: Int): String = s"$nodeName ${argString(maxFields)}".trim

  /**
   * ONE line description of this node containing the node identifier.
   * @return
   */
  def simpleStringWithNodeId(): String

  /** ONE line description of this node with more information */
  def verboseString(maxFields: Int): String

  /** ONE line description of this node with some suffix information */
  def verboseStringWithSuffix(maxFields: Int): String = verboseString(maxFields)

  override def toString: String = treeString

  /** Returns a string representation of the nodes in this tree */
  final def treeString: String = treeString(verbose = true)

  final def treeString(
    verbose: Boolean,
    addSuffix: Boolean = false,
    maxFields: Int = 100,
    printOperatorId: Boolean = false): String = {
    val concat = new PlanStringConcat()
    treeString(concat.append, verbose, addSuffix, maxFields, printOperatorId)
    concat.toString
  }

  def treeString(
    append: String => Unit,
    verbose: Boolean,
    addSuffix: Boolean,
    maxFields: Int,
    printOperatorId: Boolean): Unit = {
    generateTreeString(0, Nil, append, verbose, "", addSuffix, maxFields, printOperatorId, 0)
  }

  /**
   * Returns a string representation of the nodes in this tree, where each operator is numbered.
   * The numbers can be used with [[TreeNode.apply]] to easily access specific subtrees.
   *
   * The numbers are based on depth-first traversal of the tree (with innerChildren traversed first
   * before children).
   */
  def numberedTreeString: String =
    treeString.split("\n").zipWithIndex.map { case (line, i) => f"$i%02d $line" }.mkString("\n")

  /**
   * Returns the tree node at the specified number, used primarily for interactive debugging.
   * Numbers for each node can be found in the [[numberedTreeString]].
   *
   * Note that this cannot return BaseType because logical plan's plan node might return
   * physical plan for innerChildren, e.g. in-memory relation logical plan node has a reference
   * to the physical plan node it is referencing.
   */
  def apply(number: Int): TreeNode[_] = getNodeNumbered(new MutableInt(number)).orNull

  /**
   * Returns the tree node at the specified number, used primarily for interactive debugging.
   * Numbers for each node can be found in the [[numberedTreeString]].
   *
   * This is a variant of [[apply]] that returns the node as BaseType (if the type matches).
   */
  def p(number: Int): BaseType = apply(number).asInstanceOf[BaseType]

  private def getNodeNumbered(number: MutableInt): Option[TreeNode[_]] = {
    if (number.i < 0) {
      None
    } else if (number.i == 0) {
      Some(this)
    } else {
      number.i -= 1
      // Note that this traversal order must be the same as numberedTreeString.
      innerChildren.map(_.getNodeNumbered(number)).find(_ != None).getOrElse {
        children.map(_.getNodeNumbered(number)).find(_ != None).flatten
      }
    }
  }

  /**
   * All the nodes that should be shown as a inner nested tree of this node.
   * For example, this can be used to show sub-queries.
   */
  def innerChildren: Seq[TreeNode[_]] = Seq.empty

  /**
   * Appends the string representation of this node and its children to the given Writer.
   *
   * The `i`-th element in `lastChildren` indicates whether the ancestor of the current node at
   * depth `i + 1` is the last child of its own parent node.  The depth of the root node is 0, and
   * `lastChildren` for the root node should be empty.
   *
   * Note that this traversal (numbering) order must be the same as [[getNodeNumbered]].
   */
  def generateTreeString(
    depth: Int,
    lastChildren: Seq[Boolean],
    append: String => Unit,
    verbose: Boolean,
    prefix: String = "",
    addSuffix: Boolean = false,
    maxFields: Int,
    printNodeId: Boolean,
    indent: Int = 0): Unit = {
    append("   " * indent)
    if (depth > 0) {
      lastChildren.init.foreach { isLast =>
        append(if (isLast) "   " else ":  ")
      }
      append(if (lastChildren.last) "+- " else ":- ")
    }

    val str = if (verbose) {
      if (addSuffix) verboseStringWithSuffix(maxFields) else verboseString(maxFields)
    } else {
      if (printNodeId) {
        simpleStringWithNodeId()
      } else {
        simpleString(maxFields)
      }
    }
    append(prefix)
    append(str)
    append("\n")

    if (innerChildren.nonEmpty) {
      innerChildren.init.foreach(_.generateTreeString(
        depth + 2, lastChildren :+ children.isEmpty :+ false, append, verbose,
        addSuffix = addSuffix, maxFields = maxFields, printNodeId = printNodeId, indent = indent))
      innerChildren.last.generateTreeString(
        depth + 2, lastChildren :+ children.isEmpty :+ true, append, verbose,
        addSuffix = addSuffix, maxFields = maxFields, printNodeId = printNodeId, indent = indent)
    }

    if (children.nonEmpty) {
      children.init.foreach(_.generateTreeString(
        depth + 1, lastChildren :+ false, append, verbose, prefix, addSuffix,
        maxFields, printNodeId = printNodeId, indent = indent)
      )
      children.last.generateTreeString(
        depth + 1, lastChildren :+ true, append, verbose, prefix,
        addSuffix, maxFields, printNodeId = printNodeId, indent = indent)
    }
  }

  /**
   * Returns a 'scala code' representation of this `TreeNode` and its children.  Intended for use
   * when debugging where the prettier toString function is obfuscating the actual structure. In the
   * case of 'pure' `TreeNodes` that only contain primitives and other TreeNodes, the result can be
   * pasted in the REPL to build an equivalent Tree.
   */
  def asCode: String = {
    val args = productIterator.map {
      case tn: TreeNode[_] => tn.asCode
      case s: String => "\"" + s + "\""
      case other => other.toString
    }
    s"$nodeName(${args.mkString(",")})"
  }

}