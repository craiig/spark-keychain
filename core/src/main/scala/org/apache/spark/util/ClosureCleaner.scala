/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream, DataOutputStream}
import java.io.{ObjectStreamClass}
import java.io.{IOException}
import sun.misc.Unsafe
import java.lang.reflect.Modifier
import java.lang.reflect.{Array => ReflectArray};
import java.nio.ByteBuffer;

import scala.collection.mutable.{Map, Set, SortedSet, Queue, Stack, ListBuffer, HashMap}
import scala.language.existentials
import scala.collection.JavaConverters._

import org.objectweb.asm.{ClassReader, ClassVisitor, MethodVisitor, Type, Label, Handle}
import org.objectweb.asm.tree.{ClassNode, MethodNode, InsnList, AbstractInsnNode}
import org.objectweb.asm.util.{TraceMethodVisitor, Textifier}
import org.objectweb.asm.Opcodes._

//for function serialization & encoding
import java.security.MessageDigest
import java.util.regex.Pattern
import org.apache.spark.SparkContext
import org.apache.commons.codec.binary.Base64

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.internal.Logging

import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{write => WriteJson}

/**
 * A cleaner that renders closures serializable if they can be done so safely.
 */
private[spark] object ClosureCleaner extends Logging {

  // Get an ASM class reader for a given class from the JAR that loaded it
  private[util] def getClassReader(cls: Class[_]): ClassReader = {
    // Copy data over, before delegating to ClassReader - else we can run out of open file handles.
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    val resourceStream = cls.getResourceAsStream(className)
    // todo: Fixme - continuing with earlier behavior ...
    if (resourceStream == null) return new ClassReader(resourceStream)

    val baos = new ByteArrayOutputStream(128)
    Utils.copyStream(resourceStream, baos, true)
    new ClassReader(new ByteArrayInputStream(baos.toByteArray))
  }

  // Check whether a class represents a Scala closure
  private def isClosure(cls: Class[_]): Boolean = {
    cls.getName.contains("$anonfun$")
  }

  // Get a list of the outer objects and their classes of a given closure object, obj;
  // the outer objects are defined as any closures that obj is nested within, plus
  // possibly the class that the outermost closure is in, if any. We stop searching
  // for outer objects beyond that because cloning the user's object is probably
  // not a good idea (whereas we can clone closure objects just fine since we
  // understand how all their fields are used).
  private def getOuterClassesAndObjects(obj: AnyRef): (List[Class[_]], List[AnyRef]) = {
    for (f <- obj.getClass.getDeclaredFields if f.getName == "$outer") {
      f.setAccessible(true)
      val outer = f.get(obj)
      // The outer pointer may be null if we have cleaned this closure before
      if (outer != null) {
        if (isClosure(f.getType)) {
          val recurRet = getOuterClassesAndObjects(outer)
          return (f.getType :: recurRet._1, outer :: recurRet._2)
        } else {
          return (f.getType :: Nil, outer :: Nil) // Stop at the first $outer that is not a closure
        }
      }
    }
    (Nil, Nil)
  }
  /* returns same as above but not filtered for outers only */
  private def getAllClassesAndObjects(obj: AnyRef): (List[Class[_]], List[AnyRef]) = {
    for (f <- obj.getClass.getDeclaredFields) {
      f.setAccessible(true)
      val outer = f.get(obj)
      // The outer pointer may be null if we have cleaned this closure before
      if (outer != null) {
        if (isClosure(f.getType)) {
          val recurRet = getAllClassesAndObjects(outer)
          return (f.getType :: recurRet._1, outer :: recurRet._2)
        } else {
          return (f.getType :: Nil, outer :: Nil) // Stop at the first $outer that is not a closure
        }
      }
    }
    (Nil, Nil)
  }
  /**
   * Return a list of classes that represent closures enclosed in the given closure object.
   */
  private def getInnerClosureClasses(obj: AnyRef): List[Class[_]] = {
    val seen = Set[Class[_]](obj.getClass)
    val stack = Stack[Class[_]](obj.getClass)
    while (!stack.isEmpty) {
      val cr = getClassReader(stack.pop())
      val set = Set[Class[_]]()
      cr.accept(new InnerClosureFinder(set), 0)
      for (cls <- set -- seen) {
        seen += cls
        stack.push(cls)
      }
    }
    (seen - obj.getClass).toList
  }

  /* returns a map of (class to (funcname, funcsig) ) of all functions called by the given class, recursively */
  type Class2Func = (String,String,String)
  private def getFunctionsCalled(obj: AnyRef): Set[Class2Func] = {
    var cls = obj.getClass

    /* read first set */
    val seen = SortedSet[Class2Func]()
    getClassReader(obj.getClass).accept(new CalledFunctionsFinder(seen), 0)

    /* add all called functions to visit stack */
    var stack = SortedSet[Class2Func]()
    getClassReader(obj.getClass).accept(new CalledFunctionsFinder(stack), 0)

    /* visit all other functions */
    while(!stack.isEmpty){
      val tup = stack.head
      stack = stack.tail
      val clazz = Class.forName(tup._1.replace('/', '.'), false, Thread.currentThread.getContextClassLoader)
      val cr = getClassReader(clazz)

      val set = SortedSet[Class2Func]()
      val sigtup = (tup._2, tup._3)
      cr.accept(new CalledFunctionsFinder(set, Some(Set(sigtup))), 0)

      for (newtups <- set -- seen){
        seen += newtups
        stack += newtups
      }
    }
    logDebug(s"Found ${seen.size} functions called by ${obj.getClass.getName}")

    seen
  }

  /* hash a given classes bytecode, optionally only the given functions,
   * will anonymize certain names to avoid false negatives due to:
   *  REPL line numbers
   *  names of called functions (redundant when the bytecode is included instead)
   *  outer classes of called functions
   * @param obj the object to hash (class or lambda)
   * @param hashObj the digest to add bytecode to
   * @param hashTrace map object that is used to store debug information about the hash procedure
   * @param funcList list of functions to hash of the class (if empty, all functions will be hashed)
   * */
  type HashTraceMap = HashMap[String,Any]
  private def hashClass(obj: AnyRef,
    hashObj: MessageDigest,
    hashTrace: HashTraceMap,
    funcList: Set[(String,String)] = Set()
    ): Unit  = {
     val anonymizeClass = true; //always anonymize the class

     /* build classnode */
     val cn = new ClassNode()
     //we might get passed a function of a class, or a class itself, so we disambiguate
     val objcls:Class[_] = if (obj.isInstanceOf[Class[_]]){
       obj.asInstanceOf[Class[_]]
     } else {
       obj.getClass
     }
     getClassReader(objcls).accept(cn, 0)

     /* build debug trace */
     var localHash = MessageDigest.getInstance(preferredHashType)
     hashTrace += (
       ("name" -> cn.name), 
       ("outerClass" -> cn.outerClass),
       ("outerMethod" -> cn.outerMethod),
       ("outerMethodDesc" -> cn.outerMethodDesc),
       ("signature" -> cn.signature),
       ("superName" -> cn.superName)
     )
     
     for( f <- objcls.getDeclaredFields ){
       if (f.getName == "serialVersionUID"){
         try {
           hashTrace += ("serialVersionUID" -> f.getLong(null).toString)
         } catch {
           case e: Exception => hashTrace += ("serialVersionUID" -> null)
         }
       }
     }

     /* read class using a visitor we can override
      * make sure to rewrite any non-identifying symbols that could cause
      * differences in the hash but aren't functionally relevant, such as:
      *  - Scala REPL adds 'lineXX' package for each lambda
      * for now we only want to support very simple lambdas, so we only rewrite
      * references to their current package to a non-REPL form, but keep any references
      * to other potentially REPL functions, but we also want to avoid ambiguity
      */

     //create a regex that replaces the original name with the munged
     val re_line = (s:String) => {
       if(anonymizeClass){
         val quoted_name = "\\$line\\d+".r
         var ret = quoted_name.replaceAllIn(s, "\\$lineXX")
         ret
       } else {
         s
       }
     }
     val re_name = (s:String) => {
       if(anonymizeClass && cn.name != null){
         val quoted_name = Pattern.quote(cn.name).r
         quoted_name.replaceAllIn(re_line(s), "THISCLASS")
       } else {
         s
       }
     }
     val re_outer = (s:String) => {
       if(anonymizeClass && cn.outerClass != null){
         val quoted_outer = Pattern.quote(cn.outerClass).r
         quoted_outer.replaceAllIn(re_line(s), "THISOUTER")
       } else {
         s
       }
     }

     // A methodvisitor to rename strings in the bytecode output
     val p = new Textifier(ASM5) {
       override def visitLocalVariable(name:String, desc:String,
         signature:String, start:Label, end:Label, index:Int){
           var newdesc = re_outer(re_name(desc))
           super.visitLocalVariable(name, newdesc, signature,
             start, end, index)
       }
       /* deprecated by ASM5, kept here to ensure renaming works */
      override def visitMethodInsn(opcode:Int, owner:String,
        name:String, desc:String){
          var newname = re_name(name)
          var newowner = re_name(owner)
          var newdesc = re_outer(re_name(desc))
          super.visitMethodInsn(opcode, newowner, newname, newdesc);
        }
       override def visitMethodInsn(opcode:Int, owner:String,
         name:String, desc:String, itf:Boolean){
          var newname = re_name(name)
          var newowner = re_name(owner)
          var newdesc = re_outer(re_name(desc))

          if(newowner.startsWith("$line")){
            newname = """VAL\d+""".r.replaceAllIn(newname, "VALXX")
          }

          //call super to print method
          super.visitMethodInsn(opcode, newowner, newname, newdesc, itf);
       }
       override def visitLineNumber(line:Int, start:Label){
          //this is a no-op to suppress line number output
       }
       override def visitFieldInsn( opcode:Int, owner:String,
         name:String, desc:String){
           /* rename references to fields */
           val newowner = re_name(owner)
           val newdesc = re_name(desc)
           var newname = re_name(name)
           if (opcode == GETFIELD && owner.startsWith("$line")) {
             newname = """VAL\d+""".r.replaceAllIn(newname, "VALXX")
           }
           super.visitFieldInsn(opcode, newowner, newname, newdesc)
       }
     }
     val tm = new TraceMethodVisitor(p)

     var bytecode_string = new StringBuilder

     val methods = cn.methods.asInstanceOf[java.util.ArrayList[MethodNode]].asScala
     val filter = funcList.size > 0
     for(m:MethodNode <- methods){
       //println(s"checking if ${(m.name,m.desc)} in ${funcList}: ${ funcList contains (m.name,m.desc) }");
       if( !filter || (funcList contains (m.name,m.desc)) ){
         val newdesc = re_outer(re_name(m.desc))
         var newname = re_name(m.name)
         if (newdesc.startsWith("()L$line")) {
           newname = """VAL\d+""".r.replaceAllIn(newname, "VALXX")
         }
         var methodHeader = s"${newname} ${newdesc} ${m.signature}"

         bytecode_string.append(methodHeader)
         hashObj.update(methodHeader.getBytes)
         localHash.update(methodHeader.getBytes)

         m.accept(tm) //visit method

         //read text from method visitor
         for(o <- p.getText.asScala){
           var s = o.toString()
           bytecode_string.append(s)
           hashObj.update(s.getBytes)
           localHash.update(methodHeader.getBytes)
         }
         p.getText.clear
       }
     }

    hashTrace += ("bytecode" -> bytecode_string.toString)
    hashTrace += ("localHash" -> Base64.encodeBase64URLSafeString(localHash.digest))

    //recurse into callees of the given function
  }

  /* Hash all referenced fields of an object, ignoring everything except bytes
   * of primitives, making Array[Int,Int] = (Int,Int). Alone this might
   * introduce ambiguity but when combined with the bytecode hash there is no
   * ambiguity.
   *
   * use java reflection and sun.misc.unsafe to introspect the memory directly
   * copy the serialization code and walk the objects ourselves
   * this will only serialize non-transitive, non-static primitive values
   *
   * this routine assumes the object has been passed through the clean method
   * so there are no irrelevant fields to hash that remain on the object
   *
   * @param func the function to walk to hash the primitives
   * @param dos the stream where the bytes will be written to
   * @param visited a set of previously visited objects, to avoid duplicating work (used for recursion)
   * @param clz class 
   */
  def hashInputPrimitives(func: AnyRef,
    dos: DataOutputStream,
    hashTrace:HashTraceMap,
    visited:Set[Object] = Set[Object](),
    clz:Class[_] = null
  ):Unit = {
    /* get unsafe handle */
    //var unsafe = Unsafe.getUnsafe();
    val field = classOf[Unsafe].getDeclaredField("theUnsafe");
    field.setAccessible(true);
    val unsafe = field.get(null).asInstanceOf[Unsafe];
    
    //avoid loops
    if(visited contains func){
      logTrace(s"returning early, ${func} already visited");
      return;
    }
    visited += func;

    /* queue of objects to visit */
    val toVisit = Queue[AnyRef]() //Queue is ordered?

    var cl = clz;
    if(cl == null){
      cl = func.getClass()
    }
    logTrace(s"hashInputPrimitives: class: ${cl.getName}")
    /* trace enough so we can attribute differences in hashing to a FIELD */
    //var hashTrace:HashTraceMap = Map(
    hashTrace += (
      "class"->cl.getName, 
      "fields"->ListBuffer[HashTraceMap](),
      "children"->ListBuffer[HashTraceMap]()
    )

    for( f <- cl.getDeclaredFields ){
      f.setAccessible(true)
      val transient = Modifier.isTransient(f.getModifiers)
      val static = Modifier.isStatic(f.getModifiers)
      val fldtype = f.getType()
      val primitive = fldtype.isPrimitive()

      logTrace(s"hashInputPrimitives:\tfield ${f.getName} type: ${fldtype.getName}")
      logTrace(s"hashInputPrimitives:\t\tstatic: ${static} primitive: ${primitive} type: ${fldtype.getName} transient: ${transient}")

      var fieldTrace:HashTraceMap = HashMap( "name"->f.getName, "type"->fldtype.getName,
        "static"->static, "primitive"->primitive, "transient"->transient )

      val fieldBytesTrace = ListBuffer[String]()

      val writeBytes = (output:DataOutputStream, thing:AnyRef, offset:Long, size:Integer) => {
        for(i <- 0 until size){
             val b = unsafe.getByte(thing.asInstanceOf[Object], offset+i)
             output.writeByte( b )
             fieldBytesTrace += b.toString
             logTrace(s"hashInputPrimitives:\t\twriteBytes up to: ${output.size}")
        }
      }

      if( !( static || transient ) ){
        /* only read fields that are not static and not transient */
        val offset = unsafe.objectFieldOffset( f )
        //if primitive, grab value
        if(primitive && !fldtype.isArray()){
          logTrace(s"hashInputPrimitives:\t\tadding to hash (primitive)")
          fieldTrace += ("hashedAs"->"primitive")
          if( fldtype == classOf[Byte] ){
            //dos.writeByte( unsafe.getByte(func.asInstanceOf[Object], offset) )
            writeBytes(dos, func, offset, 1)
          } else if( fldtype == classOf[Char] ){
            //dos.writeChar( unsafe.getChar(func.asInstanceOf[Object], offset) )
            writeBytes(dos, func, offset, 2)
          } else if( fldtype == classOf[Int] ){
            //dos.writeInt( unsafe.getInt(func.asInstanceOf[Object], offset) )
            //dos.writeByte( unsafe.getByte(func.asInstanceOf[Object], offset) )
            //dos.writeByte( unsafe.getByte(func.asInstanceOf[Object], offset+1) )
            //dos.writeByte( unsafe.getByte(func.asInstanceOf[Object], offset+2) )
            //dos.writeByte( unsafe.getByte(func.asInstanceOf[Object], offset+3) )
            writeBytes(dos, func, offset, 4)
          } else if( fldtype == classOf[Long] ){
            //dos.writeLong( unsafe.getLong(func.asInstanceOf[Object], offset) )
            writeBytes(dos, func, offset, 8)
          } else if( fldtype == classOf[Float] ){
            //dos.writeFloat( unsafe.getFloat(func.asInstanceOf[Object], offset) )
            writeBytes(dos, func, offset, 4)
          } else if( fldtype == classOf[Double] ){
            //dos.writeDouble( unsafe.getDouble(func.asInstanceOf[Object], offset) )
            writeBytes(dos, func, offset, 8)
          } else if( fldtype == classOf[Boolean] ){
            //dos.writeBoolean( unsafe.getBoolean(func.asInstanceOf[Object], offset) )
            writeBytes(dos, func, offset, 1)
          } else {
            logError("hashInputPrimitives: Error could not determine primitive type")
          }
        }
        else if(fldtype.isArray()){
          val Value:Array[_] = f.get(func).asInstanceOf[Array[_]]
          if(Value != null){
            val length = ReflectArray.getLength(Value)
            val baseOffset = unsafe.arrayBaseOffset( fldtype );
            val indexScale = unsafe.arrayIndexScale( fldtype );
            logTrace(s"hashInputPrimitives:\tfield is array, value:${Value.mkString(",")} length: ${length} baseOffset: ${baseOffset} indexScale: ${indexScale}");
            logTrace(s"hashInputPrimitives:\t\tadding to hash (array)")
            fieldTrace += ("hashedAs"->"array", "length"->length, "baseOffset"->baseOffset,
              "indexScale"->indexScale, "value"->Value.mkString)

            // if this is an array of objects (not primitives),
            // make sure we visit each one
            if(Value.isInstanceOf[Array[Object]]){
              logTrace(s"hashInputPrimitives:\t\twriting objects")
              fieldTrace += ("elementsHashedAs"->"objects")
              for(p <- Value.asInstanceOf[Array[Object]]){
                if(p != null){
                  toVisit += p
                }
              }
            } else {
              logTrace(s"hashInputPrimitives:\t\twriting raw bytes")
              fieldTrace += ("elementsHashedAs"->"bytes")
              //primitive arrays get visited directly??
              //read the bytes of the array so that we don't have to infer type
              // this has a problem in that reading bytes gives different byte ordering
              // than reading int, double, etc, due to endianness
              //val byteArray:Array[Byte] = new Array[Byte](indexScale*length);
              for(i <- 0 to (indexScale*length-1) ){
                //byteArray(i) = unsafe.getByte( Value, baseOffset + i );
                val b = unsafe.getByte( Value, baseOffset + i )
                dos.writeByte( b )
                fieldBytesTrace += b.toString
              }
            }
          }
        } else {
          //if not primitive, recurse into and find it's primitives
          // we don't care about the value of the reference really
          // or the type, as this is encoded in the bytecode hash! :D
          val p = f.get(func)
          if(p != null){
            logTrace(s"hashInputPrimitives:\t\tadding to visit list")
            fieldTrace += ("hashedAs"->"visited")
            toVisit += p
          }
        }
      } else {
        //trace non visited fields
        fieldTrace += ("hashedAs" -> "skipped")
      }

      //add field trace to the hash trace
      fieldTrace += ("hashed_bytes" -> fieldBytesTrace.mkString(","))
      hashTrace("fields").asInstanceOf[ListBuffer[HashTraceMap]] += fieldTrace
    }

    logTrace(s"visit list: ${toVisit.mkString(",")}")
    for( p <- toVisit ){
      logTrace(s"visiting ${p}")
      val childTrace = new HashTraceMap
      hashInputPrimitives(p, dos, childTrace, visited)
      hashTrace("children").asInstanceOf[ListBuffer[HashTraceMap]] += childTrace
    }

    //TODO support externalizable objects?

    //follow the parent class desc
    if( cl.getSuperclass() != null ){
      val childTrace = new HashTraceMap
      hashInputPrimitives(func, dos, childTrace, visited, cl.getSuperclass() )
      hashTrace += ("superclass"->childTrace)
    }
  }

  /* serialize the entire function to bytes, and fix/blank out
   * any fields that we don't need, then return the bytes for hashing
   * we just serialize the top-level class which should capture everything
   * required 
   *
   * this function is very deprecated
   * */
  def hashSerialization(func:AnyRef): Array[Byte] = {
    var serialize_func = (f:AnyRef) => {
      val bos = new ByteArrayOutputStream()
      val out = new ObjectOutputStream(bos)
      out.writeObject(func)
      out.close()
      //logTrace(s"serializationHash: unmangled serialized func byte array:\n${bos.toByteArray.mkString(",")}")
      new String(bos.toByteArray, "ISO-8859-1") //iso 8859-1 is 1-1 character to byte
    }
    /* fix up the serialization to avoid some non-determinism from the REPL */
    val re_serialized = (s:String) => {
     /* serialversionUID */
     // python re.sub(r'sr..lineXX.\$read(\$\$iwC)*........\x02', 'sr\x00\x09.REPLCLASS\x02', s)
     val all_res = "res\\d+".r.replaceAllIn(s, "resXX")
     val all_vals = "VAL\\d+".r.replaceAllIn(all_res, "valXX")
     val lineXX = "\\$line\\d+".r.replaceAllIn(all_vals, "lineXX")
     /* replace the serialversion ID with a dummy value
      * (?s) needed to get the dot matching newlines, etc.
      * */
     val replclass = "(?s)sr..lineXX.\\$read(\\$\\$iwC)*........\\x02\\x00".r.replaceAllIn(lineXX, "sr..REPLCLASS\\x02\\x00")

     /* replace the RDD ID with a dummy value
      * lots of context is included here, up to the start of the RDD id serialization, so we're sure we got it right
      * as time goes on we may need to refine this so that we don't miss a case
      * we need to mask the rddid off, because all parents have a numerical rdd, even if they also have RDDUnique
      * */
     val rddid = "r\\x00\\x18org.apache.spark.rdd.RDD........\\x02\\x00\\x05I\\x00\\x02idL\\x00\\x0echeckpointDatat\\x00\\x0eLscala/Option;L\\x00'org\\$apache\\$spark\\$rdd\\$RDD\\$\\$dependencies_t\\x00\\x16Lscala/collection/Seq;L\\x00\\$org\\$apache\\$spark\\$rdd\\$RDD\\$\\$evidence\\$1q\\x00~\\x00.L\\x00\\x0cstorageLevelt\\x00'Lorg/apache/spark/storage/StorageLevel;xp....s"
     .r.replaceAllIn(replclass, "RDDIDMASK")
     rddid
    }

    //testing that serialization is deterministic
    val serial1 = re_serialized(serialize_func(func));
    //logTrace(s"serializationHash: mangled serialized func byte array:\n${serial1.getBytes("ISO-8859-1").mkString(",")}")
    serial1.getBytes("ISO-8859-1")
  }

  /** Hash the given closure.
   * This hashes only the functional pieces of the closure
   *  - function signature
   *  - instructions
   * so that it's functionality can be summarized as a hash, used for
   * comparison between different spark contexts that may be in use
   */
  val preferredHashType = "SHA-256"
  var hashCache: Map[String, Array[Byte]] = Map.empty
  def hash(func: AnyRef): Option[String] = {
    try {
      hash_internal(func)
    } catch {
      /* IOException happens when a class we need to hash can't be found.  We
       * need to access the class but can't, so we can't produce a valid hash,
       * so just return None.  Encountered when SparkContext has been stopped
       * in testing */
      case e: IOException => {
        logInfo(s"HLS Hash failed: ${e}")
        return None
      }
    }
  }
  def hash_internal(func: AnyRef): Option[String] = {
    var hashStart = System.currentTimeMillis

    if (!isClosure(func.getClass)) {
      logInfo("Expected a closure; got " + func.getClass.getName)
      return None
    }
    if (func == null) {
      return None
    }
    logDebug(s"+++ Hashing closure $func (${func.getClass.getName}) +++")

    /* first check to see if we've already hash the given ref */
    /* TODO potentially implement cache that stores callees seperately */
    var bytecodeHashTraces = ListBuffer[HashTraceMap]()

    var bytecodeHashbytes:Array[Byte] = hashCache get func.getClass.getName match {
      case Some(result) => {
        logInfo("Hash Cache hit for: " + func.getClass.getName)
        bytecodeHashTraces += HashMap(
          "hashCacheHit"->true, 
          "name"-> func.getClass.getName, 
          "localHash" -> Base64.encodeBase64URLSafeString(result)
        )
        result
      }
      case None => {
          logInfo("Hash Cache miss for: " + func.getClass.getName)

          var classesToVisit = getFunctionsCalled(func)
          logDebug(s"+++ All classes to visit: $classesToVisit")

          /* hash the given function */
          var hash = MessageDigest.getInstance(preferredHashType)
          var bytecodeHashTrace = new HashTraceMap
          hashClass(func, hash, bytecodeHashTrace) 
          bytecodeHashTraces += bytecodeHashTrace

          /* hash the bytecode of all functions that this function calls */
          /* TODO rewrite this so we only call it once per class */
          for( cls <- classesToVisit ){
            var clzname = cls._1.replace('/', '.')
            var obj = Class.forName( clzname,
            false, Thread.currentThread.getContextClassLoader)
            logDebug(s"+++ hashing $clzname functions: ${ (cls._2, cls._3) }")

            var bytecodeHashTrace:HashTraceMap = new HashTraceMap
            hashClass(obj, hash, bytecodeHashTrace, Set((cls._2, cls._3)) )
            bytecodeHashTraces += bytecodeHashTrace
          }
          val digest = hash.digest
          hashCache(func.getClass.getName) = digest
          digest
      }
    }
    var hashBytecodeStop = System.currentTimeMillis

    // Now we want to serialize & hash any referenced fields
    /* disable serialization hashing for now */
    /* 
    val serializationBytes = hashSerialization(func);
    var serial_hash = MessageDigest.getInstance("SHA-256")
    serial_hash.update(serializationBytes)
    var serial_hash64 = Base64.encodeBase64URLSafeString(serial_hash.digest)
    logInfo(s"Serialization hash: $serial_hash64")
    */

    //hash the primitives
    var hashPrimitivesStart = System.currentTimeMillis
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)
    logTrace("Hash input primitives start for: " + func.getClass.getName)
    val primitiveHashTrace = new HashTraceMap
    hashInputPrimitives( func, dos, primitiveHashTrace )
    dos.close()
    logTrace(s"Primitive Bytes: ${ bos.toByteArray.mkString(",") } ")

    var primitive_hash64 = if( bos.toByteArray.length > 0){
      var primitiveHash = MessageDigest.getInstance("SHA-256")
      primitiveHash.update(bos.toByteArray)
      Base64.encodeBase64URLSafeString(primitiveHash.digest)
    } else {
      "none"
    }
    logInfo(s"Primitive hash: $primitive_hash64")

    //finalize the hash
    var hashbytesenc = Base64.encodeBase64URLSafeString(bytecodeHashbytes)
    logInfo(s"Bytecode hash: $hashbytesenc")
    //Some(hashbytesenc)

    //merge bytecode and primitive hash
    var merged = s"bc:${hashbytesenc}_pr:${primitive_hash64}"
    logInfo(s"Merged hash: ${merged}")

    var hashStop = System.currentTimeMillis
    logInfo(s"Hashing took: ${hashStop - hashStart} ms closure:${func.getClass.getName}")
    logInfo(s"Hashing Bytecode took: ${hashBytecodeStop - hashStart} ms closure:${func.getClass.getName}")
    logInfo(s"Hashing Primitives took: ${hashStop - hashPrimitivesStart} ms closure:${func.getClass.getName}")

    var overallHashTrace = Map(
      "closureName" -> func.getClass.getName,
      "bytecode"-> Map(
        "trace"->bytecodeHashTraces,
        "hash"->hashbytesenc,
        "took_ms"->(hashBytecodeStop - hashStart)
      ),
      "primitives"-> Map(
        "trace"->primitiveHashTrace,
        "hash"->primitive_hash64,
        "hashed_bytes"->bos.toByteArray.mkString(","),
        "took_ms"->(hashStop - hashPrimitivesStart)
      ),
      "mergedHash"->merged,
      "took_ms"->(hashStop - hashStart)
    )

    implicit val jsonformats = Serialization.formats(NoTypeHints)
    logInfo(s"Hashing trace: ${WriteJson(overallHashTrace)}")

    Some(merged)
  }

  /**
   * Clean the given closure in place.
   *
   * More specifically, this renders the given closure serializable as long as it does not
   * explicitly reference unserializable objects.
   *
   * @param closure the closure to clean
   * @param checkSerializable whether to verify that the closure is serializable after cleaning
   * @param cleanTransitively whether to clean enclosing closures transitively
   */
  def clean(
      closure: AnyRef,
      checkSerializable: Boolean = true,
      cleanTransitively: Boolean = true): Unit = {
    clean(closure, checkSerializable, cleanTransitively, Map.empty)
  }

  /**
   * Helper method to clean the given closure in place.
   *
   * The mechanism is to traverse the hierarchy of enclosing closures and null out any
   * references along the way that are not actually used by the starting closure, but are
   * nevertheless included in the compiled anonymous classes. Note that it is unsafe to
   * simply mutate the enclosing closures in place, as other code paths may depend on them.
   * Instead, we clone each enclosing closure and set the parent pointers accordingly.
   *
   * By default, closures are cleaned transitively. This means we detect whether enclosing
   * objects are actually referenced by the starting one, either directly or transitively,
   * and, if not, sever these closures from the hierarchy. In other words, in addition to
   * nulling out unused field references, we also null out any parent pointers that refer
   * to enclosing objects not actually needed by the starting closure. We determine
   * transitivity by tracing through the tree of all methods ultimately invoked by the
   * inner closure and record all the fields referenced in the process.
   *
   * For instance, transitive cleaning is necessary in the following scenario:
   *
   *   class SomethingNotSerializable {
   *     def someValue = 1
   *     def scope(name: String)(body: => Unit) = body
   *     def someMethod(): Unit = scope("one") {
   *       def x = someValue
   *       def y = 2
   *       scope("two") { println(y + 1) }
   *     }
   *   }
   *
   * In this example, scope "two" is not serializable because it references scope "one", which
   * references SomethingNotSerializable. Note that, however, the body of scope "two" does not
   * actually depend on SomethingNotSerializable. This means we can safely null out the parent
   * pointer of a cloned scope "one" and set it the parent of scope "two", such that scope "two"
   * no longer references SomethingNotSerializable transitively.
   *
   * @param func the starting closure to clean
   * @param checkSerializable whether to verify that the closure is serializable after cleaning
   * @param cleanTransitively whether to clean enclosing closures transitively
   * @param accessedFields a map from a class to a set of its fields that are accessed by
   *                       the starting closure
   */
  private def clean(
      func: AnyRef,
      checkSerializable: Boolean,
      cleanTransitively: Boolean,
      accessedFields: Map[Class[_], Set[String]]): Unit = {

    if (!isClosure(func.getClass)) {
      logWarning("Expected a closure; got " + func.getClass.getName)
      return
    }

    // TODO: clean all inner closures first. This requires us to find the inner objects.
    // TODO: cache outerClasses / innerClasses / accessedFields

    if (func == null) {
      return
    }

    logDebug(s"+++ Cleaning closure $func (${func.getClass.getName}) +++")

    // A list of classes that represents closures enclosed in the given one
    val innerClasses = getInnerClosureClasses(func)

    // A list of enclosing objects and their respective classes, from innermost to outermost
    // An outer object at a given index is of type outer class at the same index
    val (outerClasses, outerObjects) = getOuterClassesAndObjects(func)

    // For logging purposes only
    val declaredFields = func.getClass.getDeclaredFields
    val declaredMethods = func.getClass.getDeclaredMethods

    logDebug(" + declared fields: " + declaredFields.size)
    declaredFields.foreach { f => logDebug("     " + f) }
    logDebug(" + declared methods: " + declaredMethods.size)
    declaredMethods.foreach { m => logDebug("     " + m) }
    logDebug(" + inner classes: " + innerClasses.size)
    innerClasses.foreach { c => logDebug("     " + c.getName) }
    logDebug(" + outer classes: " + outerClasses.size)
    outerClasses.foreach { c => logDebug("     " + c.getName) }
    logDebug(" + outer objects: " + outerObjects.size)
    outerObjects.foreach { o => logDebug("     " + o) }

    // Fail fast if we detect return statements in closures
    getClassReader(func.getClass).accept(new ReturnStatementFinder(), 0)

    // If accessed fields is not populated yet, we assume that
    // the closure we are trying to clean is the starting one
    if (accessedFields.isEmpty) {
      logDebug(s" + populating accessed fields because this is the starting closure")
      // Initialize accessed fields with the outer classes first
      // This step is needed to associate the fields to the correct classes later
      for (cls <- outerClasses) {
        accessedFields(cls) = Set[String]()
      }
      // Populate accessed fields by visiting all fields and methods accessed by this and
      // all of its inner closures. If transitive cleaning is enabled, this may recursively
      // visits methods that belong to other classes in search of transitively referenced fields.
      for (cls <- func.getClass :: innerClasses) {
        getClassReader(cls).accept(new FieldAccessFinder(accessedFields, cleanTransitively), 0)
      }
    }

    logDebug(s" + fields accessed by starting closure: " + accessedFields.size)
    accessedFields.foreach { f => logDebug("     " + f) }

    // List of outer (class, object) pairs, ordered from outermost to innermost
    // Note that all outer objects but the outermost one (first one in this list) must be closures
    var outerPairs: List[(Class[_], AnyRef)] = (outerClasses zip outerObjects).reverse
    var parent: AnyRef = null
    if (outerPairs.size > 0) {
      val (outermostClass, outermostObject) = outerPairs.head
      if (isClosure(outermostClass)) {
        logDebug(s" + outermost object is a closure, so we clone it: ${outerPairs.head}")
      } else if (outermostClass.getName.startsWith("$line")) {
        // SPARK-14558: if the outermost object is a REPL line object, we should clone and clean it
        // as it may carray a lot of unnecessary information, e.g. hadoop conf, spark conf, etc.
        logDebug(s" + outermost object is a REPL line object, so we clone it: ${outerPairs.head}")
      } else {
        // The closure is ultimately nested inside a class; keep the object of that
        // class without cloning it since we don't want to clone the user's objects.
        // Note that we still need to keep around the outermost object itself because
        // we need it to clone its child closure later (see below).
        logDebug(" + outermost object is not a closure or REPL line object, so do not clone it: " +
          outerPairs.head)
        parent = outermostObject // e.g. SparkContext
        outerPairs = outerPairs.tail
      }
    } else {
      logDebug(" + there are no enclosing objects!")
    }

    // Clone the closure objects themselves, nulling out any fields that are not
    // used in the closure we're working on or any of its inner closures.
    for ((cls, obj) <- outerPairs) {
      logDebug(s" + cloning the object $obj of class ${cls.getName}")
      // We null out these unused references by cloning each object and then filling in all
      // required fields from the original object. We need the parent here because the Java
      // language specification requires the first constructor parameter of any closure to be
      // its enclosing object.
      val clone = instantiateClass(cls, parent)
      for (fieldName <- accessedFields(cls)) {
        val field = cls.getDeclaredField(fieldName)
        field.setAccessible(true)
        val value = field.get(obj)
        field.set(clone, value)
      }
      // If transitive cleaning is enabled, we recursively clean any enclosing closure using
      // the already populated accessed fields map of the starting closure
      if (cleanTransitively && isClosure(clone.getClass)) {
        logDebug(s" + cleaning cloned closure $clone recursively (${cls.getName})")
        // No need to check serializable here for the outer closures because we're
        // only interested in the serializability of the starting closure
        clean(clone, checkSerializable = false, cleanTransitively, accessedFields)
      }
      parent = clone
    }

    // Update the parent pointer ($outer) of this closure
    if (parent != null) {
      val field = func.getClass.getDeclaredField("$outer")
      field.setAccessible(true)
      // If the starting closure doesn't actually need our enclosing object, then just null it out
      if (accessedFields.contains(func.getClass) &&
        !accessedFields(func.getClass).contains("$outer")) {
        logDebug(s" + the starting closure doesn't actually need $parent, so we null it out")
        field.set(func, null)
      } else {
        // Update this closure's parent pointer to point to our enclosing object,
        // which could either be a cloned closure or the original user object
        field.set(func, parent)
      }
    }

    logDebug(s" +++ closure $func (${func.getClass.getName}) is now cleaned +++")

    if (checkSerializable) {
      ensureSerializable(func)
    }
  }

  private def ensureSerializable(func: AnyRef) {
    try {
      if (SparkEnv.get != null) {
        SparkEnv.get.closureSerializer.newInstance().serialize(func)
      }
    } catch {
      case ex: Exception => throw new SparkException("Task not serializable", ex)
    }
  }

  private def instantiateClass(
      cls: Class[_],
      enclosingObject: AnyRef): AnyRef = {
    // Use reflection to instantiate object without calling constructor
    val rf = sun.reflect.ReflectionFactory.getReflectionFactory()
    val parentCtor = classOf[java.lang.Object].getDeclaredConstructor()
    val newCtor = rf.newConstructorForSerialization(cls, parentCtor)
    val obj = newCtor.newInstance().asInstanceOf[AnyRef]
    if (enclosingObject != null) {
      val field = cls.getDeclaredField("$outer")
      field.setAccessible(true)
      field.set(obj, enclosingObject)
    }
    obj
  }
}

private[spark] class ReturnStatementInClosureException
  extends SparkException("Return statements aren't allowed in Spark closures")

private class ReturnStatementFinder extends ClassVisitor(ASM5) {
  override def visitMethod(access: Int, name: String, desc: String,
      sig: String, exceptions: Array[String]): MethodVisitor = {
    if (name.contains("apply")) {
      new MethodVisitor(ASM5) {
        override def visitTypeInsn(op: Int, tp: String) {
          if (op == NEW && tp.contains("scala/runtime/NonLocalReturnControl")) {
            throw new ReturnStatementInClosureException
          }
        }
      }
    } else {
      new MethodVisitor(ASM5) {}
    }
  }
}

/** Helper class to identify a method. */
private case class MethodIdentifier[T](cls: Class[T], name: String, desc: String)

/**
 * Find the fields accessed by a given class.
 *
 * The resulting fields are stored in the mutable map passed in through the constructor.
 * This map is assumed to have its keys already populated with the classes of interest.
 *
 * @param fields the mutable map that stores the fields to return
 * @param findTransitively if true, find fields indirectly referenced through method calls
 * @param specificMethod if not empty, visit only this specific method
 * @param visitedMethods a set of visited methods to avoid cycles
 */
private[util] class FieldAccessFinder(
    fields: Map[Class[_], Set[String]],
    findTransitively: Boolean,
    specificMethod: Option[MethodIdentifier[_]] = None,
    visitedMethods: Set[MethodIdentifier[_]] = Set.empty)
  extends ClassVisitor(ASM5) {

  override def visitMethod(
      access: Int,
      name: String,
      desc: String,
      sig: String,
      exceptions: Array[String]): MethodVisitor = {

    // If we are told to visit only a certain method and this is not the one, ignore it
    if (specificMethod.isDefined &&
        (specificMethod.get.name != name || specificMethod.get.desc != desc)) {
      return null
    }

    new MethodVisitor(ASM5) {
      override def visitFieldInsn(op: Int, owner: String, name: String, desc: String) {
        if (op == GETFIELD) {
          for (cl <- fields.keys if cl.getName == owner.replace('/', '.')) {
            fields(cl) += name
          }
        }
      }

      override def visitMethodInsn(
          op: Int, owner: String, name: String, desc: String, itf: Boolean) {
        for (cl <- fields.keys if cl.getName == owner.replace('/', '.')) {
          // Check for calls a getter method for a variable in an interpreter wrapper object.
          // This means that the corresponding field will be accessed, so we should save it.
          if (op == INVOKEVIRTUAL && owner.endsWith("$iwC") && !name.endsWith("$outer")) {
            fields(cl) += name
          }
          // Optionally visit other methods to find fields that are transitively referenced
          if (findTransitively) {
            val m = MethodIdentifier(cl, name, desc)
            if (!visitedMethods.contains(m)) {
              // Keep track of visited methods to avoid potential infinite cycles
              visitedMethods += m
              ClosureCleaner.getClassReader(cl).accept(
                new FieldAccessFinder(fields, findTransitively, Some(m), visitedMethods), 0)
            }
          }
        }
      }
    }
  }
}
private[util] class RecursiveFieldAccessFinder(
    fields: Map[Class[_], Set[String]],
    findTransitively: Boolean,
    specificMethod: Option[MethodIdentifier[_]] = None,
    visitedMethods: Set[MethodIdentifier[_]] = Set.empty)
  extends ClassVisitor(ASM5) {

  override def visitMethod(
      access: Int,
      name: String,
      desc: String,
      sig: String,
      exceptions: Array[String]): MethodVisitor = {

    // If we are told to visit only a certain method and this is not the one, ignore it
    if (specificMethod.isDefined &&
        (specificMethod.get.name != name || specificMethod.get.desc != desc)) {
      return null
    }

    new MethodVisitor(ASM5) {
      override def visitFieldInsn(op: Int, owner: String, name: String, desc: String) {
        if (op == GETFIELD) {
          var cl = Class.forName(owner.replace('/', '.'),
            false,
            Thread.currentThread.getContextClassLoader)
          if( ! (fields contains cl) ) fields(cl) = Set[String]()
          fields(cl) += name
        }
      }

      override def visitMethodInsn(op: Int, owner: String, name: String, desc: String, itf: Boolean) {
        var cl = Class.forName(owner.replace('/', '.'),
          false,
          Thread.currentThread.getContextClassLoader)
        println(s"visitMethodInsn: $owner")
        return
        //for (cl <- fields.keys if cl.getName == owner.replace('/', '.')) {
          // Check for calls a getter method for a variable in an interpreter wrapper object.
          // This means that the corresponding field will be accessed, so we should save it.
          if (op == INVOKEVIRTUAL && owner.endsWith("$iwC") && !name.endsWith("$outer")) {
            if( ! (fields contains cl) ) fields(cl) = Set[String]()
            fields(cl) += name
          }
          // Optionally visit other methods to find fields that are transitively referenced
          if (findTransitively) {
            val m = MethodIdentifier(cl, name, desc)
            if (!visitedMethods.contains(m)) {
              // Keep track of visited methods to avoid potential infinite cycles
              visitedMethods += m
              ClosureCleaner.getClassReader(cl).accept(
                new RecursiveFieldAccessFinder(fields, findTransitively, Some(m), visitedMethods), 0)
            }
          }
        //}
      }
    }
  }
}

private class InnerClosureFinder(output: Set[Class[_]]) extends ClassVisitor(ASM5) {
  var myName: String = null

  // TODO: Recursively find inner closures that we indirectly reference, e.g.
  //   val closure1 = () = { () => 1 }
  //   val closure2 = () => { (1 to 5).map(closure1) }
  // The second closure technically has two inner closures, but this finder only finds one

  override def visit(version: Int, access: Int, name: String, sig: String,
      superName: String, interfaces: Array[String]) {
    myName = name
  }

  override def visitMethod(access: Int, name: String, desc: String,
      sig: String, exceptions: Array[String]): MethodVisitor = {
    new MethodVisitor(ASM5) {
      override def visitMethodInsn(
          op: Int, owner: String, name: String, desc: String, itf: Boolean) {
        val argTypes = Type.getArgumentTypes(desc)
        if (op == INVOKESPECIAL && name == "<init>" && argTypes.length > 0
            && argTypes(0).toString.startsWith("L") // is it an object?
            && argTypes(0).getInternalName == myName) {
          // scalastyle:off classforname
          output += Class.forName(
              owner.replace('/', '.'),
              false,
              Thread.currentThread.getContextClassLoader)
          // scalastyle:on classforname
        }
      }
    }
  }
}

/* Finds all functions called from the given class, returning a list */
private class CalledFunctionsFinder(
    output: Set[(String,String,String)],
    funcsToRead: Option[Set[(String,String)]] = None) extends ClassVisitor(ASM5) {
  var myName: String = null
  var filterFunctions = !funcsToRead.isEmpty
  var followFunctions = funcsToRead getOrElse Set()

  override def visit(version: Int, access: Int, name: String, sig: String,
      superName: String, interfaces: Array[String]) {
    myName = name
  }

  override def visitMethod(access: Int, name: String, desc: String,
      sig: String, exceptions: Array[String]): MethodVisitor = {

        /* only visit this method if it's in the list */
       var sig = (name, desc)
       if(filterFunctions && !(followFunctions contains sig)){
         //return noop method visitor
         new MethodVisitor(ASM5) { }
       } else {
         new MethodVisitor(ASM5) {
           override def visitMethodInsn(
             op: Int, owner: String, name: String, desc: String, itf: Boolean) {
               val argTypes = Type.getArgumentTypes(desc)

               if(owner != myName){
                 output += new Tuple3(owner, name, desc)
               }
           }
         }
       }
  }
}
