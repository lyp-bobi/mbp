/*
 * Copyright 2017 by mbp Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.spark.sql.catalyst.expressions.mbp

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.mbp.Feature.Feature
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._


object FeatureType extends UserDefinedType[Feature]{
  val kryo=new Kryo()
  override def sqlType: DataType = ArrayType(ByteType, containsNull = false)
  override def userClass: Class[Feature] = classOf[Feature]
  override def serialize(p: Feature): GenericInternalRow = {
    val out = new ByteArrayOutputStream()
    val output = new Output(out)
    kryo.writeObject(output, p)
    output.close()
    new GenericInternalRow(Array[Any](out.toByteArray))

  }

  override def deserialize(datum: Any): Feature = {
    val arr=datum.asInstanceOf[InternalRow]
    val raw = arr.getBinary(0)
    val in = new ByteArrayInputStream(raw)
    val input = new Input(in)
    val resx = kryo.readObject(input, classOf[Feature])
    resx
  }
}

class FeatureType extends UserDefinedType[Feature]{
  val kryo=new Kryo()
  override def sqlType: DataType = ArrayType(ByteType, containsNull = false)
  override def userClass: Class[Feature] = classOf[Feature]
  override def serialize(p: Feature): GenericInternalRow = {
    val out = new ByteArrayOutputStream()
    val output = new Output(out)
    kryo.writeObject(output, p)
    output.close()
    new GenericInternalRow(Array[Any](out.toByteArray))

  }

  override def deserialize(datum: Any): Feature = {
    val arr=datum.asInstanceOf[InternalRow]
    val raw = arr.getBinary(0)
    val in = new ByteArrayInputStream(raw)
    val input = new Input(in)
    val resx = kryo.readObject(input, classOf[Feature])
    resx
  }
}