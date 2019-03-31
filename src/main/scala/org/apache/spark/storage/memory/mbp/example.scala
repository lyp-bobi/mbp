package main.scala.org.apache.spark.storage.memory

import scala.collection.mutable

case class MBR(x:Double,y:Double){
  def dist(other:MBR):Double={
    (x-other.x)*(x-other.x)+(y-other.y)*(y-other.y)
  }
}
case class knnSpatialPQ2[A,B](distArray: mutable.LinkedHashMap[A,MBR])
  extends mutable.LinkedHashMap[A, B]{
  import java.util.{LinkedList => JLinkedList}
  val neighbours = new mutable.LinkedHashMap[A,JLinkedList[(A,Double)]]
  def addNew(neighbour:JLinkedList[(A,Double)],x:A,dist:Double)={
    if(neighbour.size()==0){
      neighbour.add((x,dist))
    }else{
      var i=0
      while(i<neighbour.size()&&neighbour.get(i)._2>dist){
        i = i+1
      }
      neighbour.add(i,(x,dist))
    }
    if(neighbour.size()>3){
      neighbour.pollFirst()
    }
  }
  override def put(key: A, value:B ): Option[B] = {

    val newMBR = distArray.get(key)
    if(newMBR.isDefined){
      //TODO:get the mbr of key Block
      val neighbour =  new JLinkedList[(A,Double)]
      //candidate in memory
      neighbours.foreach(x =>{

        //TODO:get the mbr of rid Block
        val oldMBR = distArray.get(x._1)
        if(oldMBR.isDefined){
          val dist= newMBR.get.dist(oldMBR.get)
          addNew(neighbour,x._1,dist)
          if(neighbours.get(x._1).isDefined){
            addNew(neighbours(x._1),key,dist)
          }
        }
      })
      neighbours+=(key->neighbour)
      val iter = neighbour.iterator()
      while(iter.hasNext){
        moveToTail(iter.next())
      }
    }

    super.put(key,value)
  }
  def moveToTail(key:(A,Double))={
    super.remove(key._1) match {
      case Some(v) =>super.put(key._1,v)
      //case _=>super.put(key._1,_)
    }
  }
  def getSpatial(key: A):Option[B]={

      if(neighbours.get(key).isDefined){
        val iter = neighbours(key).iterator()
        while(iter.hasNext){
          moveToTail(iter.next())
        }
      }

      super.get(key)

  }
  //remove all blockId value in LinkedHashMap or just remove the blockId key
  override def remove(key:A):Option[B]={

        neighbours.remove(key.asInstanceOf[A])
        neighbours.foreach(li=>li._2.remove(key))
        super.remove(key.asInstanceOf[A])

  }

}
object example {
  def main(args: Array[String]): Unit = {
    val distArray = new mutable.LinkedHashMap[Int,MBR]
    distArray.put(1,MBR(2,3))
    distArray.put(2,MBR(5,6))
    distArray.put(3,MBR(10,20))
    distArray.put(4,MBR(1,2))
    distArray.put(5,MBR(3,2))
    val entries = new knnSpatialPQ2[Int, String](distArray)
    entries.put(1,"1")
    entries.put(2,"2")
    entries.put(3,"3")
    entries.put(4,"4")
    entries.put(9,"none")
    entries.getSpatial(10)
    entries.getSpatial(4)
    entries.remove(4)
    entries.remove(4)
    entries.put(5,"5")
    entries.put(6,"6")
  }

}
