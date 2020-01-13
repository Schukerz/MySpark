package com.atguigu.spark.acc

import com.atguigu.spark.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

//map((cid,"click"),count)
class CategoryAcc extends AccumulatorV2[UserVisitAction,Map[(String,String),Long]] {

  private var map: Map[(String, String), Long] = Map[(String,String),Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, Map[(String, String), Long]] ={

    val acc = new CategoryAcc
    acc.map = map
    acc
  }

  override def reset(): Unit = {map = Map[(String,String),Long]()}

  override def add(v: UserVisitAction): Unit = {

    if(v.click_category_id != -1){

      val clickTuple: (String, String) = (v.click_category_id.toString,"click")

      map += clickTuple->(map.getOrElse(clickTuple,0L)+1L)

    }else if(v.order_category_ids != "null"){

      val cids: Array[String] = v.order_category_ids.split(",")

      cids.foreach(cid =>{
        map += (cid,"order")->(map.getOrElse((cid,"order"),0L)+1L)
      }
      )

    }else if(v.pay_category_ids != "null"){

      val cids: Array[String] = v.pay_category_ids.split(",")

      cids.foreach(cid =>{
        map += (cid,"pay")->(map.getOrElse((cid,"pay"),0L)+1L)
      })

    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, Map[(String, String), Long]]): Unit = other match{
    case o : CategoryAcc => {

      o.map.foreach{
        case (cidAction,count) => this.map += (cidAction -> (this.map.getOrElse(cidAction,0L)+count))
        case _  => throw new UnsupportedOperationException
      }
    }
  }

  override def value: Map[(String, String), Long] = map
}
