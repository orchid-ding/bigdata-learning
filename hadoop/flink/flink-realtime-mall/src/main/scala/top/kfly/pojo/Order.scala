package top.kfly.pojo

/**
   * 定义样例类，用于封装数据
   */
case class Order(orderNo:String
                 , userId:String
                 , goodId:String
                 , goodsMoney:String
                 , realTotalMoney:String
                 , payFrom:String
                 , province:String
                  ) extends Serializable
