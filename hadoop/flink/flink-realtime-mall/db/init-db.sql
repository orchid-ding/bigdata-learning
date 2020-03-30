
-- 1. create databases
CREATE DATABASE IF NOT EXISTS flink_house DEFAULT CHARACTER SET=utf8;
use flink_house;

-- 2. create data table
DROP TABLE IF EXISTS `kfly_goods`;

CREATE TABLE flink_house.`kfly_goods` (
                                 `goodsId` BIGINT(10) NOT NULL AUTO_INCREMENT,
                                 `goodsName` VARCHAR(256) DEFAULT NULL,  -- 商品名称
                                 `sellingPrice` VARCHAR(256) DEFAULT NULL,  -- 商品售价
                                 `productPic` VARCHAR(256) DEFAULT NULL,  -- 商品图片
                                 `productBrand` VARCHAR(256) DEFAULT NULL,  -- 商品品牌
                                 `productfbl` VARCHAR(256) DEFAULT NULL, -- 手机分片率
                                 `productNum` VARCHAR(256) DEFAULT NULL,  -- 商品编号
                                 `productUrl` VARCHAR(256) DEFAULT NULL,   -- 商品url地址
                                 `productFrom` VARCHAR(256) DEFAULT NULL,  -- 商品来源
                                 `goodsStock` INT(11) DEFAULT NULL,        -- 商品库存
                                 `appraiseNum` INT(11) DEFAULT NULL,       -- 商品评论数
                                 PRIMARY KEY (`goodsId`)
) ENGINE=INNODB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;


-- 2. create data table
CREATE TABLE kfly_orders (
                                        orderId int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
                                        orderNo varchar(50) NOT NULL COMMENT '订单号',
                                        userId int(11) NOT NULL COMMENT '用户ID',
                                        goodId int(11) NOT NULL COMMENT '商品ID',
                                        goodsMoney decimal(11,2) NOT NULL DEFAULT '0.00' COMMENT '商品总金额',
                                        realTotalMoney decimal(11,2) NOT NULL DEFAULT '0.00' COMMENT '实际订单总金额',
                                        payFrom int(11) NOT NULL DEFAULT '0' COMMENT '支付来源(1:支付宝，2：微信)',
                                        province varchar(50) NOT NULL COMMENT '省份',
                                        createTime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                        PRIMARY KEY (`orderId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8


-- 3. hbase
-- bin/hbase shell
-- create_namespace 'flink'
-- create 'flink:data_goods',{NAME=>'f1',BLOCKCACHE=>true,BLOOMFILTER=>'ROW',DATA_BLOCK_ENCODING => 'PREFIX_TREE', BLOCKSIZE => '65536'}

-- 4. 创建商品表
-- bin/kafka-topics.sh --create --topic flink_house --replication-factor 1  --partitions 3 --zookeeper node01:2181

-- create 'flink:data_orders',{NAME=>'f1',BLOCKCACHE=>true,BLOOMFILTER=>'ROW',DATA_BLOCK_ENCODING => 'PREFIX_TREE', BLOCKSIZE => '65536'}