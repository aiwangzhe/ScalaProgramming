select
    dw.goodsId,
    dw.goodsSn,
    dw.productNo,
    dw.goodsName,
    dw.goodsImg,
    dw.shopId,
    dw.goodsType,
    dw.marketPrice,
    dw.shopPrice,
    dw.warnStock,
    dw.goodsStock,
    dw.goodsUnit,
    dw.goodsTips,
    dw.isSale,
    dw.isBest,
    dw.isHot,
    dw.isNew,
    dw.isRecom,
    dw.goodsCatIdPath,
    dw.goodsCatId,
    dw.shopCatId1,
    dw.shopCatId2,
    dw.brandId,
    dw.goodsDesc,
    dw.goodsStatus,
    dw.saleNum,
    dw.saleTime,
    dw.visitNum,
    dw.appraiseNum,
    dw.isSpec,
    dw.gallery,
    dw.goodsSeoKeywords,
    dw.illegalRemarks,
    dw.dataFlag,
    dw.createTime,
    dw.isFreeShipping,
    dw.goodsSerachKeywords,
    dw.modifyTime,
    dw.dw_start_date,
    case when dw.dw_end_date = '9999-12-31' and ods.goodsId is not null
             then '2019-09-09'
         else dw.dw_end_date
        end as dw_end_date
from
    `itcast_dw`.`dim_goods` dw
        left join
    (select * from `itcast_ods`.`itcast_goods` where dt='2019-09-10') ods
    on dw.goodsId = ods.goodsId ;

drop table if exists `itcast_dw`.`tmp_dim_goods_history`;
create table `itcast_dw`.`tmp_dim_goods_history`
as
select
    dw.goodsId,
    dw.goodsSn,
    dw.productNo,
    dw.goodsName,
    dw.goodsImg,
    dw.shopId,
    dw.goodsType,
    dw.marketPrice,
    dw.shopPrice,
    dw.warnStock,
    dw.goodsStock,
    dw.goodsUnit,
    dw.goodsTips,
    dw.isSale,
    dw.isBest,
    dw.isHot,
    dw.isNew,
    dw.isRecom,
    dw.goodsCatIdPath,
    dw.goodsCatId,
    dw.shopCatId1,
    dw.shopCatId2,
    dw.brandId,
    dw.goodsDesc,
    dw.goodsStatus,
    dw.saleNum,
    dw.saleTime,
    dw.visitNum,
    dw.appraiseNum,
    dw.isSpec,
    dw.gallery,
    dw.goodsSeoKeywords,
    dw.illegalRemarks,
    dw.dataFlag,
    dw.createTime,
    dw.isFreeShipping,
    dw.goodsSerachKeywords,
    dw.modifyTime,
    dw.dw_start_date,
    case when dw.dw_end_date >= '9999-12-31' and ods.goodsId is not null
             then '2019-09-09'
         else dw.dw_end_date
        end as dw_end_date
from
    `itcast_dw`.`dim_goods` dw
        left join
    (select * from `itcast_ods`.`itcast_goods` where dt='2019-09-10') ods
    on dw.goodsId = ods.goodsId
union all
select
    goodsId,
    goodsSn,
    productNo,
    goodsName,
    goodsImg,
    shopId,
    goodsType,
    marketPrice,
    shopPrice,
    warnStock,
    goodsStock,
    goodsUnit,
    goodsTips,
    isSale,
    isBest,
    isHot,
    isNew,
    isRecom,
    goodsCatIdPath,
    goodsCatId,
    shopCatId1,
    shopCatId2,
    brandId,
    goodsDesc,
    goodsStatus,
    saleNum,
    saleTime,
    visitNum,
    appraiseNum,
    isSpec,
    gallery,
    goodsSeoKeywords,
    illegalRemarks,
    dataFlag,
    createTime,
    isFreeShipping,
    goodsSerachKeywords,
    modifyTime,
    case when modifyTime is not null
             then from_unixtime(unix_timestamp(modifyTime, 'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd')
         else from_unixtime(unix_timestamp(createTime, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd')
        end as dw_start_date,
    '9999-12-31' as dw_end_date
from
    `itcast_ods`.`itcast_goods`
where dt = '2019-09-10';

insert overwrite table `itcast_dw`.`dim_goods`
select * from `itcast_dw`.`tmp_dim_goods_history`;


-- 创建dw层订单事实表--带有分区字段
DROP TABLE IF EXISTS itcast_dw.fact_orders;
create  table itcast_dw.fact_orders(
                                       orderId            bigint,
                                       orderNo            string,
                                       shopId             bigint,
                                       userId             bigint,
                                       orderStatus        bigint,
                                       goodsMoney         double,
                                       deliverType        bigint,
                                       deliverMoney       double,
                                       totalMoney         double,
                                       realTotalMoney     double,
                                       payType            bigint,
                                       isPay              bigint,
                                       areaId             bigint,
                                       userAddressId      bigint,
                                       areaIdPath         string,
                                       userName           string,
                                       userAddress        string,
                                       userPhone          string,
                                       orderScore         bigint,
                                       isInvoice          bigint,
                                       invoiceClient      string,
                                       orderRemarks       string,
                                       orderSrc           bigint,
                                       needPay            double,
                                       payRand            bigint,
                                       orderType          bigint,
                                       isRefund           bigint,
                                       isAppraise         bigint,
                                       cancelReason       bigint,
                                       rejectReason       bigint,
                                       rejectOtherReason  string,
                                       isClosed           bigint,
                                       goodsSearchKeys    string,
                                       orderunique        string,
                                       receiveTime        string,
                                       deliveryTime       string,
                                       tradeNo            string,
                                       dataFlag           bigint,
                                       createTime         string,
                                       settlementId       bigint,
                                       commissionFee      double,
                                       scoreMoney         double,
                                       useScore           bigint,
                                       orderCode          string,
                                       extraJson          string,
                                       orderCodeTargetId  bigint,
                                       noticeDeliver      bigint,
                                       invoiceJson        string,
                                       lockCashMoney      double,
                                       payTime            string,
                                       isBatch            bigint,
                                       totalPayFee        bigint,
                                       modifiedTime        string,
                                       dw_start_date       string,
                                       dw_end_date         string
)
    partitioned by (dt string) --按照天分区
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');
--临时订单表
DROP TABLE IF EXISTS itcast_dw.tmp_fact_orders;
create  table itcast_dw.tmp_fact_orders(
                                           orderId            bigint,
                                           orderNo            string,
                                           shopId             bigint,
                                           userId             bigint,
                                           orderStatus        bigint,
                                           goodsMoney         double,
                                           deliverType        bigint,
                                           deliverMoney       double,
                                           totalMoney         double,
                                           realTotalMoney     double,
                                           payType            bigint,
                                           isPay              bigint,
                                           areaId             bigint,
                                           userAddressId      bigint,
                                           areaIdPath         string,
                                           userName           string,
                                           userAddress        string,
                                           userPhone          string,
                                           orderScore         bigint,
                                           isInvoice          bigint,
                                           invoiceClient      string,
                                           orderRemarks       string,
                                           orderSrc           bigint,
                                           needPay            double,
                                           payRand            bigint,
                                           orderType          bigint,
                                           isRefund           bigint,
                                           isAppraise         bigint,
                                           cancelReason       bigint,
                                           rejectReason       bigint,
                                           rejectOtherReason  string,
                                           isClosed           bigint,
                                           goodsSearchKeys    string,
                                           orderunique        string,
                                           receiveTime        string,
                                           deliveryTime       string,
                                           tradeNo            string,
                                           dataFlag           bigint,
                                           createTime         string,
                                           settlementId       bigint,
                                           commissionFee      double,
                                           scoreMoney         double,
                                           useScore           bigint,
                                           orderCode          string,
                                           extraJson          string,
                                           orderCodeTargetId  bigint,
                                           noticeDeliver      bigint,
                                           invoiceJson        string,
                                           lockCashMoney      double,
                                           payTime            string,
                                           isBatch            bigint,
                                           totalPayFee        bigint,
                                           modifiedTime        string,
                                           dw_start_date       string,
                                           dw_end_date         string
)
    partitioned by (dt string)
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');

--创建订单退款表--带有分区字段
drop table if exists `itcast_dw`.`fact_order_refunds`;
create  table `itcast_dw`.`fact_order_refunds`(
                                                  id                bigint,
                                                  orderId           bigint,
                                                  goodsId           bigint,
                                                  refundTo          bigint,
                                                  refundReson       bigint,
                                                  refundOtherReson  string,
                                                  backMoney         double,
                                                  refundTradeNo     string,
                                                  refundRemark      string,
                                                  refundTime        string,
                                                  shopRejectReason  string,
                                                  refundStatus      bigint,
                                                  createTime        string,
                                                  modifiedTime        string,
                                                  dw_start_date string,
                                                  dw_end_date string
)
    partitioned by (dt string) --按照天分区
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');
--临时表
drop table if exists `itcast_dw`.`tmp_fact_order_refunds`;
create  table `itcast_dw`.`tmp_fact_order_refunds`(
                                                      id                bigint,
                                                      orderId           bigint,
                                                      goodsId           bigint,
                                                      refundTo          bigint,
                                                      refundReson       bigint,
                                                      refundOtherReson  string,
                                                      backMoney         double,
                                                      refundTradeNo     string,
                                                      refundRemark      string,
                                                      refundTime        string,
                                                      shopRejectReason  string,
                                                      refundStatus      bigint,
                                                      createTime        string,
                                                      modifiedTime        string,
                                                      dw_start_date string,
                                                      dw_end_date string
)
    partitioned by (dt string)
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');