create database itcast_ods;

-- 创建ods层订单表
drop table if exists `itcast_ods`.`itcast_orders`;
create EXTERNAL table `itcast_ods`.`itcast_orders`(
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
    modifiedTime        string
)
partitioned by (dt string)
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');

-- 创建ods层订单明细表
drop table if exists `itcast_ods`.`itcast_order_goods`;
create EXTERNAL table `itcast_ods`.`itcast_order_goods`(
    ogId            bigint,
    orderId         bigint,
    goodsId         bigint,
    goodsNum        bigint,
    goodsPrice      double,
    payPrice        double,
    goodsSpecId     bigint,
    goodsSpecNames  string,
    goodsName       string,
    goodsImg        string,
    extraJson       string,
    goodsType       bigint,
    commissionRate  double,
    goodsCode       string,
    promotionJson   string,
    createtime      string
)
partitioned by (dt string)
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');

-- 创建ods层店铺表
drop table if exists `itcast_ods`.`itcast_shops`;
create EXTERNAL table `itcast_ods`.`itcast_shops`(
    shopId             bigint,
    shopSn             string,
    userId             bigint,
    areaIdPath         string,
    areaId             bigint,
    isSelf             bigint,
    shopName           string,
    shopkeeper         string,
    telephone          string,
    shopCompany        string,
    shopImg            string,
    shopTel            string,
    shopQQ             string,
    shopWangWang       string,
    shopAddress        string,
    bankId             bigint,
    bankNo             string,
    bankUserName       string,
    isInvoice          bigint,
    invoiceRemarks     string,
    serviceStartTime   bigint,
    serviceEndTime     bigint,
    freight            bigint,
    shopAtive          bigint,
    shopStatus         bigint,
    statusDesc         string,
    dataFlag           bigint,
    createTime         string,
    shopMoney          double,
    lockMoney          double,
    noSettledOrderNum  bigint,
    noSettledOrderFee  double,
    paymentMoney       double,
    bankAreaId         bigint,
    bankAreaIdPath     string,
    applyStatus        bigint,
    applyDesc          string,
    applyTime          string,
    applyStep          bigint,
    shopNotice         string,
    rechargeMoney      double,
    longitude          double,
    latitude           double,
    mapLevel           bigint,
    BDcode             string,
    modifyTime         string
)
partitioned by (dt string)
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');

-- 创建ods层商品表
drop table if exists `itcast_ods`.`itcast_goods`;
create EXTERNAL table `itcast_ods`.`itcast_goods`(
    goodsId              bigint,
    goodsSn              string,
    productNo            string,
    goodsName            string,
    goodsImg             string,
    shopId               bigint,
    goodsType            bigint,
    marketPrice          double,
    shopPrice            double,
    warnStock            bigint,
    goodsStock           bigint,
    goodsUnit            string,
    goodsTips            string,
    isSale               bigint,
    isBest               bigint,
    isHot                bigint,
    isNew                bigint,
    isRecom              bigint,
    goodsCatIdPath       string,
    goodsCatId           bigint,
    shopCatId1           bigint,
    shopCatId2           bigint,
    brandId              bigint,
    goodsDesc            string,
    goodsStatus          bigint,
    saleNum              bigint,
    saleTime             string,
    visitNum             bigint,
    appraiseNum          bigint,
    isSpec               bigint,
    gallery              string,
    goodsSeoKeywords     string,
    illegalRemarks       string,
    dataFlag             bigint,
    createTime           string,
    isFreeShipping       bigint,
    goodsSerachKeywords  string,
    modifyTime           string
)
partitioned by (dt string)
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');

-- 创建ods层组织机构表
drop table `itcast_ods`.`itcast_org`;
create EXTERNAL table `itcast_ods`.`itcast_org`(
    orgId        bigint,
    parentId     bigint,
    orgName      string,
    orgLevel     bigint,
    managerCode  string,
    isdelete     bigint,
    createTime   string,
    updateTime   string,
    isShow       bigint,
    orgType      bigint
)
partitioned by (dt string)
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');

-- 创建ods层商品分类表
drop table if exists `itcast_ods`.`itcast_goods_cats`;
create EXTERNAL table `itcast_ods`.`itcast_goods_cats`(
    catId               bigint,
    parentId            bigint,
    catName             string,
    isShow              bigint,
    isFloor             bigint,
    catSort             bigint,
    dataFlag            bigint,
    createTime          string,
    commissionRate      double,
    catImg              string,
    subTitle            string,
    simpleName          string,
    seoTitle            string,
    seoKeywords         string,
    seoDes              string,
    catListTheme        string,
    detailTheme         string,
    mobileCatListTheme  string,
    mobileDetailTheme   string,
    wechatCatListTheme  string,
    wechatDetailTheme   string,
    cat_level           bigint
)
partitioned by (dt string)
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');

-- 创建ods层用户表
drop table if exists `itcast_ods`.`itcast_users`;
create EXTERNAL table `itcast_ods`.`itcast_users`(
    userId          bigint,
    loginName       string,
    loginSecret     bigint,
    loginPwd        string,
    userType        bigint,
    userSex         bigint,
    userName        string,
    trueName        string,
    brithday        string,
    userPhoto       string,
    userQQ          string,
    userPhone       string,
    userEmail       string,
    userScore       bigint,
    userTotalScore  bigint,
    lastIP          string,
    lastTime        string,
    userFrom        bigint,
    userMoney       double,
    lockMoney       double,
    userStatus      bigint,
    dataFlag        bigint,
    createTime      string,
    payPwd          string,
    rechargeMoney   double,
    isInform        bigint
)
partitioned by (dt string)
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');

-- 创建ods层退货表
drop table if exists `itcast_ods`.`itcast_order_refunds`;
create EXTERNAL table `itcast_ods`.`itcast_order_refunds`(
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
    modifiedTime        string
)
partitioned by (dt string)
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');

-- 创建ods层地址表
drop table if exists `itcast_ods`.`itcast_user_address`;
create EXTERNAL table `itcast_ods`.`itcast_user_address`(
    addressId    bigint,
    userId       bigint,
    userName     string,
    otherName    string,
    userPhone    string,
    areaIdPath   string,
    areaId       bigint,
    userAddress  string,
    isDefault    bigint,
    dataFlag     bigint,
    createTime   string
)
partitioned by (dt string)
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');

-- 创建ods层支付方式表
drop table if exists `itcast_ods`.`itcast_payments`;
create EXTERNAL table `itcast_ods`.`itcast_payments`(
    id         bigint,
    payCode    string,
    payName    string,
    payDesc    string,
    payOrder   bigint,
    payConfig  string,
    enabled    bigint,
    isOnline   bigint,
    payFor     string
)
partitioned by (dt string)
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');


