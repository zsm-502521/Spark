--  需求：各区域热门商品 Top3
--  这里的热门商品是从点击量的维度来看的，计算各个区域前三大热门商品，并备注上每
--  个商品在主要城市中的分布比例，超过两个城市用其他显示。
--  例如：
--   地区      商品名称    点击次数                 城市备注
--   华北      商品 A      100000      北京 21.2%，天津 13.2%，其他 65.6%
--   华北      商品 P      80200       北京 63.0%，太原 10%，其他 27.0%
--   华北      商品 M      40000       北京 63.0%，太原 10%，其他 27.0%
--   东北      商品 J      92000       大连 28%，辽宁 17.0%，其他 55.0%



--  3.2.2 需求分析
--  ➢ 查询出来所有的点击记录，并与 city_info 表连接，得到每个城市所在的地区，与
--  Product_info 表连接得到产品名称
--  ➢ 按照地区和商品 id 分组，统计出每个商品在每个地区的总点击次数
--  ➢ 每个地区内按照点击次数降序排列
--  ➢ 只取前三名
--  ➢ 城市备注需要自定义 UDAF 函数
--  3.2.3 功能实现
--  ➢ 连接三张表的数据，获取完整的数据（只有点击）
--  ➢ 将数据根据地区，商品名称分组
--  ➢ 统计商品点击次数总和,取 Top3
--  ➢ 实现自定义聚合函数显示备