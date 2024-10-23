# zyy-experiment2
## 设计思路

### DailyFundFlow

 **TokenizerMapper 类**

- **数据解析**：`map` 方法接收文本行，从中提取日期（`reportDate`）、购买金额（`totalPurchaseAmt`）和赎回金额（`totalRedeemAmt`）。日期作为输出的键，购买和赎回金额作为值，两者之间用逗号连接。
- **容错处理**：通过捕获 `NumberFormatException` ，保证数据有效性，忽略无效的行。

 **IntSumReducer 类**

- `reduce` 方法遍历同一个日期的所有记录，将购买金额和赎回金额分别累加。
- 输出的键是日期，值是该日期的总购买金额和总赎回金额。
<img width="637" alt="fcb5aca2b1356b96eee763f674044d4" src="https://github.com/user-attachments/assets/c703b5ea-b89b-4b36-8b07-f0d457b2e362">

### WeeklyTrancationVolume

计算每个星期几的平均购买和赎回金额，并按购买金额从高到低排序输出。

 **TokenizerMapper 类**

这个类负责将输入数据解析并映射到星期几和相应的金额值。

- 使用`SimpleDateFormat`解析日期字段，并确定该日期是星期几。
- 将星期作为输出的键，将金额（购买和赎回）作为输出的值。如果解析日期或格式出错，则忽略该记录。

 **IntSumReducer 类**

- 对每个键收到的所有金额值（购买和赎回）进行累加，并计算每个星期几的交易次数。
- 使用收集到的总和和交易次数计算平均购买和赎回金额。
- 使用`TreeMap`（按键逆序排列）存储平均购买金额和相关数据，保证数据输出时按照购买金额从大到小排序。

 **cleanup 方法**

在`cleanup`阶段，Reducer将排序后的结果输出，确保结果是按照平均购买金额从高到低排列的。
<img width="637" alt="a0b1f8db8b6fbbce515be14113a5230" src="https://github.com/user-attachments/assets/86f2c5cf-35ef-41d3-828b-ba85d745ca2e">

### UserActivityAnalysis

分析用户的活跃度，通过计算每个用户的交易次数（无论是购买还是赎回）来进行。

**ActivityMapper 类**

- `map` 方法首先检查数据行是否包含特定的表头或不相关字段，如包含则跳过，防止处理非数据行。
- 解析每行数据，分割成数组后，检查购买金额（`purchaseAmt`）和赎回金额（`redeemAmt`）字段。如果任意一个金额大于零，则认为该用户在该日有活跃交易，向Reducer输出用户ID和固定值1（表示一个活动记录）。

**SumReducer 类**

- `reduce` 方法对每个用户的活跃天数进行累加。
- 使用`TreeMap`（按键逆序排列）收集所有用户的活跃天数。键为用户活跃天数，值为用户ID。
- 在`cleanup`方法中，将排序后的用户ID和对应的活跃天数输出，从而得到按活跃天数排序的用户列表。
<img width="637" alt="03e2a042e2be6dd20995700816a9da4" src="https://github.com/user-attachments/assets/110c6a61-b628-4119-85f4-64195bafb35b">

### FinancialAnalyst

分析银行利率与用户申购/赎回行为的关系，并结合用户的总余额数据（即tBalance字段），来看是否高利率的日子会影响用户的总余额和交易行为。

- 使用`user_balance_table`和`mfd_bank_shibor`两个数据源，前者提供用户的交易和余额信息，后者提供日常的银行间拆借利率。
- 两个Mapper类
  - **`BalanceMapper`**：处理`user_balance_table`，提取关键字段如日期、总余额、申购量和赎回量，并以日期为key输出。
  - **`ShiborMapper`**：处理`mfd_bank_shibor`表，关注一周利率（`Interest_1_W`），以日期为key，输出利率信息。

- `AnalysisReducer`
  - Reducer接收来自两个Mapper的输出。由于都以日期为key，因此每个Reducer任务接收到的是同一日期的用户余额和利率数据。
  - 根据接收到的一周利率，将其分类到不同的利率区间。
  - 对于同一天内的数据，计算总余额、总申购量和总赎回量。
  - 最终输出包括日期、利率区间和对应的交易总额，使得输出数据既包含时间信息也反映了不同利率对用户行为的影响。

```
输出示例：
20140820	Range: 3-5%, Total Balance: 20233180538, Total Purchase: 308378692, Total Redeem: 202452782
20140821	Range: 3-5%, Total Balance: 20339106448, Total Purchase: 251763517, Total Redeem: 219963356
20140822	Range: 3-5%, Total Balance: 20370906609, Total Purchase: 246316056, Total Redeem: 179349206
20140825	Range: 3-5%, Total Balance: 20319023288, Total Purchase: 309574223, Total Redeem: 312413411
20140826	Range: 3-5%, Total Balance: 20316184100, Total Purchase: 306945089, Total Redeem: 285478563
20140827	Range: 3-5%, Total Balance: 20337650626, Total Purchase: 302194801, Total Redeem: 468164147
20140828	Range: 3-5%, Total Balance: 20171681280, Total Purchase: 245082751, Total Redeem: 297893861
20140829	Range: 3-5%, Total Balance: 20118870170, Total Purchase: 267554713, Total Redeem: 273756380

```
<img width="637" alt="390d7f2b11765d080611db2a8e81bdc" src="https://github.com/user-attachments/assets/ca674178-5b26-40d9-b498-a469f933fe4c">

- 分析：
1. 在利率较高时（5%以上），申购量和赎回量均有所增加，申购量的增长更为显著，这表明用户在利率较高时可能更倾向于购买金融产品。
2. 在中等利率（3-5%）区间，申购和赎回行为相对平稳，与较高利率区间相比，交易行为的波动较小。
3. 在低利率（0-3%）区间，申购和赎回行为的变化不如其他利率区间显著，可能表明低利率对用户交易行为的刺激作用较弱，用户交易意愿较低。

### 可以改进之处
在问题四的设计之中 我只划分了三个区间，及 `0-3%`、`3-5%`、`5%以上`，通过运算结果可以看出，整体的利率变化频率很小，一般连续10天左右都在同一个利率区间，并且5%以上的利率较少，不利于观察用户申购和赎回的影响，可以考虑进一步细化区间或者尝试别的方案。
