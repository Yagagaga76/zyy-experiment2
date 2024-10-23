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

#### InterestImpactAnalyst

研究**银行间利率对用户申购和赎回行为的影响**，假设利率较高时，用户可能更倾向于赎回资金而非进行余额宝申购，因为较高的利率会促使用户将资金投入更高收益的投资方式。

- 具体分析：

使用 `mfd_bank_shibor` 表中的一周利率 (`Interest_1_W`) 和 `user_balance_table` 表中的总申购 (`total_purchase_amt`) 与总赎回 (`total_redeem_amt`) 数据进行分析，通过将不同的利率分为区间来计算这些区间下的资金流入和流出情况，从而观察利率变化对用户申购和赎回的影响。

- 设计思路：

1. **Mapper**：
   - 输入 `user_balance_table` 数据和 `mfd_bank_shibor` 数据，根据日期进行匹配。
   - 将一周利率划分为区间，如 `0-3%`、`3-5%`、`5%以上`。

2. **Reducer**：
   - 对于每个利率区间，统计所有的申购和赎回总量。
   - 计算每个日期的平均申购和赎回金额，并输出。

- 输出内容为：

```
<利率区间> <日期> <平均申购量>,<平均赎回量>
例如：
0-3% 20130910	Purchase: 94684143, Redeem: 37128363
3-5% 20130911	Purchase: 98944459, Redeem: 71674082
0-3% 20130912	Purchase: 68573684, Redeem: 45147220
0-3% 20130913	Purchase: 71655946, Redeem: 60512675
3-5% 20130916	Purchase: 161656210, Redeem: 45184589
3-5% 20130917	Purchase: 76204815, Redeem: 58260798
```
<img width="637" alt="c67fd7f9aa091c36270ea6b845eae3d" src="https://github.com/user-attachments/assets/f5a47100-c592-4e7e-a1f4-a1b35053e93b">
<img width="1280" alt="deedd037c59d6f52a0d8b1c352c92a2" src="https://github.com/user-attachments/assets/473b0e86-c8ce-4a0c-a603-79a0721df518">

### 可以改进之处
在问题四的设计之中 我只划分了三个区间，及 `0-3%`、`3-5%`、`5%以上`，通过运算结果可以看出，整体的利率变化频率很小，一般连续10天左右都在同一个利率区间，并且5%以上的利率较少，不利于观察用户申购和赎回的影响，可以考虑进一步细化区间或者尝试别的方案。
