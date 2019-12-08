# `CUBE` and `ROLLUP` in Python pandas

Some SQL engines support `CUBE` and `ROLLUP` (e.g., Spark and Flink). It's very convenient when it comes to analytics. But it's not provided in pandas.

I implement a quick and dirty way to add supports for `CUBE` and `ROLLUP` in pandas in Python 3. The code is on [Github](https://github.com/fyears/cubing_in_pandas), but remember, **USE IT AT YOUR OWN RISK!**

This article explains the idea behind the code.

## normal group by

Supposing we have the following SQL table `example_table`:

```csv
product,price
A,1
A,2
B,2
B,3
```

Normally, if we want to get the sum of price, we would do the `GROUP BY` in SQL:

```sql
-- assuming we have the table "example_table" with the data insertted
SELECT
  product,
  SUM(price) price
FROM example_table
GROUP BY
  product
```

```csv
product,price
A,3
B,5
```

In Python, `pd.DataFrame` also supports the good old `groupby`:

```python
# self contained example
import pandas as pd
from io import StringIO

input_text = """product,price
A,1
A,2
B,2
B,3
"""
df = pd.read_csv(StringIO(input_text), sep=',')

df.groupby(['product'], as_index=False).agg({
    'price': 'sum'
})
##    product price
## 0    A       3
## 1    B       5
```

## folding some dimensions

What should we do, if we want to see the **total price**?

A quick way in SQL, is of course summing the column `price`. Easy.

```sql
SELECT
  SUM(price) price
FROM example_tbl
```

```csv
price
8
```

pandas has a similar way.

```python
df.agg({
    'price': 'sum'
}).to_frame().T
##    price
## 0    8
```

Sematically, we are just folding the dimension `product` by excluding it in group by columns.

**But**, what should we do if we want to see the total price of all products, **as well as** the total price of each product, **all at once**?

## `CUBE` and `ROLLUP` to rescue

Some SQL engines provide an elegant way to answer the above question. By using `CUBE` and `ROLLUP`, things become easy.

`CUBE` basically means folding the dimensions while still output those columns. `ROLLUP` produces a subset of `CUBE` by dropping some combinations of dimensions. Readers can search their differences online, such as [this Stack Overflow question](https://stackoverflow.com/questions/7053471).

Take the above data for example, in SQL, we may write this:

```sql
SELECT
  product,
  SUM(price) price
FROM example_table
GROUP BY
  product WITH CUBE
```

```csv
product,price
A,3
B,5
NULL,8
```

Notice that the result table has **3 rows**, instead of **2 rows** from simple group by. This is really useful when we are doing analytics.

Actually, the above `CUBE` result, is semantically equivalent to doing `UNION ALL` on different sub SQL statements:

```sql
SELECT
  product,
  price
FROM (
  SELECT
    product,
    SUM(price) AS price
  FROM example_table
  GROUP BY
    product

  UNION ALL
  SELECT
    NULL AS product,
    SUM(price) AS price
  FROM example_table
) t
```

## pandas implementation of `CUBE` and `ROLLUP`

But does Python pandas officially provides `CUBE` and `ROLLUP` methods? Not yet (as of Dec 2019, pandas version 0.25.3). I see that an issue had been created ([#29418](https://github.com/pandas-dev/pandas/issues/29418)), but it's still under discussion.

So I create my **quick and dirty** **helper function** (not officially in pandas core, but just a helper function) to achieve the same result. Basically, it's just a wrapper of unioning different simple `groupby`s together, and casting some values to the same `dtype`s. The code is on [GitHub](https://github.com/fyears/cubing_in_pandas) and released under MIT License. It's not (and likely not going to) be distributed on PyPI though.

**USE IT AT YOUR OWN RISK!**

**I strongly suggest every user reads and understands the code firstly, before using it in production environment.**

**The code supports Python 3 only!**

After copying and pasting the file `cubing_in_pandas.py` locally, you can use it like:

```python
from cubing_in_pandas import cubinggroupby

cubinggroupby(
    df, 
    cube_cols=['product'], # the columns you want to cube on
    agg={'price':'sum'},  # the aggregrate functions
    fill_grouping={'product':'TOTAL'}, # insert the special values to the rows of folded dimensions
    as_index=False # whether the dimensions are marked as pandas index in the result
)
##    product  price
## 0     A       3
## 1     B       5
## 2   TOTAL     8
```

**Caveats**. If the dimensions have null values (that could be determined by `pd.isnull()`), pandas just drop the corresponding rows in the result. Since my implementation is using basic pandas `groupby`, the problem still exists. This behavior is different from that in SQL `GROUP BY`.

Another example for the caveats:

```python
input_text_2 = """product,price
A,1
A,2
B,2
,3
"""
df2 = pd.read_csv(StringIO(input_text_2), sep=',')

# attention! product of null is not in the result.
df2.groupby(['product'], as_index=False).agg({
    'price': 'sum'
})
##    product price
## 0    A       3
## 1    B       2

# attention! The sum of price of all products, is still 8, not 5.
df2.agg({
    'price': 'sum'
}).to_frame().T
##    price
## 0    8

# so the cube wrapper function is just the unioned result
cubinggroupby(
    df2, # use the new example
    cube_cols=['product'],
    agg={'price':'sum'},
    fill_grouping={'product':'TOTAL'},
    as_index=False
)
##    product  price
## 0     A       3
## 1     B       2
## 2   TOTAL     8    # <- it is 8 not 5
```

## feedbacks are welcomed

If you have any suggestions or questions, you are welcomed to [create an issue in the Github repo](https://github.com/fyears/cubing_in_pandas/issues).

Thank you for reading this article.
