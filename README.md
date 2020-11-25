# Spark

## Spark Core

*  建立RDD物件
```
from pyspark import SparkContext
sc = SparkContext()
rdd = sc.parallelize([1,2,3])
# or
rdd = sc.textFiles(<檔案路徑>)
```
* rdd.map()
```
In [44]: example = sc.parallelize(['This is a book'])
In [45]: output = example.map(lambda x : x.split(' '))
In [47]: output.collect()
```
```
Out[47]: [['This', 'is', 'a', 'book']]
```
* rdd.flatMap()
```
In [48]: example = sc.parallelize(['This is a book'])
In [49]: output = example.flatMap(lambda x : x.split(' '))
In [50]: output.collect()
```
```
Out[50]: ['This', 'is', 'a', 'book']
```
* rdd.filter()
```
In [51]: example = sc.parallelize([1,2,4,5,8,10])
In [53]: output = example.filter(lambda x : x % 2 == 0)
In [54]: output.collect()
```
```
Out[54]: [2, 4, 8, 10]
```
* rdd.distinct()
```
In [55]: example = sc.parallelize([1,1,2,3])
In [56]: output = example.distinct()
In [57]: output.collect()
```
```
Out[57]: [3, 1, 2]
```
* rdd.sortBy()
```
In [58]: example = sc.parallelize(['A','F','C','G','M','E'])
In [61]: output = example.sortBy(lambda x : x)
In [62]: output.collect()
```
```
Out[62]: ['A', 'C', 'E', 'F', 'G', 'M']
```
* rdd.union()
```
In [63]: rdd = sc.parallelize([1,2,3])
In [64]: rdd2 = sc.parallelize([6,7,8])
In [65]: rdd3 = rdd.union(rdd2)
In [66]: rdd3.collect()
```
```
Out[66]: [1, 2, 3, 6, 7, 8]
```
* rdd.collect() ， rdd.take() ， rdd.first() ， rdd.count()
```
# collect
In [66]: rdd3.collect()
# take
In [68]: rdd3.take(3)
# first
In [69]: rdd3.first()
# count
In [70]: rdd3.count()
```
```
# collect
Out[66]: [1, 2, 3, 6, 7, 8]
# take
Out[68]: [1, 2, 3]
# first
Out[69]: 1
# count
Out[70]: 6
```
* rdd.reduce()
```
In [71]: example = sc.parallelize([2,3,4,7,8])
In [72]: output = example.reduce(lambda x, y : x + y)
In [73]: output
```
```
Out[73]: 24
```
* rdd.mapValues()
```
In [74]: example = sc.parallelize([('c', 2),('a', 3),('b', 1)])
In [75]: output = example.mapValues(lambda x : x * 2)
In [76]: output.collect()
```
```
Out[76]: [('c', 4), ('a', 6), ('b', 2)]
```
* rdd.reduceByKey()
```
In [77]: example = sc.parallelize([(1,30),(2,25),(1,20),(2,40),(2,15),(1,50)])
In [80]: def f(x,y): 
    ...:     return x + y
In [81]: output = example.reduceByKey(f)
In [82]: output.collect()
```
```
Out[82]: [(1, 100), (2, 80)]
```
* rdd.groupByKey()
```
In [83]: example = sc.parallelize([(1, 30),(2, 25),(1, 20),(2, 40),(2, 15),(1, 50)])
In [84]: output = example.groupByKey()
# 1
In [85]: output.collect()
# 2
In [86]: for i in output.collect()[0][1]: 
    ...:     print(i) 
```
```
# 1
Out[85]: [(1, <pyspark.resultiterable.ResultIterable at 0x7f4e5b43ca50>),
 　　　　　　(2, <pyspark.resultiterable.ResultIterable at 0x7f4e5b43c710>)]
# 2
Out[86]: 30
　　　　  20
　　　　  50
```
* rdd.join() ， rdd.leftOuterJoin() ， rdd.rightOuterJoin() ， rdd.fullOuterJoin
()
```
In [87]: x = sc.parallelize([('a', 1),('b', 4)])
In [88]: y = sc.parallelize([('a', 2),('a', 3),('c', 1)])
# join
In [89]: output = x.join(y)
In [90]: output.collect()
# leftOuterJoin
In [91]: output = x.leftOuterJoin(y)
In [92]: output.collect()
# rightOuterJoin
In [93]: output = x.rightOuterJoin(y)
In [94]: output.collect()
# fullOuterJoin
In [95]: output = x.fullOuterJoin(y)
In [96]: output.collect()
```
```
# join
Out[90]: [('a', (1, 2)), ('a', (1, 3))]
# leftOuterJoin
Out[92]: [('b', (4, None)), ('a', (1, 2)), ('a', (1, 3))]
# rightOuterJoin
Out[94]: [('c', (None, 1)), ('a', (1, 2)), ('a', (1, 3))]
# fullOuterJoin
Out[96]: [('c', (None, 1)), ('b', (4, None)), ('a', (1, 2)), ('a', (1, 3))]
```
* Accumulators(計數器)
```
In [97]: accum = sc.accumulator(0)
In [98]: sc.parallelize([1,2,3,4]).foreach(lambda x :accum.add(x))
In [99]: accum.value
```
```
Out[99]: 10
```
