#!/usr/bin/env python
# coding: utf-8

# In[ ]:


##Narrow Transformations
input_list=[1,2,3,4]


# In[ ]:


input_rdd=sc.parallelize(input_list,4)
input_rdd.getNumPartitions()


# In[ ]:


input_rdd.map(lambda x:x**2).collect()


# In[ ]:


def power_list(x):
  return x**2
input_rdd.map(power_list).collect()


# In[ ]:


input_list1=["Spark","PySpark","Python","Hive"]
input_rdd2=sc.parallelize(input_list1,4)


# In[ ]:


input_rdd2.filter(lambda x:len(x)>=5).collect()


# In[ ]:


print([i for i in input_list1 if len(i)>=5])


# In[ ]:


itera=list(filter(lambda x:len(x)>5,input_list1))


# In[ ]:


itera


# In[ ]:


list(map(lambda x: len(x)>=5,input_list1))
#map(lambda x: 'lower' if x < 3 else 'higher', lst)


# In[ ]:


def more_than(x):
  if len(x)>=5:
    return x
input_rdd2.map(more_than).collect()
  


# In[ ]:


def more_than_filter(x):
  if len(x)>=5:
    return x
input_rdd2.filter(more_than_filter).collect()


# In[ ]:


input_rdd2.map(lambda x:len(x)>=5).collect()


# In[ ]:


def filter_def(x):
  if len(x)>=5:
    return x
def map_def(x):
  x+=" in Big Data"
  return x
input_rdd2.filter(filter_def).map(map_def).collect()


# In[ ]:


input_rdd2.filter(lambda x:len(x)>=5).map(lambda x:x+" in Big Data").collect()


# In[ ]:


input_2d_list=[["Spark","Scala","PySpark"],["Python","Java","CPP"],["Spring Boot","Flask","Django"]]


# In[ ]:


input_rdd3=sc.parallelize(input_2d_list)


# In[ ]:


input_rdd3.getNumPartitions()


# In[ ]:


input_rdd3.flatMap(lambda x:x).collect()


# In[ ]:


input_rdd3.flatMap(lambda x:x).count()


# In[ ]:


input_rdd3.map(lambda x:x).collect()


# In[ ]:


input_rdd3.map(lambda x:x).count()


# In[ ]:


len(input_rdd3.flatMap(lambda x:x).collect())


# In[ ]:


#flatMap reduces the dimensions of the datastructure while map doesnt reduce the dimensions of the data structure


# In[ ]:


#Wide Transformations


# In[ ]:


input_list_4=[1,2,3,4,4,3,2,1]
input_rdd4=sc.parallelize(input_list_4,4)


# In[ ]:


input_rdd4.distinct().collect()


# In[ ]:


input_rdd4.distinct().count()


# In[ ]:


len(input_rdd4.distinct().collect())


# In[ ]:


input_rdd4.distinct().take(input_rdd4.distinct().count())


# In[ ]:


input_rdd4.distinct().takeOrdered(len(input_rdd4.collect()),key=None)


# In[ ]:


input_rdd4.repartition(8)


# In[ ]:


input_rdd4.getNumPartitions()


# In[ ]:


input_rdd4.repartition(2)


# In[ ]:


input_rdd4.getNumPartitions()


# In[ ]:


get_ipython().run_line_magic('sh', 'pwd')


# In[ ]:


get_ipython().run_line_magic('fs', 'ls')


# In[ ]:


get_ipython().run_line_magic('fs', 'ls /FileStore/')


# In[ ]:


get_ipython().run_line_magic('fs', 'ls /user/')


# In[ ]:


dbutils.fs.help()


# In[ ]:


dbutils.fs.put("abcd5.txt","Teja",False)


# In[ ]:


get_ipython().run_line_magic('fs', 'ls')


# In[ ]:


input_Rdd=sc.textFile('/abcd.txt')


# In[ ]:


input_Rdd.collect()


# In[ ]:


dbutils.fs.help()


# In[ ]:





# In[ ]:


dbutils.fs.ls('/')


# In[ ]:


get_ipython().run_line_magic('fs', 'ls')


# In[ ]:


get_ipython().run_line_magic('fs', "ls '/FileStore/'")


# In[ ]:


get_ipython().run_line_magic('fs', "ls '/FileStore/tables/'")


# In[ ]:


input_Rdd1=sc.textFile('/FileStore/tables/spark-3.txt')


# In[ ]:


input_Rdd1.collect()


# In[ ]:


dbutils.fs.help()


# In[ ]:


input_Rdd3=sc.textFile('/FileStore/tables/spark-2.txt',3)


# In[ ]:


input_Rdd3.collect()


# In[ ]:


input_Rdd1.getNumPartitions()


# In[ ]:


input_Rdd3.getNumPartitions()


# In[ ]:


#Pair RDD
input_Pair=[(1,2),(2,4),(3,6),(4,8)]
input_Pair_RDD=sc.parallelize(input_Pair)


# In[ ]:


input_Pair_RDD.getNumPartitions()


# In[ ]:


input_Pair_RDD.mapValues(lambda a:a*10).collect()


# In[ ]:


def multily(a):
  return a*10
input_Pair_RDD.mapValues(multily).collect()


# In[ ]:


def multiply(a):
  return a*10
  
input_Pair_RDD.mapValues(multiply).collect()


# In[ ]:


dbutils.fs.ls('/')


# In[ ]:


dbutils.fs.ls('/FileStore')


# In[ ]:


dbutils.fs.ls('/FileStore/tables/')


# In[ ]:


dbutils.fs.help()


# In[ ]:


Pair_list_1=[("Python","OOPS"),("Java","OOPS"),("Java","SpringBoot"),("Python","FLASK")]
Pair_list_2=[("Python","PySpark"),("Java","Scala"),("Java","Hibernate"),("Python","AI")]

Pair_RDD_1=sc.parallelize(Pair_list_1)
Pair_RDD_2=sc.parallelize(Pair_list_2)


# In[ ]:


Pair_RDD_1.collect()


# In[ ]:


Pair_RDD_2.collect()


# In[ ]:


Pair_RDD_1.count()


# In[ ]:


Pair_RDD_2.count()


# In[ ]:


Pair_RDD_1.getNumPartitions()


# In[ ]:


Pair_RDD_1.reduceByKey(lambda a,b:b+" Learning",numPartitions=4).collect()


# In[ ]:


def reducing(a,b):
  return b+" Learning"
Pair_RDD_1.reduceByKey(reducing,numPartitions=8).collect()


# In[ ]:


Pair_RDD_1.collect()


# In[ ]:


def multiply_1(a):
  return a*10
def filtering(b):
  return b[1]>50
input_Pair_RDD.mapValues(multiply_1).filter(filtering).collect()


# In[ ]:


Pair_RDD_1.collect()


# In[ ]:


Pair_RDD_2.collect()


# In[ ]:


Pair_RDD_1.getNumPartitions()


# In[ ]:


Pair_RDD_1.reduceByKey(lambda acc,n:acc+" & "+n).collect()


# In[ ]:


Pair_RDD_2.reduceByKey(lambda acc,n:acc+" & "+n).collect()


# In[ ]:


def Joining(acc,n):
  return acc+" & "+n
  
Pair_RDD_1.reduceByKey(Joining).collect()


# In[ ]:


def Joining_1(acc,N):
  return acc+" ^ "+N+" Learning"
Pair_RDD_2.reduceByKey(Joining_1).collect()


# In[ ]:


initial_Value=0
sc.accumulator(initial_Value).value


# In[ ]:


def joining_using_ACC(initial_Value,N):
  return initial_Value+" ACC "+N
Pair_RDD_2.reduceByKey(joining_using_ACC).collect()


# In[ ]:


Pair_RDD_1.distinct().collect()


# In[ ]:


Pair_RDD_3=sc.parallelize([(1,2),(1,2),(2,4),(2,5),(3,6),(4,5)],4)


# In[ ]:


Pair_RDD_3.distinct().collect()


# In[ ]:


Pair_RDD_3.count()


# In[ ]:


Pair_RDD_3.distinct().reduceByKey(lambda acc,n:acc+n).collect()


# In[ ]:


def check(a):
  return a[1]>5
Pair_RDD_3.distinct().reduceByKey(lambda acc,n:acc+n).filter(check).collect()


# In[ ]:


def redu(a,b):
  return b+" & "+a
Pair_RDD_2.reduceByKey(redu).collect()


# In[ ]:


from functools import reduce
l=[1,2,3]
def add_num(a,b):
  return a+b
reduce(add_num,l)


# In[ ]:


interset_1=[1,2,3,4,5,6,7]
input_RDD1=sc.parallelize(interset_1,4)
interset_2=[7,8,9,10,5,4,3,2,1]
input_RDD2=sc.parallelize(interset_2,4)


# In[ ]:


input_RDD1.getNumPartitions()


# In[ ]:


input_RDD2.getNumPartitions()


# In[ ]:


input_RDD1.intersection(input_RDD2).collect()


# In[ ]:


input_RDD1.intersection(input_RDD2).count()


# In[ ]:


input_RDD3=sc.parallelize([1,2,3,4,5,4,3,2,1],4)
input_RDD4=sc.parallelize([2,3,4,5,1,2,3,4],4)
input_RDD3.intersection(input_RDD4).collect()


# In[ ]:


input_RDD3.intersection(input_RDD4).count()


# In[ ]:


input_RDD5=sc.parallelize(["Java","Python","Spring Boot","Flask","Django"],4)
input_RDD6=sc.parallelize(["Flask","Django","Spring"],4)
input_RDD5.intersection(input_RDD6).filter(lambda a:len(a)>5).collect()


# In[ ]:


#Joins
input_Join_1=sc.parallelize([1,2,3,4],4)
input_Join_2=sc.parallelize([5,6,7,8],4)


# In[ ]:


input_Join_3=sc.parallelize([(1,2),(3,4)],4)
input_Join_4=sc.parallelize([(1,4),(1,5),(3,8)],4)


# In[ ]:


input_Join_3.join(input_Join_4).collect()


# In[ ]:


def appending(a):
  ans=[]
  for i in a:
    ans.append("Learning "+i)
  return tuple(ans)
  
input_Join_5=sc.parallelize([("Java","Scala"),("Python","Flask")],4)
input_Join_6=sc.parallelize([("Java","Spring"),("Python","Big Data")],4)

input_Join_5.join(input_Join_6).mapValues(appending).collect()


# In[ ]:



input_Join_7=sc.parallelize([(5,50),(6,60),(5,70)],4)
input_Join_8=sc.parallelize([(5,80),(6,90),(6,100)],4)

input_Join_7.join(input_Join_8).collect()


# In[ ]:


def f(a):
  ans=[]
  for i in a:
    ans.append(i+100)
  return tuple(ans)
input_Join_7.join(input_Join_8).mapValues(f).collect()


# In[ ]:


#Left Join
input_Join_7.collect()


# In[ ]:


input_Join_8.collect()


# In[ ]:


input_Join_9=input_Join_8.union(sc.parallelize([(9,90),(10,100)],4))
input_Join_9.collect()


# In[ ]:


input_Join_9.leftOuterJoin(input_Join_7).collect()


# In[ ]:


input_Join_10=input_Join_7.union(sc.parallelize([(120,130),(140,150)],4))
input_Join_9.rightOuterJoin(input_Join_10).collect()


# In[ ]:


input_Join_10.collect()


# In[ ]:


input_Join_9.collect()


# In[ ]:


input_Join_9.groupWith(input_Join_10).collect()


# In[ ]:


input_L1=[(1,2),(1,4),(2,6)]
input_L2=[(1,8),(3,10),(3,20)]
input_L1_RDD=sc.parallelize(input_L1,4)
input_L2_RDD=sc.parallelize(input_L2,4)


# In[ ]:


input_L1_RDD.join(input_L2_RDD).collect()


# In[ ]:


input_L1_RDD.leftOuterJoin(input_L2_RDD).collect()


# In[ ]:


def f(a,b):
  return sum(list(a))+sum(list(b))

input_L1_RDD.leftOuterJoin(input_L2_RDD).reduceByKey(f).collect()


# In[ ]:


def f(a,b):
  return sum(list(a))+sum(list(b))
final_list=input_L1_RDD.leftOuterJoin(input_L2_RDD).reduceByKey(f).collect()
ans=[]
for i in final_list:
  if type(i[1]) is tuple:
    ans.append((i[0],i[1][0]))
  else:
    ans.append(i)
print(ans)


# In[ ]:


Pair_RDD_1=sc.parallelize([("Python",4),("Java",3),("Python",5),("java",4)],4)


# In[ ]:


Pair_RDD_1.collect()


# In[ ]:


Pair_RDD_2=sc.parallelize([("Python",10),("Java",8)],4)


# In[ ]:


Pair_RDD_2.collect()


# In[ ]:


def a(a):
  ans=[]
  for i in a:
    ans.append(list(i))
  return ans
Pair_RDD_1.cogroup(Pair_RDD_2).mapValues(a).collect()


# In[ ]:


def a(a):
  ans=[]
  for i in a:
    ans.append(list(i))
  return ans
Pair_RDD_1.cogroup(Pair_RDD_2).mapValues(a).flatMapValues(lambda x:x).collect()


# In[ ]:


Pair_RDD_1.cogroup(Pair_RDD_2).mapValues(a).flatMap(lambda a:a).collect()


# In[ ]:


Pair_RDD_1.cogroup(Pair_RDD_2).mapValues(a).flatMapValues(lambda a:a).collect()


# In[ ]:


Pair_RDD_1.cogroup(Pair_RDD_2).mapValues(a).collect()


# In[ ]:


Pair_RDD_1.cogroup(Pair_RDD_2).mapValues(a).flatMapValues(lambda a:a).collect()


# In[ ]:


Pair_RDD_1.cogroup(Pair_RDD_2).mapValues(a).flatMapValues(lambda a:a).collect()


# In[ ]:


Pair_RDD_1.collect()


# In[ ]:


Pair_RDD_1.cogroup(Pair_RDD_2).mapValues(a).flatMapValues(lambda a:a).groupByKey().mapValues(list).collect()


# In[ ]:


Pair_RDD_1.collect()


# In[ ]:


def f(a,b):
  z=[]
  z.append(a)
  z.append(b)
  return z
Pair_RDD_1.reduceByKey(f).collect()


# In[ ]:


def a(ab):
  ans=[]
  for i in ab:
    ans.append(list(i))
  return ans
Pair_RDD_1.cogroup(Pair_RDD_2).mapValues(a).collect()


# In[ ]:


def ab(a,b):
  z=[]
  z.append(list((a,b)))
  return z

Pair_RDD_1.cogroup(Pair_RDD_2).mapValues(a).flatMapValues(lambda a:a).groupByKey().mapValues(list).collect()


# In[ ]:


Pair_RDD_1.collect()


# In[ ]:


Pair_RDD_2.collect()


# In[ ]:


Pair_RDD_1.join(Pair_RDD_2).collect()


# In[ ]:


Pair_RDD_1.join(Pair_RDD_2).mapValues(lambda a:list(a)).collect()


# In[ ]:


def f(a,b):
  a.extend(b)
  return a
Pair_RDD_1.join(Pair_RDD_2).mapValues(lambda a:list(a)).reduceByKey(f).collect()


# In[ ]:


Pair_RDD_1.join(Pair_RDD_2).mapValues(lambda a:list(a)).reduceByKey(f).mapValues(lambda a:sum(a)).collect()


# In[ ]:


input_list=[("Kohli",10),("virat",20),("dhoni",40),("virat",50),("dhoni",60)]
input_RDD=sc.parallelize(input_list,4)


# In[ ]:


input_RDD.collect()


# In[ ]:



input_RDD.groupByKey().mapValues(list).collect()


# In[ ]:


input_RDD.groupByKey().mapValues(list).mapValues(lambda a:sum(a)/len(a)).collect()


# In[ ]:


input_RDD.collect()


# In[ ]:


input_RDD.pipe('cat').collect()


# In[ ]:


input_RDD.pipe('ls').collect()


# In[ ]:


input_RDD.pipe("pwd").collect()


# In[ ]:


input_RDD.pipe("pwd").take(1)


# In[ ]:


input_RDD.collect()


# In[ ]:


input_RDD.reduceByKey(lambda a,b:a+b).collect()


# In[ ]:


input_RDD.groupByKey().mapValues(list).mapValues(lambda a:sum(a)/len(a)).collect()


# In[ ]:


def first(a):
  return [a]
def second(a,b):
  a.append(b)
  return a
def third(a,b):
  a.extend(b)
  return a
input_RDD.combineByKey(first,second,third).collect()


# In[ ]:


input_RDD.collect()


# In[ ]:


def first(a):
  return (a,1)
def second(a,b):
  return a[0]+b,a[1]+1
def third(a,b):
  return (a[0]+b[0],a[1]+b[1])
input_RDD.combineByKey(first,second,third).collect()


# In[ ]:


input_RDD.combineByKey(first,second,third).mapValues(lambda a:a[0]/a[1]).collect()


# In[ ]:


input_RDD.getNumPartitions()


# In[ ]:


input_RDD.repartition(5)


# In[ ]:


input_RDD.getNumPartitions()


# In[ ]:


input_RDD.coalesce(5)


# In[ ]:


input_RDD.getNumPartitions()


# In[ ]:


def first(a):
  return (a,1)
def second(a,b):
  return a[0]+b,a[1]+1
def third(a,b):
  return (a[0]+b[0],a[1]+b[1])
import time
begin=time.time()
input_RDD.groupByKey().mapValues(list).mapValues(lambda a:sum(a)/len(a)).collect()
end=time.time()
exec_time=end-begin
print(exec_time)
begin_1=time.time()
input_RDD.combineByKey(first,second,third).mapValues(lambda a:a[0]/a[1]).collect()
end_1=time.time()
exec_time_1=end_1-begin_1
print(exec_time_1)


# In[ ]:


input_RDD.repartition(2).getNumPartitions()


# In[ ]:


import time
repartition_time_begin=time.time()
print(input_RDD.repartition(2).getNumPartitions())
repartition_time_end=time.time()
repartition_time=repartition_time_end-repartition_time_begin
coalesce_time_begin=time.time()
print(input_RDD.coalesce(2).getNumPartitions())
coalesce_time_end=time.time()
coalesce_time=coalesce_time_end-coalesce_time_begin
print(repartition_time)
print(coalesce_time)


# In[ ]:


def second(a,b):
  return a+b
def third(a,b):
  return a+b
input_RDD.aggregateByKey((0),second,third).collect()


# In[ ]:


word_list=[("hello",1),("world",1),("hello",1)]
word_RDD=sc.parallelize(word_list,4)
word_RDD.reduceByKey(lambda a,b:a+b).collect()


# In[ ]:


input_str="I am new to Hadoop Hadoop is a Distributed Hadoop is a ecosystem"
input_list=input_str.split()
input_tu=[(i,1) for i in input_list]
print(input_tu)


# In[ ]:


word_RDD_1=sc.parallelize(input_tu,4)
word_RDD_1.reduceByKey(lambda a,b:a+b).takeOrdered(2)


# In[ ]:




