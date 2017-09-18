from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
from pyspark.sql.functions import *

def parseInput(line):
    fields = line.split(' ')
    return fields

if __name__ == "__main__":     
     
    spark = SparkSession.builder.appName('SparkExample').getOrCreate()
    sc = spark.sparkContext.getOrCreate()
    sparkWikiRDD = sc.textFile('C:/spark_text.txt')

    #map Example   
    mappedSparkWiki = sparkWikiRDD.map(lambda line : line.split(' '))
    #mappedsparkWiki = sparkWikiRDD.map(parseInput)    
    
    #take Example
    mappedSparkWiki.take(5)  
    
    
    #flatMap Example
    flatmapSparkWiki = sparkWikiRDD.flatMap(lambda line : line.split(' '))
    flatmapSparkWiki.take(5)
    
    #filter Example
    filteredCluster = sparkWikiRDD.filter(lambda line: ('cluster' in line.lower()))
    
    #count Example
    filteredCluster.count()
    
    #collect Example
    filteredCluster.collect()
    
    #sample Example
    sampledSparkWiki = sparkWikiRDD.sample(True,0.5,0)
    sampledSparkWiki.collect()
    
    student1_marks = [("C#", 90), ("PHP", 92), ("Machine Learning", 95)]    
    student2_marks = [("C#", 80), ("PHP", 83), ("Machine Learning", 87)]
    
    #dataFrame Example
    student1 = spark.createDataFrame(student1_marks)
    student2 = spark.createDataFrame(student2_marks)
    
    #union Example
    students_union = student1.union(student2).collect()
    
    #iterate Example
    for a in students_union:
        print (a[0], a[1])
    
    #join Example 
    students_join = student1.join(student2, student1['_1'] == student2['_1'], 'inner')
    sj = students_join.collect()
    for a in sj:
        print (a)
    
    #columns Example    
    student1.columns    
       
    #Without DataFrame Example
    #parallelize Example
    student1_par = sc.parallelize(student1_marks)
    student2_par = sc.parallelize(student2_marks)    
    students_join_par = student1_par.join(student2_par)
    students_join_par.collect()
    
    
    batsman = ['sachin', 'rahul', 'sehwag', 'kapil']
    bowler = ['kapil', 'anil', 'srinath']
    fielder = ['ajay', 'rahul']
    batsmanRDD = sc.parallelize(batsman)
    bowlerRDD = sc.parallelize(bowler)   
    
    #intersection Example
    allRounders = batsmanRDD.intersection(bowlerRDD)
    allRounders.collect()
          
    
    fielderRDD = sc.parallelize(fielder)    
    totalRDD = batsmanRDD.union(bowlerRDD).union(fielderRDD)
    
    #distinct Example
    teamRDD = totalRDD.distinct()
    teamRDD.collect()
   
    #getNumPartitions Example  
    teamRDD.getNumPartitions()