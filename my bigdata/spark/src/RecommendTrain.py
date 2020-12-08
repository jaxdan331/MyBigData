# -*- coding: UTF-8 -*-
from pyspark.mllib.recommendation import ALS
from pyspark import SparkConf, SparkContext


def SetLogger(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR )
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)    


def SetPath(sc):
    global Path
    if sc.master[0:5] == "local" :
        # Path = "file:///home/node1/pythonwork/PythonProject/"
        Path = "./"
    else:
        #如果要在cluster模式运行(hadoop yarn 或Spark Stand alone)，请按照书上的说明，先把文件上传到HDFS目录
        Path = "hdfs://node1:9000/user/node1/"


def CreateSparkContext():
    sparkConf = SparkConf().setAppName("RecommendTrain").set("spark.ui.showConsoleProgress", "false")
    sc = SparkContext(conf=sparkConf)
    print ("master="+sc.master)    
    SetLogger(sc)
    SetPath(sc)
    return (sc)
    
  
def PrepareData(sc): 
    # 建立用户评价数据
    print("开始读取用户评分数据...")
    rawUserData = sc.textFile(Path + "data/movie_test")
    ratingsRDD = rawUserData.map(lambda line: line.split("\t")[:3])  # 分割、取前三列
    # 显示数据项数
    numRatings = ratingsRDD.count()  # rate
    numUsers = ratingsRDD.map(lambda x: x[0]).distinct().count()  # user
    numMovies = ratingsRDD.map(lambda x: x[1]).distinct().count()  # movie
    print("总评论数：" + str(numRatings) + "，用户数：" + str(numUsers) + "，电影数：" + str(numMovies))
    return(ratingsRDD)


def SaveModel(sc): 
    try:        
        model.save(sc, Path + "ALSmodel")
        print("已存储 Model 在 ALSmodel")
    except Exception:
        print("Model 已经存在，请先删除再存储.")


if __name__ == "__main__":
    sc = CreateSparkContext()
    print("数据准备中...")
    ratingsRDD = PrepareData(sc)
    print("开始训练 ALS 模型...")
    print("参数：rank = 5, iterations = 20, lambda = 0.1")
    model = ALS.train(ratingsRDD, 5, 15, 0.1)
    print("训练结束...")
    print("正在存储 Model...")
    SaveModel(sc)
    # print("训练结束.")

