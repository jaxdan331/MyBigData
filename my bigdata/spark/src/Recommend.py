# -*- coding: UTF-8 -*-
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import  MatrixFactorizationModel
import os
import sys

with open('./data/movie_test', 'r', encoding='utf-8') as f:
    lines = f.readlines()

users, mvs = [], []
for line in lines:
    items = line.split('\t')
    # 推荐的时候用户和电影 id 都必须是整数，所以：
    users.append(int(items[0]))
    mvs.append(int(items[1]))
users = sorted(set(users))
mvs = sorted(set(mvs))
print("共找到 %d 个用户，%d 部电影." % (len(users), len(mvs)))


def CreateSparkContext():
    sparkConf = SparkConf().setAppName("Recommend").set("spark.ui.showConsoleProgress", "false")
    sc = SparkContext(conf=sparkConf)
    print("master = "+sc.master)
    SetLogger(sc)
    SetPath(sc)
    return (sc)


def SetPath(sc):
    global Path
    if sc.master[0:5] == "local" :
        # Path = "file:///home/node1/pythonwork/PythonProject/"
        Path = "./"
    else:   
        Path = "hdfs://node1:9000/user/node1/"


def SetLogger(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR )
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)


# def get_users_and_mvs(sc):
#     print("开始读取所有用户和电影...")
#     itemRDD = sc.textFile(Path + "input_data/u.data")
#     movieTitle = itemRDD.map(lambda line: line.split("\t")).map(lambda a: (float(a[0]), a[1])).collectAsMap()
#     return(movieTitle)


def RecommendMovies(model):
    for usr in users:
        RecommendMovie = model.recommendProducts(usr, 10)  # 第二个参数是推荐的数量，后同
        print("针对id为 ", usr, " 的用户推荐下列电影:")
        for rmd in RecommendMovie:
            # rmd[0] 是用户id，这里不需要
            print("\t电影id：{0}，推荐指数：{1}".format(rmd[1], rmd[2]))


def RecommendUsers(model):
    for mv in mvs:
        RecommendUser = model.recommendUsers(mv, 10)
        print("针对id为 ", mv, " 的电影推荐下列用户:")
        for rmd in RecommendUser:
            print("\t用户id：{0}，推荐指数：{1}".format(rmd[0], rmd[2]))


def loadModel(sc):
    try:        
        model = MatrixFactorizationModel.load(sc, Path + "ALSmodel")
        print("载入ALSModel模型")
        return model
    except Exception:
        print("找不到ALSModel模型，请先训练")


if __name__ == "__main__":
    sc = CreateSparkContext()
    # print("==========数据准备===============")
    # # movieTitle = PrepareData(sc)
    print("模型载入中...")
    model = loadModel(sc)
    print("推荐结果：")
    RecommendMovies(model)
    # RecommendUsers(model)


