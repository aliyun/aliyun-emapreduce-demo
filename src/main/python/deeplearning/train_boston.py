from pylearning.model.tensorflow_base import tensorflow_base
from pyspark.sql import SparkSession
from pyspark import SparkContext

import tensorflow as tf
from pyspark.sql.functions import col

class train_boston(tensorflow_base):
    @staticmethod
    def pre_train():
        spark_context = SparkContext.getOrCreate()
        spark = SparkSession(spark_context).builder.getOrCreate()
        df = spark.read.format('csv').option("header","True").load('/train.csv')
        cast_df = df.select(*(col(c).cast("double").alias(c) for c in df.columns))
        return cast_df

    @staticmethod
    def train(dataframe, env):
        crim = tf.feature_column.numeric_column('crim', dtype=tf.float64, shape=())
        zn = tf.feature_column.numeric_column('zn', dtype=tf.float64, shape=())
        indus = tf.feature_column.numeric_column('indus', dtype=tf.float64, shape=())
        chas = tf.feature_column.numeric_column('chas', dtype=tf.int64, shape=())
        nox = tf.feature_column.numeric_column('nox', dtype=tf.float64, shape=())
        rm = tf.feature_column.numeric_column('rm', dtype=tf.float64, shape=())
        age = tf.feature_column.numeric_column('age', dtype=tf.float64, shape=())
        dis = tf.feature_column.numeric_column('dis', dtype=tf.float64, shape=())
        rad = tf.feature_column.numeric_column('rad', dtype=tf.int64, shape=())
        tax = tf.feature_column.numeric_column('tax', dtype=tf.int64, shape=())
        ptratio = tf.feature_column.numeric_column('ptratio', dtype=tf.float64, shape=())
        black = tf.feature_column.numeric_column('black', dtype=tf.float64, shape=())
        lstat = tf.feature_column.numeric_column('lstat', dtype=tf.float64, shape=())

        feature_cols = [crim, zn, indus, chas, nox, rm, age, dis, rad, tax, ptratio, black, lstat]
        feature_names = ['ID','crim', 'zn', 'indus', 'chas', 'nox', 'rm', 'age', 'dis', 'rad', 'tax', 'ptratio', 'black',
                         'lstat']
        label_name = 'medv'

        dict = {}

        index = 0
        for i in feature_names:
            dict[i] = index
            index+=1

        def train_input():
            feature_dict = {}
            for i in feature_names[1:]:
                feature_dict[i] = dataframe.get(i)

            _dataset = tf.data.Dataset.from_tensor_slices((feature_dict, dataframe.get(label_name)))
            dataset = _dataset.batch(32)
            return dataset

        ps = tf.contrib.distribute.ParameterServerStrategy()
        config = tf.estimator.RunConfig(train_distribute=ps, eval_distribute=ps)
        estimator = tf.estimator.LinearRegressor(feature_columns=feature_cols, model_dir='hdfs://emr-header-1:9000/boston', config=config)

        train_spec = tf.estimator.TrainSpec(input_fn=train_input, max_steps=100)
        eval_spec = tf.estimator.EvalSpec(input_fn=train_input, start_delay_secs=0, throttle_secs=10,steps=10)
        tf.estimator.train_and_evaluate(estimator, train_spec, eval_spec)

