from pylearning.model.tensorflow_base import tensorflow_base
from pyspark.sql import SparkSession
from pyspark import SparkContext
import os
import random
import numpy as np

from pyspark.sql.functions import col
import tensorflow as tf

class tf_fm(tensorflow_base):

    @staticmethod
    def pre_train(env):
        spark_context = SparkContext.getOrCreate()
        spark = SparkSession(spark_context).builder.getOrCreate()
        rating_df = spark.read.format('csv').option('header', 'True').load('/moviedata/ratings.csv')
        movie_df = spark.read.format('csv').option('header', 'True').load('/moviedata/movies.csv')

        # process user first
        distinct_user_df = rating_df.select('userId').distinct()
        users_number = distinct_user_df.count()
        env.get("algo")["users_number"] = str(users_number)

        users_row = distinct_user_df.collect()
        users = []
        users_dict = []
        users_map = {}
        for user in users_row:
            users.append(user['userId'])
        sorted_users = sorted(users)
        for user in sorted_users:
            users_dict.append((user,len(users_dict)))
            users_map[user] = len(users_map)

        # It is use for later process, to get the sorted user id.
        columns = ["userid","id"]
        users_sort_df = spark.createDataFrame(users_dict,columns)
        # users_sort_df.write.format("csv").save("/moviedata/sortedusers")

        # process genres
        geners_row = movie_df.select("genres").distinct().collect()
        genres_set = set()
        genres_map = {}
        for genres in geners_row:
            for one_genre in genres['genres'].split('|'):
                genres_set.add(one_genre)
        for genre in genres_set:
            genres_map[genre] = len(genres_map)

        # join two dataframe and process later, userid(bigint) genres(string, need split), rating(float)
        joined_df = rating_df.join(movie_df, rating_df.movieId == movie_df.movieId)
        joined_df = joined_df.select(col('userId'),col('genres'),col('rating').cast('float').alias('rating'))

        users_map_bc = spark_context.broadcast(users_map)
        genres_map_bc = spark_context.broadcast(genres_map)
        env.get("algo")["genres_number"] = str(len(genres_map))

        def process_row(row):
            userId = row.userId
            genres = row.genres
            users_map_rdd = users_map_bc.value
            genres_map_rdd = genres_map_bc.value
            genres_return_list = []
            for i in genres.split("|"):
                genres_return_list.append(str(genres_map_rdd[i]))
            return (users_map_rdd[userId], "|".join(genres_return_list), row.rating)

        return joined_df.rdd.map(process_row).toDF(['userId','genres','rating'])

    @staticmethod
    def train(dataframe, env):
        environ = os.environ
        ps_hosts = environ.get("ps_hosts").split(",")
        worker_hosts = environ.get("worker_hosts").split(",")
        job_name = environ.get("job_name")
        task_index = int(environ.get("task_index"))

        cluster = tf.train.ClusterSpec({"ps": ps_hosts, "worker": worker_hosts})
        server = tf.train.Server(cluster,
                                 job_name= job_name,
                                 task_index=task_index)

        if job_name == "ps":
            server.join()
        else :
            # batch size is 2000, parameter size including embedding for user and one hot for genres
            # embedding size is 128, one hot size is 20(we can obtain it from env)
            batch_size = 2000

            embedding_size = 128
            genres_size = int(env.get("algo")["genres_number"])
            users_size = int(env.get("algo")["users_number"])
            p_size = embedding_size + genres_size
            k = 10
            embeddings = tf.Variable(tf.random_uniform([users_size,embedding_size], -1.0, 1.0))
            USER = tf.placeholder('int64',shape=[batch_size,1])
            ITEM = tf.placeholder('float', shape=[batch_size, genres_size])
            embed = tf.nn.embedding_lookup(embeddings, USER)
            user_embed = tf.reshape(embed, shape=[batch_size, embedding_size])
            X = tf.concat([user_embed, ITEM], 1)
            Y = tf.placeholder('float', shape=[batch_size,1])

            w0 = tf.Variable(tf.zeros([1]))
            W = tf.Variable(tf.zeros([p_size]))

            V = tf.Variable(tf.random_normal([k, p_size], stddev=0.01))
            y_hat = tf.Variable(tf.zeros([batch_size, 1]))

            linear_terms = tf.add(w0, tf.reduce_sum(tf.multiply(W, X), 1, keep_dims=True))
            interactions = (tf.multiply(0.5, tf.reduce_sum(
                tf.subtract(tf.pow(tf.matmul(X, tf.transpose(V)), 2),
                            tf.matmul(tf.pow(X, 2), tf.transpose(tf.pow(V, 2)))), 1,
                keep_dims=True)))

            y_hat = tf.add(linear_terms, interactions)
            lambda_w = tf.constant(0.001, name='lambda_w')
            lambda_v = tf.constant(0.001, name='lambda_v')

            l2_norm = (tf.reduce_sum(
                tf.add(
                    tf.multiply(lambda_w, tf.pow(W, 2)),
                    tf.multiply(lambda_v, tf.pow(V, 2)))))

            error = tf.reduce_mean(tf.square(tf.subtract(Y, y_hat)))

            loss = tf.add(error, l2_norm)

            N_EPOCHS = 100
            eta = tf.constant(0.1)
            global_step = tf.contrib.framework.get_or_create_global_step()
            optimizer = tf.train.AdagradOptimizer(eta).minimize(loss, global_step=global_step)

            init = tf.global_variables_initializer()

            def get_train_data():
                users_sub, genres_sub, rating_sub = \
                    zip(*random.sample(list(zip(dataframe.userId, dataframe.genres, dataframe.rating)), batch_size))
                batch_user = np.zeros(shape=(batch_size,1), dtype=np.int64)
                batch_genre = np.zeros(shape=(batch_size,genres_size), dtype=np.float32)
                label = np.ndarray(shape=(batch_size,1), dtype = np.float32)
                for i in range(batch_size):
                    batch_user[i] = users_sub[i]
                    for genre in genres_sub[i].split("|"):
                        batch_genre[i][int(genre)] = 1
                    label[i] = rating_sub[i]
                return batch_user, batch_genre, label

            checkpoint_dir = "hdfs://emr-header-1:9000/movie"
            saver = tf.train.Saver()
            epoch = 0

            with tf.train.MonitoredTrainingSession(master = server.target,
                                           is_chief = task_index == 0,
                                           checkpoint_dir= checkpoint_dir,
                                           save_checkpoint_secs=20) as sess:
                tf.reset_default_graph()
                sess.run(init)
                latest_path = tf.train.latest_checkpoint(checkpoint_dir=checkpoint_dir)
                saver.restore(sess, latest_path)
                while epoch < N_EPOCHS:
                    (batch_user,batch_genre,label) = get_train_data()
                    sess.run(optimizer, feed_dict={USER: batch_user, ITEM: batch_genre, Y:label})
                    print(sess.run(error, feed_dict={USER: batch_user, ITEM: batch_genre, Y: label}))
                    epoch = epoch + 1
