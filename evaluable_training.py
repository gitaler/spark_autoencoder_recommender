from elephas.spark_model import SparkModel
from pyspark import SparkContext

# returns mse loss
def mse_value(x):
    squared_errors = (x[0] - x[1]) ** 2
    mse = squared_errors.sum() / len(x[0])
    return mse

# returns average mse loss given the original and the reconstructed rdds
def calculate_avg_mse_loss(original_set, recs_set):

    indexed_set = original_set.zipWithIndex().map(lambda x: (x[1], x[0]))
    indexed_recs = recs_set.zipWithIndex().map(lambda x: (x[1], x[0]))
    indexed_sample_rec = indexed_set.join(indexed_recs) # [(0, (sample, rec)), (1, (sample, rec)), ... ]
    squared_errors = indexed_sample_rec.map(lambda x: mse_value(x[1]))

    total_sum = squared_errors.sum()
    total_count = squared_errors.count()
    average = total_sum / total_count

    return average



def evaluable_distributed_training(context, user_ratings, model, num_workers, epochs):
    train_set, val_set, test_set = user_ratings.randomSplit([0.7, 0.1, 0.2], seed=42)
    test_set.unpersist()

    # (input, target) elephas rdd required format
    train_set_pairs = train_set.map(lambda x: (x, x))

    spark_ae_model = SparkModel(model, frequency='epoch', mode='synchronous', num_workers=num_workers)

    epochs = epochs
    train_losses = []
    val_losses = []

    for epoch in range(epochs):

        spark_ae_model.fit(train_set_pairs, epochs=1, batch_size=64, verbose=2, validation_split=0.0)

        train_recs = context.parallelize(spark_ae_model.predict(train_set))
        train_losses.append(calculate_avg_mse_loss(train_set, train_recs))
        train_recs.unpersist()

        val_recs = context.parallelize(spark_ae_model.predict(val_set))
        val_losses.append(calculate_avg_mse_loss(val_set, val_recs))
        val_recs.unpersist()

        print(f'epoch {epoch}:   tl: {train_losses[-1]:.5f}   vl: {val_losses[-1]:.5f}')

    return train_losses, val_losses
