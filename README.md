# Big Data project
### Autoencoder-based Recommendation System: Integrating Spark and Deep Learning models
##### Development of a autoencoder based recommender system within Apache Spark cluster environment. Project includes data manipulation. The distributed training of the model is performed using [Elephas](https://github.com/danielenricocahall/elephas) library. [MovieLens](https://grouplens.org/datasets/movielens/) is the dataset used.
&nbsp;
&nbsp;

**Files:**
+ **big data report.pdf:** project report including explanation and analysis of performance.
+ **spark_autoencoder_rec.ipynb:** notebook including the entire pipeline of the project. There is the possibility of recording CPU and RAM performance.
+ **spark_autoencoder_rec - no_monitor.ipynb:** notebook same as the previous one but without code segments to record performance to make the code more readable.
+ **resources_monitor.py**: monitor that records resource usage at regular intervals.
+ **evaluable_training.py:** script that I created to conduct distributed training from epoch to epoch in order to record the loss values. Elephas does not return losses at the end of training.
&nbsp;
&nbsp;

**Folders**
+ **trained AEs:** saved models obtained from the training phase.
+ **movie_lens_stats:** collects the csv files produced by resources_monitor.py. They are grouped by project phase.
+ **losses:** losses recorded using evaluable_training.py.
+ **ml-25m:** where the dataset should be stored.

