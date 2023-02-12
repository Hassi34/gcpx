import numpy as np
from tensorflow import feature_column as fc
import tensorflow as tf

class tfTabularFeatureEng:
    def __init__(self, df:object, target:str, num_features: list=None, cols_to_bucketize: list=None, cat_features: list=None):
        self.df = df.copy()
        self.labels = self.df.pop(target)
        self.feature_cols = []

        if num_features is None:
            self.num_features = self.df.select_dtypes(include=np.number).columns.tolist()
        else:
            self.num_features = num_features 

        if cat_features is None:
            self.cat_features = self.df.select_dtypes(include=['object', 'category']).columns.tolist()
        else:
            self.cat_features = cat_features
        
        if cols_to_bucketize:
            self.bucketized_cols = cols_to_bucketize

    def df_to_dataset(self, shuffle=True, batch_size=32):
        ds = tf.data.Dataset.from_tensor_slices((dict(self.df), self.labels))
        if shuffle:
            ds = ds.shuffle(buffer_size=len(self.df))
        ds = ds.batch(batch_size)
        return ds

    def __get_scal(self, feature):
        def minmax(x):
            mini = self.df[feature].min()
            maxi = self.df[feature].max()
            return (x - mini)/(maxi-mini)
            return(minmax)

    def transform_num_features(self):
        for header in self.num_features:
            scal_input_fn = self.__get_scal(header)
            self.feature_cols.append(fc.numeric_column(header,
                                                    normalizer_fn=scal_input_fn))
    def transform_cat_features(self):
        for feature_name in self.cat_features:
            vocabulary = self.df[feature_name].unique()
            categorical_c = fc.categorical_column_with_vocabulary_list(feature_name, vocabulary)
            one_hot = fc.indicator_column(categorical_c)
            self.feature_cols.append(one_hot)
    
    def bucketize(self, cols: list=None):
        for col in self.bucketized_cols:
            col_to_bucketize = fc.numeric_column(col)
            buckets = fc.bucketized_column(col_to_bucketize, boundaries=np.arange(0, 110, 10).tolist())
            self.feature_cols.append(buckets)
    
    def feature_cross(self, cat_col:str, bucketized_col:str):
        vocabulary = self.df[cat_col].unique()
        ocean_proximity = fc.categorical_column_with_vocabulary_list(cat_col,
                                                                     vocabulary)
        col_to_bucketize = fc.numeric_column(bucketized_col)
        buckets = fc.bucketized_column(col_to_bucketize, boundaries=np.arange(0, 110, 10).tolist())

        crossed_feature = fc.crossed_column([buckets, ocean_proximity],
                                            hash_bucket_size=1000)
        crossed_feature = fc.indicator_column(crossed_feature)
        
        self.feature_cols.append(crossed_feature)
            