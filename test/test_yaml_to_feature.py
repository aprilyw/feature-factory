import unittest
from framework.feature_factory.feature import Feature, FeatureSet, CompositeFeature
from framework.feature_factory.feature_dict import ImmutableDictBase
from framework.feature_factory import Feature_Factory
from framework.feature_factory.helpers import Helpers
import pyspark.sql.functions as f
from pyspark.sql.functions import col, sum, count_distinct
from pyspark.sql.types import StructType
from test.local_spark_singleton import SparkSingleton
import json
import yaml

class CommonFeatures(ImmutableDictBase):
    def __init__(self):
        self._dct["customer_id"] = Feature(_name="customer_id", _base_col=f.col("ss_customer_sk"))
        self._dct["trans_id"] = Feature(_name="trans_id", _base_col=f.concat("ss_ticket_number","d_date"))

    @property
    def collector(self):
        return self._dct["customer_id"]

    @property
    def trans_id(self):
        return self._dct["trans_id"]


class Filters(ImmutableDictBase):
    def __init__(self):
        self._dct["valid_sales"] = f.col("ss_net_paid") > 0

    @property
    def valid_sales(self):
        return self._dct["valid_sales"]


class StoreSales(CommonFeatures, Filters):
    def __init__(self):
        self._dct = dict()
        CommonFeatures.__init__(self)
        Filters.__init__(self)

        self._dct["total_quants"] = Feature(_name="total_quants",
                                           _base_col=f.col("ss_quantity"),
                                           _filter=[],
                                           _negative_value=None,
                                           _agg_func=f.sum)

        self._dct["total_sales"] = Feature(_name="total_sales",
                                           _base_col=f.col("ss_net_paid").cast("float"),
                                           _filter=self.valid_sales,
                                           _negative_value=0,
                                           _agg_func=f.sum)

        self._dct["sales_per_quants"] = (self.total_sales / self.total_quants).withName("sales_per_quants")


    @property
    def total_sales(self):
        return self._dct["total_sales"]

    @property
    def total_quants(self):
        return self._dct["total_quants"]

    @property
    def sales_per_quants(self):
        return self._dct["sales_per_quants"]

class TestFeatureDict(unittest.TestCase):
    def setUp(self):
        with open("test/data/sales_store_schema.json") as f:
            sales_schema = StructType.fromJson(json.load(f))
            self.sales_df = SparkSingleton.get_instance().read.csv("test/data/sales_store_tpcds.csv", schema=sales_schema, header=True)
    def test_features_from_yaml(self):
        with open("channelDemoStore/sales_features.yaml") as file:
            data = yaml.safe_load(file)
 
        with open("test/data/sales_store_schema.json") as f:
            sales_schema = StructType.fromJson(json.load(f))
            sales_enhanced_df = SparkSingleton.get_instance().read.csv("test/data/tomes_tpcds_delta_1tb_store_sales_enhanced.csv", schema=sales_schema, header=True)
 
        print(data)
 
        myFeatureSet = FeatureSet()
        for d in data:
            filter = eval(d['filter']) if d['filter'] else []
            myFeatureSet.add_feature(Feature(_name=d['name'],
                                            _base_col=eval(d['base_col']),
                                            _filter=filter,
                                            _negative_value=d['negative_value'],
                                            _agg_func=eval(d['agg_func'])))
 
        ff = Feature_Factory()
        df = ff.append_features(sales_enhanced_df, ["ss_customer_sk"], [myFeatureSet])