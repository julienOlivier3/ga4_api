import pandas as pd
import numpy as np
import os
import datetime
from typing import List, Tuple
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (Dimension, Metric, DateRange, Metric, OrderBy, 
                                               FilterExpression, MetricAggregation, CohortSpec, MetricType)
from google.analytics.data_v1beta.types import RunReportRequest, RunRealtimeReportRequest, GetMetadataRequest

__author__ = 'Julian Doerr'
__version__ = 'v1.1.0'

class GA4Exception(Exception):
    '''base class for GA4 exceptions'''

class GA4RealTimeReport:
    """class to query GA4 real time report
    More information: https://support.google.com/analytics/answer/9271392?hl=en
    """

    def __init__(self, property_id):
        self.property_id = property_id
        self.client = BetaAnalyticsDataClient()

    def query_report(self, dimensions: List[str], metrics: List[Metric], 
        row_limit: int=10000, quota_usage: bool=False):
        """
        :param dimensions: categorical attributes (age, country, city, etc)
        :type dimensions: [dimension type]
        :param metrics: numeric attributes (views, user count, active users)
        :type metrics: [metric type]
        """
        try:
            dimension_list = [Dimension(name=dim) for dim in dimensions]
            metrics_list = [Metric(name=m) for m in metrics]
            report_request = RunRealtimeReportRequest(
                property=f'properties/{self.property_id}',
                dimensions=dimension_list,
                metrics=metrics_list,
                limit=row_limit,
                return_property_quota=quota_usage,

            )
            response = self.client.run_realtime_report(report_request)
     
            output = {}
            if 'property_quota' in response:
                output['quota'] = response.property_quota

            # construct the dataset
            headers = [header.name for header in response.dimension_headers] + [header.name for header in response.metric_headers]
            rows = []
            for row in response.rows:
                rows.append(
                    [dimension_value.value for dimension_value in row.dimension_values] + \
                    [metric_value.value for metric_value in row.metric_values])            
            output['headers'] = headers
            output['rows'] = rows
            return output
        except Exception as e:
            raise GA4Exception(e)


class GA4Report:
    def __init__(self, property_id):
        self.property_id = property_id
        self.client = BetaAnalyticsDataClient()

    def query_report(self, dimensions: List[str], metrics: List[Metric], date_ranges: list=[('90daysAgo', 'today')],
        offset_row: int=0, row_limit: int=10000, keep_empty_rows: bool=True, quota_usage: bool=False):
        """Returns a customized report of your Google Analytics event data.
        :param dimensions: categorical attributes (age, country, city, etc)
        :type dimensions: [dimension type]
        :param metrics: numeric attributes (views, user count, active users)
        :type metrics: [metric type]
        :param start_date: The inclusive start date for the query in the format YYYY-MM-DD.
        :param end_date: The inclusive end date for the query in the format YYYY-MM-DD.
        """
        try:
            dimension_list = [Dimension(name=dim) for dim in dimensions]
            metrics_list = [Metric(name=m) for m in metrics]
            # date_range = DateRange(start_date=start_date, end_date=end_date)
            date_ranges = [DateRange(start_date=date_range[0], end_date=date_range[1]) for date_range in date_ranges]

            report_request = RunReportRequest(
                property=f'properties/{self.property_id}',
                dimensions=dimension_list,
                metrics=metrics_list,
                limit=row_limit,
                return_property_quota=quota_usage,
                date_ranges=date_ranges,
                offset=offset_row,
                keep_empty_rows=keep_empty_rows
            )
            response = self.client.run_report(report_request)
     
            output = {}
            if 'property_quota' in response:
                output['quota'] = response.property_quota

            # construct the dataset
            headers = [header.name for header in response.dimension_headers] + [header.name for header in response.metric_headers]
            rows = []
            for row in response.rows:
                rows.append(
                    [dimension_value.value for dimension_value in row.dimension_values] + \
                    [metric_value.value for metric_value in row.metric_values])            

            output['headers'] = headers
            output['rows'] = rows
            output['row_count'] = response.row_count
            output['metadata'] = response.metadata
            output['response'] = response
            return output            
        except Exception as e:
            raise GA4Exception(e)

    def return_df(self, dimensions: List[str], metrics: List[Metric], date_ranges: list=[('90daysAgo', 'today')],
        offset_row: int=0, row_limit: int=10000, keep_empty_rows: bool=True, quota_usage: bool=False):
    
        response = self.query_report(dimensions, metrics, date_ranges, offset_row, row_limit, keep_empty_rows, quota_usage)
        response = response["response"]

        # Row index
        row_index_names = [header.name for header in response.dimension_headers]
        row_header = []
        for i in range(len(row_index_names)):
            row_header.append([row.dimension_values[i].value for row in response.rows])

        row_index_named = pd.MultiIndex.from_arrays(np.array(row_header), names = np.array(row_index_names))
        
        # Row flat data
        metric_names = [header.name for header in response.metric_headers]
        data_values = []
        for i in range(len(metric_names)):
            data_values.append([row.metric_values[i].value for row in response.rows])

        df = pd.DataFrame(data = np.transpose(np.array(data_values, dtype = 'f')), 
                            index = row_index_named, columns = metric_names).reset_index()
        
        return df


class GA4Metadata:
    def __init__(self, property_id):
        self.property_id = property_id
        self.client = BetaAnalyticsDataClient()

    def query_report(self):
        try:
            report_request = GetMetadataRequest(
                name=f'properties/{self.property_id}/metadata'
            )
            response = self.client.get_metadata(report_request)
            return response    
        
        except Exception as e:
            raise GA4Exception(e)

    def return_df(self):
    
        response = self.query_report()

        output =[]
        for dimension in response.dimensions:
            output.append({"Type": "Dimension", "API_Name": f"{dimension.api_name}", "UI_Name": f"{dimension.ui_name}", "Description": f"{dimension.description}", "Custom_definition": f"{dimension.custom_definition}", "Metric_type": "N/A"})

        for metric in response.metrics:
            output.append({"Type": "Metric", "API_Name": f"{metric.api_name}", "UI_Name": f"{metric.ui_name}", "Description": f"{metric.description}", "Custom_definition": f"{metric.custom_definition}", "Metric_type": f"{MetricType(metric.type_).name}"})
    
        df = pd.DataFrame(output)
        
        return df