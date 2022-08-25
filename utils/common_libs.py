import re
import gc
import os
import sys
import json
import pytz
import uuid
import hashlib
import logging
import requests
import itertools
import warnings
import numpy as np
import pandas as pd
import pyarrow as pa
from importlib import reload
from time import time, sleep
import pyarrow.parquet as pq
from dateutil.parser import parse
from datetime import datetime, timedelta, date, timezone
from kafka import KafkaProducer, KafkaConsumer, TopicPartition


#########_file proccessing procedures

class NpEncoder(json.JSONEncoder):
    """ Custom encoder for numpy data types """
    
    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):

            return int(obj)

        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)

        elif isinstance(obj, (np.complex_, np.complex64, np.complex128)):
            return {'real': obj.real, 'imag': obj.imag}

        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()

        elif isinstance(obj, (np.bool_)):
            return bool(obj)

        elif isinstance(obj, (np.void)): 
            return None
        
        elif isinstance(obj, datetime):
            return obj.isoformat()
        
        return json.JSONEncoder.default(self, obj)


def to_json(dic, fname, enc='utf-16'):
    """
    saving dicts to json
    """
        
    try:
        with open(fname+'.json', 'w',encoding=enc) as fp:
            json.dump(dic, fp, ensure_ascii=False)
        #print('Saving:',fname,'OK!')
    except TypeError:
        dcc={str(k):v for k,v in dic.items()}
        try:
            with open(fname+'.json', 'w',encoding=enc) as fp:
                json.dump(dcc, fp, cls=NpEncoder)
        except:
            print('Problem with type:',fname)
    except:
        print('Problem with saving:',fname)


def json_extract(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    return values


def json_extract_internal_levels(obj):
    """
    Recursively fetch values from nested JSON to single level.
    """
    
    dc = {}
    
    def extract(obj, dc, key):
        """
        Recursively search for values of key in JSON tree.
        """

        if isinstance(obj, dict):
            for k, v in obj.items():
                if key == '':
                    full_key = k.lower()
                else:
                    full_key = '_'.join(map(str, [key,k])).lower()
                
                if isinstance(v, dict):
                    extract(v, dc, full_key)
                elif isinstance(v, list):
                    dc[full_key] = []
                    for item in v:
                        if isinstance(item, dict):
                            extract(item, dc, full_key)
                        else:
                            dc[full_key].append(item)
                    if dc[full_key] == []:
                        del dc[full_key]
                else:
                    dc[full_key] = v if str(v).lower() not in ['none', 'nan','nat',''] else '0'
            
        elif isinstance(obj, list):
            dc[key] = []
            for item in obj:
                if isinstance(item, dict):
                    extract(item, dc, key)
                else:
                    dc[key].append(item)
            if dc[key] != []:
                del dc[key]
        
        else:
            dc[key] = obj if str(obj).lower() not in ['none', 'nan','nat', ''] else '0'
           
        return dc

    values = extract(obj, dc, '')

    return values


def parse_json(odj, key, check_existance_key = False, return_dict_values = False):
    """
    Recursively fetch values from nested JSON.
    if check_existance_key is true - return bool of existance definite key in obj
    if return_dict_values is true - return only list of dictionaries which is value of definite key in obj
    """
    
    ### init result list
    result = []
    
    def parse_json_recursively(json_object, res, target_key, che, ret_dc):
        
        if type(json_object) is dict and json_object:
            for key in json_object:
                if key == target_key and isinstance(json_object[key], dict) and ret_dc:
                    res.append(json_object[key])
                elif key == target_key and isinstance(json_object[key], list):
                    if json_object[key] == []:
                        continue
                    for val in json_object[key]:
                        if isinstance(val, list) and not ret_dc:                            
                            res += val
                        elif isinstance(val, dict) and not ret_dc:
                            continue
                        elif isinstance(val, dict) and ret_dc:
                            res.append(val)
                        elif not ret_dc:
                            res.append(val)
                elif key == target_key and not isinstance(json_object[key], dict) and not ret_dc:
                    res.append(json_object[key])
                elif key == target_key and che:
                    res.append(True)
                
                parse_json_recursively(json_object[key], res, target_key, che, ret_dc)
        elif type(json_object) is list and json_object:
            for item in json_object:
                parse_json_recursively(item, res, target_key, che, ret_dc)
        
        return res
    
    result = parse_json_recursively(odj, result, key, check_existance_key, return_dict_values)
    
    if check_existance_key:
        if result != []:return True
        else: return False
    
    return result


def json_validate(list_of_dc : list, schema : dict):
    """
    Check json to schema
    """
    
    errors = []
    try:
        validator = Draft3Validator(schema)
        errors = [i for i,x in enumerate(list_of_dc) if ErrorTree(validator.iter_errors(x))]
        
        return True, ','.join(map(str, errors))
    except Exception as err:
        return False, err        


def append_to_parquet_table(dataframe = pd.DataFrame(), **pars):
    """Method writes/append dataframes in parquet format.

    This method is used to write pandas DataFrame as pyarrow Table in parquet format. If the methods is invoked
    with writer, it appends dataframe to the already written pyarrow table.

    :param dataframe: pd.DataFrame to be written in parquet format.
    :param filepath: target file location for parquet file.
    :param writer: ParquetWriter object to write pyarrow tables in parquet format.
    :return: ParquetWriter object. This can be passed in the subsequenct method calls to append DataFrame
        in the pyarrow Table
    """
    
    if dataframe.empty:
        return 'empty dataset', False
    
    if pars['solo']:
        dataframe = pd.read_parquet(pars['filepath'])\
                      .merge(dataframe, how = 'outer')\
                      .drop_duplicates()
    
    num_rows, chk_rows = dataframe.shape[0], 0
    if pars['max_size'] > 0 and pars['max_size'] / num_rows < .95 and not pars['solo']:
        parts = np.int16(np.round(num_rows / pars['max_size']))
        for i in range(parts):
            
            if i == (parts - 1):
                if pars['schema']: table = pa.Table.from_pandas(dataframe.iloc[chk_rows : ], pars['schema'])
                else: table = pa.Table.from_pandas(dataframe)
            else:
                if pars['schema']: table = pa.Table.from_pandas(dataframe.iloc[chk_rows : chk_rows + pars['max_size']], pars['schema'])
                else: table = pa.Table.from_pandas(dataframe)
                
                chk_rows += pars['max_size']

            pq.write_to_dataset(table,\
                                root_path=pars['filepath'],\
                                use_deprecated_int96_timestamps = True,\
                                compression=pars['compression'])
    else:
        if pars['schema']: table = pa.Table.from_pandas(dataframe, pars['schema'])
        else: table = pa.Table.from_pandas(dataframe)
        
        if pars['solo']:
            writer = pq.ParquetWriter(pars['filepath'], table.schema)
            writer.write_table(table=table)
            writer.close()
        else:
            pq.write_to_dataset(table,\
                                root_path=pars['filepath'],\
                                use_deprecated_int96_timestamps = True,\
                                compression=pars['compression'])
    
    return '', True

###################################################################


def make_hash(*args):
    """
    procedure of hashing values in args
    """
    
    return hashlib.md5('-'.join(map(str, args)).encode()).hexdigest()


def make_guid(*args):
    """
    procedure of making GUID from values in args
    """
    
    return str(uuid.uuid5(uuid.NAMESPACE_OID, '-'.join(map(str, args))))

############## Connectors ####################################


class DBkafka:
    """
    KAFKA Database class.
    """

    def __init__(self, topic: str = '',
                 host: str = None,
                 username: str = None,
                 password: str = None, 
                 port: int = 9092,
                 tz: str = None, 
                 log_path: str = os.getcwd(),
                 bootstrap_servers: list = [],
                 log_level: str = 'error',
                 headers: dict = {},
                 consumer_offset_reset: str = 'latest', 
                 api_version: tuple = (2,10), 
                 security_protocol: str = 'PLAINTEXT',\
                 auth_mechanizm: str = 'PLAIN',
                 value_deserializer = None,
                 value_serializer = None):

        self.topic = topic
        self.timezone = tz
        self.headers = headers
        self.log_path = log_path
        self.offset_reset = consumer_offset_reset
        self.host = host
        self.bootstrap_servers = bootstrap_servers
        self.security_protocol = security_protocol
        self.api_version = api_version
        self.user = username
        self.password = password
        self.port = port
        self.batch_size = 1
        self.df_result = pd.DataFrame()
        self.dc_result = {}
        self.format_dt = '%Y-%m-%d %H:%M:%S'
        self.auth_mechanizm = auth_mechanizm
        self.value_deserializer = value_deserializer
        self.value_serializer = value_serializer
        self.conn_cons = None
        self.conn_prod = None
        self.log_level = log_level
        self.install_tz = True
        self.localtz = pytz.timezone(self.timezone) if self.timezone else pytz.timezone('Etc/GMT')
        self.log_levels = ["debug", "info", "warn", "error"]
        self._logger_init()

    
    def _logger_init(self):

        reload(logging)
        location = f'kafka_for_{self.host}' if self.bootstrap_servers == [] else f'kafka_for_{self.bootstrap_servers[0]}'
        
        logging.basicConfig(
            filename = os.path.join(self.log_path, f'{location}.log'),
            level = logging.INFO if self.log_level == 'info' \
                    else logging.ERROR if self.log_level == 'error' \
                    else logging.WARN if self.log_level == 'warn' \
                    else logging.DEBUG,
        )
        self.logger = logging.getLogger('KAFKA_BROKER')

    
    def log(self, tag: str = 'kafka-service', log_level: str = 'info', message: str = '', data: dict = {}) -> None:

        if self.log_levels.index(log_level) >= self.log_levels.index(self.log_level):
            log_info = {
                "level": log_level,
                "time": datetime.now(timezone.utc).isoformat(),
                "tag": tag,
                "message": message,
            }
            log_info.update(data)
            
            if log_level == 'info': self.logger.info(json.dumps(log_info, cls=NpEncoder))
            elif log_level == 'debug':self.logger.debug(json.dumps(log_info, cls=NpEncoder))
            elif log_level == 'warn': self.logger.warn(json.dumps(log_info, cls=NpEncoder))
            elif log_level == 'error':self.logger.error(json.dumps(log_info, cls=NpEncoder))
    

    def connect_producer(self):
        """
        Connect to a Kafka broker as Producer.
        """
        
        if self.headers == {}:
            self.headers['version'] = '-1'

        if self.conn_prod is None:
            try:
                self.conn_prod = KafkaProducer(
                                    bootstrap_servers=[f'{self.host}:{self.port}'] if self.bootstrap_servers == [] else self.bootstrap_servers,
                                    api_version=self.api_version,
                                    value_serializer = self.value_serializer,
                                    security_protocol=self.security_protocol,
                                    sasl_mechanism=self.auth_mechanizm,
                                    sasl_plain_username=self.user,
                                    sasl_plain_password=self.password,
                                    )
            except Exception as error:
                self.log('connection to Broker', 'error', f"Connection to Bootstrap {self.host}: {error}")

    
    def connect_consumer(self):
        """
        Connect to a Kafka broker as Consumer.
        """
        
        if self.conn_cons is None:
            try:
                self.conn_cons = KafkaConsumer(
                                    auto_offset_reset=self.offset_reset,
                                    bootstrap_servers=[f'{self.host}:{self.port}'] if self.bootstrap_servers == [] else self.bootstrap_servers,
                                    api_version=self.api_version,
                                    value_deserializer = self.value_deserializer,
                                    consumer_timeout_ms=1000,
                                    security_protocol=self.security_protocol,
                                    sasl_mechanism=self.auth_mechanizm,
                                    sasl_plain_username=self.user,
                                    sasl_plain_password=self.password,
                                    )
            except Exception as error:
                self.log('connection to Broker', 'error', f"Connection to Bootstrap {self.host}: {error}")
                

    def publish_message(self, key, value):

        
        self.connect_producer()

        try:
            #key = bytes(key, encoding='utf-8')
            #value = bytes(value, encoding='utf-8')
            partitions = [x for x in self.conn_prod.partitions_for(self.topic)]
            
            self.conn_prod.send(self.topic,\
                                key=key,\
                                value=value,\
                                headers=[(k,str(v).encode()) for k,v in self.headers.items()],\
                                partition=np.random.choice(partitions))
            self.conn_prod.flush()
        except Exception as error:
            self.log('publishing to Broker', 'error', f"Publishing message to topic {self.topic}: {error}")
                

    def publish_df(self, df, key):
        """
        dumping every batch of rows from df and publishing it to broker
        """

        df = self.change_format_to_str(df.copy())
        self.batch_size = self.batch_size if self.batch_size > 0 else 1
        nb = df.shape[0] // self.batch_size + 1 if self.batch_size > 1 else df.shape[0]
        
        for i in range(nb):

            if self.batch_size > 1:
                self.publish_message(key, \
                    json.dumps(df.iloc[i * self.batch_size : (i+1) * self.batch_size].to_dict(orient = 'records'), cls=NpEncoder))
            else:
                self.publish_message(key, \
                    json.dumps(df.loc[i, : ].to_dict(), cls=NpEncoder))
        
        self.log('publishing to Broker','info', f"Published {nb} message to topic {self.topic}")
        self.close_prod()
    

    def publish_array(self, ls, key):
        """
        dumping every batch of elements from arrays and publishing it to broker
        """

        self.batch_size = self.batch_size if self.batch_size > 0 else 1
        nb = len(ls) // self.batch_size + 1
        
        for i in range(nb):

            self.publish_message(key, \
                json.dumps(ls[i * self.batch_size : (i + 1) * self.batch_size], cls=NpEncoder))
        
        self.log('publishing to Broker','info', f"Published {nb} message to topic {self.topic}")
        self.close_prod()
    

    def reading_que(self, partition_offset : dict, to_frame = True):
        """
        messages from que for transform to dict or dataframe
        """

        self.connect_consumer()
        self.dc_result, new_partition_offset = {}, {}
        self.df_result = pd.DataFrame()

        try:
            partitions = self.conn_cons.partitions_for_topic(self.topic)
            
            for p in partitions:
                tp = TopicPartition(self.topic, p)
                self.conn_cons.assign([tp])
                self.conn_cons.seek_to_beginning(tp)
                if p in partition_offset.keys():
                    if self.conn_cons.beginning_offsets([tp])[tp] <= partition_offset[p]:
                        self.conn_cons.seek(tp, int(partition_offset[p]))
                offset = self.conn_cons.position(tp)

                self.log('consuming from Broker','info', f"Start consuming partition {p} in topic {self.topic} from offset {offset}")
                
                for msg in self.conn_cons:
                    self.dc_result[f"{msg.partition}_{msg.offset}"] = {}
                    self.dc_result[f"{msg.partition}_{msg.offset}"]['key'] = msg.key if msg.key is None else msg.key.decode()
                    self.dc_result[f"{msg.partition}_{msg.offset}"]['value'] = msg.value
                    self.dc_result[f"{msg.partition}_{msg.offset}"]['date'] = datetime.fromtimestamp(msg.timestamp/1000)
                    self.dc_result[f"{msg.partition}_{msg.offset}"]['header'] = msg.headers

                new_partition_offset[p] = self.conn_cons.position(tp)

            if to_frame:
                self.df_result = pd.DataFrame(self.dc_result).T.reset_index()
        except Exception as error:
            new_partition_offset = partition_offset
            self.log('consuming from Broker','error', f"reading que from topic {self.topic}: {error}")
            
        self.close_cons()
        
        return new_partition_offset
        
    
    def que_to_df(self, start_date):
        """
        messages from que for transform to pandas DF.
        """

        self.connect_consumer()
        self.df_result = pd.DataFrame()

        if isinstance(start_date, datetime):

            try:
                for msg in self.conn_cons:
                    n = len(self.df_result)
                    date_index = datetime.fromtimestamp(msg.timestamp/1000)
                    if date_index >= start_date:
                        self.df_result.loc[n, 'date'] = date_index
                        self.df_result.loc[n, 'key'] = msg.key.decode()
                        self.df_result.loc[n, 'value'] = msg.value.decode()
            except Exception as error:
                self.log('query in broker','error', f"reading que from topic {self.topic}: {error}")
            
        self.close_cons()
       
    
    def close_cons(self):

        if self.conn_cons:
            self.conn_cons.close()
            self.conn_cons = None


    def close_prod(self):