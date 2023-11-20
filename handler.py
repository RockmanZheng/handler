import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects import (mysql,sqlite,oracle,mssql,postgresql)
import os.path
import json
import yaml
import h5py
import logging
import datetime
import pymongo
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
file_handler = logging.FileHandler('handler.log')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)



class Handler(object):
    def __init__(self,
                 config_path: str):
        self.config_path = config_path
        if config_path is not None:
            self.configs = self.local_load(config_path)
        else:
            self.configs = None

    def hdf5_to_dict(self,
                    file_path: str) -> dict:
        """
        Utility function that parses hdf5 file
        """
        data_dict = {}
        data_dict['attrs'] = {}
        def parse_hdf5_obj(obj, parent_key=''):
            if isinstance(obj, h5py.Dataset):
                data_dict[parent_key] = obj[()]
            elif isinstance(obj, h5py.Group):
                for key in obj.keys():
                    parse_hdf5_obj(obj[key], parent_key + '/' + key)

        with h5py.File(file_path, 'r') as file:
            for name,value in file.attrs.items():
                data_dict['attrs'][name]=value
            parse_hdf5_obj(file)

        return data_dict
    
    def local_load(self,
             path: str) -> dict:
        """
        Load simple configuration file and data files from local storage
        """
        ext = os.path.splitext(path)[1]
        if ext == '.json':
            with open(path,'r') as file:
                data = json.load(file)
        elif ext == '.yaml' or ext == '.yml':
            with open(path,'r') as file:
                data = yaml.safe_load(file)
        elif ext == '.hd5' or ext == '.h5' or ext == '.hdf5':
            data = self.hdf5_to_dict(path)
        else:
            raise ValueError('Unsupported file format: %s' % (path))
        
        return data

    def local_dump(self,
                   path: str):
        ext = os.path.splitext(path)[1]
        if ext == '.json':
            with open(path,'w') as file:
                json.dump(self.configs,file)
        elif ext == '.yaml' or ext == '.yml':
            with open(path,'w') as file:
                yaml.dump(self.configs,file)
        else:
            raise ValueError('Unsupported file format: %s' % (path))
        

class DatabaseHandler(Handler):
    """
    Base class for relational database handler
    """
    def __init__(self,
                 config_path: str = None,
                 csv_path: str = None,
                 table_name: str = None):
        """
        Parameters:
            - config_path (str): path to configuration file
            - csv_path (str): path to csv file, open the csv file and create a corresponding sqlite database
            - table_name (str): the name of the table if reading from csv file 
        Configs:
            - 'engine' (str): Database engine
            - 'connector' (str): Database connector
            - 'user' (str): User name
            - 'password' (str): Password
            - 'host' (str): Database server address
            - 'database' (str): Which database to connect
        """
        Handler.__init__(self,config_path=config_path)
        if csv_path is not None:
            self.engine = create_engine("sqlite:///%s"%(self.local_path))
            if table_name is None:
                name, _ = os.path.splitext(csv_path)
            else:
                name = table_name
            self.read_csv(path=csv_path,
                          table=name)
        else:
            if self.dialect=='unspecified':
                self.engine = create_engine("sqlite:///%s"%(self.local_path))
            elif self.dialect == 'sqlite':
                self.engine = create_engine("sqlite:///%s.db"%(self.configs['database']))
            elif self.dialect in ['mysql','postgresql','oracle','mssql']:
                if self.port is not None:
                    self.engine = create_engine("%s+%s://%s:%s@%s:%s/%s"%
                                                (self.configs['engine'],
                                                self.configs['connector'],
                                                self.configs['user'],
                                                self.configs['password'],
                                                self.host,
                                                self.port,
                                                self.database))
                else:
                    self.engine = create_engine("%s+%s://%s:%s@%s/%s"%
                                                (self.configs['engine'],
                                                self.configs['connector'],
                                                self.configs['user'],
                                                self.configs['password'],
                                                self.host,
                                                self.database))
            else:
                raise ValueError('Unsupported database engine: %s'%(self.configs['engine']))
    

    @property
    def dialect(self):
        if self.configs is None:
            return 'unspecified'
        return self.configs['engine']

    @property
    def local_path(self):
        return 'local.db'

    @property
    def host(self):
        if self.configs is not None and 'host' in self.configs.keys():
            return self.configs['host']
        else:
            return '.'
    
    @property
    def database(self):
        if self.configs is not None:
            return self.configs['database']
        else:
            return self.local_path
    
    @property
    def port(self):
        if self.configs is not None and 'port' in self.configs.keys():
            return self.configs['port']
        else:
            return None
    

    
    def to_csv(self,
               path: str,
               table: str,
               primary_key: str = 'id'):
        df = pd.read_sql_table(table_name=table,
                               con = self.engine)
        df.to_csv(path_or_buf=path,index=True,index_label=primary_key)
    
    def read_csv(self,
                 path: str,
                 table: str):
        """
        Read csv file in path into database table, replacing the existing table
        """
        if self.port is not None:
            logger.info('Loading csv %s into database %s:%s/%s at table %s'%(path,self.host,self.port,self.database,table))
        else:
            logger.info('Loading csv %s into database %s/%s at table %s'%(path,self.host,self.database,table))
        df = pd.read_csv(filepath_or_buffer=path)
        df.to_sql(name=table,
                  con=self.engine,
                  if_exists='replace',
                  chunksize=100)
    
    def exec_sql(self,
                 sql: str,
                 index_col: str|list[str] = None):
        if self.port is not None:
            logger.info('Downloading from database %s:%s/%s'%(self.host,self.port,self.database))   
        else:
            logger.info('Downloading from database %s/%s'%(self.host,self.database))   
        with self.engine.connect() as cnx:
            data = pd.read_sql(sql=sql,
                               con=cnx,
                               index_col=index_col)
        return data


    def write(self,
                data: dict,
                table: str):
        """
        Upload data 

        Oracle/MSSQL timestamp to millisecond not supported
        """
        if self.port is not None:
            logger.info('Uploading to database %s:%s/%s'%(self.host,self.port,self.database))
        else:
            logger.info('Uploading to database %s/%s'%(self.host,self.database))
        df = pd.DataFrame(data)

        # construct type dictionary for timestamp
        type_dict = {}
        for key in data.keys():
            if isinstance(data[key][0],datetime.datetime):
                if self.dialect == 'mysql':
                    type_dict[key] = mysql.TIMESTAMP(fsp=3)
                elif self.dialect == 'postgresql':
                    type_dict[key] = postgresql.TIMESTAMP(precision=3)
                elif self.dialect in ['unspecified','sqlite']:
                    type_dict[key] = sqlite.DATETIME(storage_format = "%(year)04d-%(month)02d-%(day)02d %(hour)02d:%(minute)02d:%(second)02d.%(microsecond)03d")
                elif self.dialect == 'oracle':
                    type_dict[key] = oracle.TIMESTAMP()
                elif self.dialect == 'mssql':
                    type_dict[key] = mssql.TIMESTAMP()

        df.to_sql(name=table,
                  con=self.engine,
                  if_exists='append',
                  index=False,
                  chunksize=100,
                  method=None,
                  dtype=type_dict)

class Session(object):
    """
    Each session can hold multiple handlers, 
    and is in charge of the connection to the database, 
    setting up the session by reading the configuration file
    """
    def __init__(self,
                 config_path: str):
        self.handlers = {}
        self.handlers['config'] = Handler(config_path=config_path)
        if not self.use_local_db:
            self.handlers['database'] = DatabaseHandler(config_path=self.configs['database'])
        else:# use local database instead
            self.handlers['database'] = DatabaseHandler()
    
    @property
    def use_local_db(self):
        return not 'database' in self.configs.keys()

    @property
    def configs(self):
        return self.handlers['config'].configs
    
    @property
    def database(self):
        return self.handlers['database']

    def run(self,**kwarg):
        pass

    def close(self):
        pass

class DataWriter(object):
    """
    Base class for stream data uploading
    Could be very slow
    Suitable for low throughput setting, e.g., 
    recording data obtained from a websocket which transmits
    data only at a fixed frequency like 100ms
    """
    def __init__(self,
                 handler: Handler,
                 table: str,
                 capacity: int = 100):
        """
        Parameters:
            - storage (str): 'csv' or 'database'
            - table (str): the name of the storage table
        """
        self._handler = handler
        self._buf_capacity = capacity
        self._table = table
        self._buffer = {}
        self._reset_buffer()
    
    def _reset_buffer(self):
        """
        Override this function to initialize the list of keys of the buffer
        Remember to reset self._buf_ptr to 0
        """
        raise NotImplementedError('Buffer resetter not implemented.')
    
    def _append_buffer(self,
                       data: dict):
        """
        Override this function to append data into the buffer
        """
        for key in data.keys():
            if key in self._buffer.keys():# truncate data if the column name does not exist in buffer
                self._buffer[key].append(data[key])

    @property
    def _buf_size(self):
        if len(self._buffer.keys())>0:
            key = list(self._buffer.keys())[0]
            return len(self._buffer[key])
        else:
            return 0
    
    def flush(self):
        """
        Flush data if the buffer is not empty
        """
        if self._buf_size > 0:
            self._handler.write(self._buffer,self._table)# flush all data in buffer to the destination
            self._reset_buffer()# clear buffer

    def write(self,
               data: dict):
        """
        Interface for uploading data
        
        Parameters:
            - data (dict): one data entry encapsulated as a dictionary
        """
        self._append_buffer(data)
        if self._buf_size >= self._buf_capacity:# when the buffer is full
            self.flush()# flush all data to the destination

class DocumentStore(Handler):
    """
    Base class for NoSQL document store handler
    """
    def __init__(self,
                 config_path: str = None):
        Handler.__init__(self,config_path=config_path)
        if self.engine == 'mongodb':
            client = pymongo.MongoClient("mongodb://%s:%s"%(self.host, self.port))
        self.store = client[self.database]
    
    def write(self,
              document: dict,
              collection: str):
        """
        Write a document to the specified collection
        """
        logger.info("Uploading to document store %s://%s/%s"%(self.engine,self.database,collection))
        table = self.store[collection]
        table.insert_one(document=document)
    
    
    @property
    def engine(self):
        return self.configs['engine']

    # @property
    # def user(self):
    #     return self.configs['user']
    
    # @property
    # def password(self):
    #     return self.configs['password']
    
    @property
    def port(self):
        return self.configs['port']
    
    @property
    def host(self):
        return self.configs['host']
    
    @property
    def database(self):
        return self.configs['database']

class EmailHandler(object):
    def __init__(self,
                 account: str,
                 password: str):
        self.sender = account
        if 'gmail' in account:
            self.smtp_server = 'smtp.gmail.com'
            self.smtp_port = 587
            self.password = password

    def send(self,
             receiver: str,
             subject: str,
             text: str):
        msg = MIMEMultipart()
        msg['From'] = self.sender
        msg['To'] = receiver
        msg['Subject'] = subject
        msg.attach(MIMEText(text, 'plain'))
        try:
            # Create a secure connection to the SMTP server
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            
            # Login to your email account
            server.login(self.sender, self.password)
            
            # Send the email
            server.sendmail(self.sender, receiver, msg.as_string())
            logger.info('Email sent successfully!')
            
        except Exception as e:
            logger.error('An error occurred while sending the email: %s'% e)
            
        finally:
            # Close the SMTP server connection
            server.quit()


