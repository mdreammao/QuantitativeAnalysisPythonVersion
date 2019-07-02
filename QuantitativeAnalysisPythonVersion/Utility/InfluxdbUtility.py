import pandas as pd
import influxdb 
from Config.myConfig import InfluxdbServer,logger
########################################################################
class InfluxdbUtility(object):
    """influxdb辅助工具"""
    #----------------------------------------------------------------------
    @classmethod 
    def saveDataFrameDataToInfluxdb(self,data:pd.DataFrame,database:str,measurement:str,tag:dict,connet=InfluxdbServer):
        client = influxdb.DataFrameClient(host=connet['host'], port=connet['port'], username=connet['username'], password=['password'], database='')
        dbs = client.get_list_database()
        if ({'name':database} in dbs)==False:
            client.create_database(database)
        try:
            client.write_points(dataframe=data,
                    database=database,
                    measurement=measurement,
                    tags=tag,
                    field_columns=list(data.columns),
                    protocol='line')
        except Exception as excp:
            logger.error(f'save data to influxdb {measurement} in {tag} error! {excp}')
        pass
########################################################################
