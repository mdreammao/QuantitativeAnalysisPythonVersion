import pymssql
########################################################################
class SqlServerUtility(object):
    """description of class"""
     #----------------------------------------------------------------------
    def __init__(self,address='(local)',user='sa',password='maoheng0',database=EMPTY_STRING,table=EMPTY_STRING):
        self.address=address
        self.user=user
        self.password=password
        self.database=database
        self.table=table
        self.connect=pymssql.connect( self.address,self.user,self.password,self.datablase,charset='utf8')
    #----------------------------------------------------------------------
    

########################################################################