'''
Created on 2012-12-18

@author: Ransom
'''

from hive import ThriftHive
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from locale import str

class HiveClient:
    """
    hive thrift client
    """
    
    transport = None
    client = None
    def __init__(self):
        pass
    
    def openTranslation(self, host="localhost", port=10000):
        """
            open thrift translation
        """
        try:
            self.transport = TSocket.TSocket(host, port)
            self.transport = TTransport.TBufferedTransport(self.transport)
            protocol = TBinaryProtocol.TBinaryProtocol(self.transport)

            self.client = ThriftHive.Client(protocol)
            self.transport.open()
            print "success connect to " + host + " " + str(port)
        except Exception, tx:
            self.reset()
            print '%s' % (tx.message)
            
    def closeTranslation(self):
        """
            close thrift translation
        """
        
        try:
            if self.client is not None:
                self.client.clean()
            if self.transport is not None:
                self.transport.close()
        except Exception, tx:
            self.reset()
            print '%s' % (tx.message)
            
    def excuteAndPrint(self, sql):
        """
            excute sql and print result
        """
        
        if(self.client is None):
            print "client is null, translation should be opened."
            return
            
        try:   
            self.client.execute(sql)
            self.resultPrint()   
        except Exception, tx:
            print '%s' % (tx.message)
        finally:
            self.client.clean()
   
   
    def resultPrint(self):
        try:  
            cols = self.getColums()
            if cols is not None:
                print cols
                
            while (1):
                row = self.client.fetchN(10)
                if (row == []):
                    break
                for i in range(0, len(row)):
                    if row[i] is not None:
                        print row[i]  
        except Exception, tx:
            print '%s' % (tx.message)
        finally:
            self.client.clean()            
                           
    def getColums(self):    
        cols = ""
        schemas = self.client.getSchema().fieldSchemas
        
        if schemas is None:
            return None
        
        for i in range(0, len(schemas)):
            if i is not 0:
                cols = '%s\t%s' % (cols, schemas[i].name)  
            else:
                cols = '%s%s' % (cols, schemas[i].name)   
                    
        return cols   
            
    def reset(self):
        self.client = None
        self.transport = None
        
    def isOpen(self):
        try:
            if self.client is not None and self.transport is not None:
                return self.transport.isOpen()
        except Exception, tx:
            print '%s' % (tx.message) 
            return False
        
if __name__ == '__main__':
    client = HiveClient()
    client.openTranslation("localhost", 10000)
    client.excuteAndPrint("show tables")
    client.closeTranslation()
    
