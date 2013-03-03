'''
Hive python client tools console.
recive console input and deal it.
Created on 2012-12-30
@author: Ransom
'''
from pytools.HiveClient import HiveClient

class PyConsole:
    """
    console input and output
    """
    
    hiveclient = None  # hive operator client 
    sql = ""  # the sql will to be excute.
    opTime = 0  # console tips about operator time.
    
    def __init__(self):
        self.hiveclient = HiveClient()
        
    def initConsole(self):
        self._help()
        print "waiting console input..."
        self.consoleTip()
        while True:
            text = raw_input()  
            if text == 'help':
                self.resetsql()
                self._help()
                self.consoleTip()
                
            elif text.startswith("open"):
                self.resetsql()
                arr = text.split(" ")
                args = []
                for index, item in enumerate(arr):
                    if item is not None and len(item) != 0 and index != 0:
                        args.append(item)
                print "translation will open at:", args
                if len(args) == 1:
                    self.hiveclient.openTranslation(args[0])
                elif len(args) == 2:
                    self.hiveclient.openTranslation(args[0], args[1])
                elif len(args) == 0:
                    self.hiveclient.openTranslation()    
                else:
                    print "open args: [host] [port]"
                self.consoleTip()
                    
            elif text == 'exit':
                self.resetsql()
                if self.hiveclient.isOpen():
                    self.hiveclient.closeTranslation()
                    exit
                    
            elif text == 'close':
                self.resetsql()
                if self.hiveclient.isOpen():
                    self.hiveclient.closeTranslation()
                self.consoleTip()    
            
            elif text.endswith(";"):
                self.sql = '%s %s' % (self.sql, text[0:len(text) - 1])  
                print "will excute sql:", self.sql
                self.hiveclient.excuteAndPrint(self.sql)
                self.resetsql()
                self.consoleTip()
            
            elif text is not None:
                self.sql = '%s %s' % (self.sql, text)  
            else:
                continue
    
    def resetsql(self):
        self.sql = ""
        
    def consoleTip(self):
        self.opTime += 1
        print "HivePyClient_" + str(self.opTime) + ">",
                 
    def _help(self):
        print "exit: exit hive python client."
        print "open host [port] : open translation from hive server."
        print "close: close translation from hive server."
        print "help: get help context."
        print "write sql to excute it direct after connection opened. and the result will be print in console."
                    

if __name__ == '__main__':
    console = PyConsole()
    console.initConsole()
