import unittest 
import requests
import time
import os

port = "13800"
ip = "localhost"
class causal_test(unittest.TestCase):

    def test_causal1():
        response = requests.put ('http://%s:%s/kvs/keys/%s'%(ip,port,"test"))
        print (response)






if _name == 'main':
    unittest.main()