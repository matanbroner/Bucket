import unittest 
import requests
import json
import time
import os

update_resp= "Updated successfully"
add_resp = "Added successfully"
get_resp = "Retrieved successfully"

get_error_resp = "Key does not exist"
unable_error_resp = "Unable to satisfy request"

get_error_msg = "Error in GET"
put_error_msg = "Error in PUT"
del_error_msg = "Error in DEL"
class causal_test(unittest.TestCase):

    
    def put_request(self,port,key,val,context):
       return requests.put ('http://localhost:%s/kvs/keys/%s'%(port,key),
                            json = {'value':val,'causal-context':context},
                            headers = {"Content-Type": "application/json"}) 

    def get_request(self,port,key,context):
       return requests.get ('http://localhost:%s/kvs/keys/%s'%(port,key),
                            json = {'causal-context':context},
                            headers = {"Content-Type": "application/json"}) 
                            
    def test_causal_1(self):
        # Write X to R1
        response = self.put_request("13801","x","1","")
        contents = response.json()
        self.assertEqual(contents["message"],add_resp)
        self.assertEqual(response.status_code,201)
        # Read X from R2 with causal-context from prev write. Should Result in error
        response= self.get_request("13802","x",contents["causal-context"])
        contents2 = response.json()
        self.assertEqual(contents2["message"],put_error_msg)
        self.assertEqual(contents2["error"],unable_error_resp)
        self.assertEqual(response.status_code,400)

    def test_causal_2(self):
        # Write X to R1
        response = self.put_request("13801","x","1","")
        contents = response.json()
        self.assertEqual(contents["message"],add_resp)
        self.assertEqual(response.status_code,201)
        # Read X from R2 without causal-context. Should Result in key DNE
        response= self.get_request("13802","x","")
        contents2 = response.json()
        self.assertEqual(contents2["message"],get_error_msg)
        self.assertEqual(contents2["error"],get_error_)
        self.assertEqual(contents2["doesExist"],False)
        self.assertEqual(response.status_code,400)

      


if __name__ == '__main__':
    unittest.main()