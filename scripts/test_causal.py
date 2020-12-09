#! /usr/bin/python3
import unittest
import requests
import json
import time
import os

update_resp = "Updated successfully"
add_resp = "Added successfully"
get_resp = "Retrieved successfully"

get_error_resp = "Key does not exist"
unable_error_resp = "Unable to satisfy request"

get_error_msg = "Error in GET"
put_error_msg = "Error in PUT"
del_error_msg = "Error in DEL"


class causal_test(unittest.TestCase):
    def put_request(self, port, key, val, context):
        return requests.put(
            "http://localhost:%s/kvs/keys/%s" % (port, key),
            json={"value": val, "causal-context": context},
            headers={"Content-Type": "application/json"},
        )

    def get_request(self, port, key, context):
        return requests.get(
            "http://localhost:%s/kvs/keys/%s" % (port, key),
            json={"causal-context": context},
            headers={"Content-Type": "application/json"},
        )

    def test_causal_1(self):
        #C1 Writes X to R1
        response = self.put_request("13801", "x", "1", "")
        contents = response.json()
        c1= contents["causal-context"] 
        self.assertEqual(response.status_code, 200)
        #C2 Writes X to R2
        response = self.put_request("13802", "x", "2", "")
        contents = response.json()
        c2= contents["causal-context"] 
        self.assertEqual(response.status_code, 200)
        #C1 Writes Y to R1
        response = self.put_request("13801", "y", "1", c1)
        contents = response.json()
        c1= contents["causal-context"] 
        self.assertEqual(response.status_code, 200)
        #C2 Writes Y to R2
        response = self.put_request("13802", "y", "2", c2)
        contents = response.json()
        c2= contents["causal-context"] 
        self.assertEqual(response.status_code, 200)
        print ("Context C1:",c1)
        print ("\n")
        print ("Context C2",c2)
        print ("\n")
        #C2 Reads Y from R1
        response = self.get_request("13801", "y", c2)
        contents = response.json()
        c2= contents["causal-context"] 
        print ("Context C1:",c1)
        print ("\nvalue:",contents["value"])
        print ("\n")
        #C1 Reads Y from R2
        response = self.get_request("13802", "y", c1)
        contents = response.json()
        c1= contents["causal-context"] 
        print ("Context C2",c2)
        print ("\nvalue:",contents["value"])
        print ("\n")



        
if __name__ == "__main__":
    unittest.main()