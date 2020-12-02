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


class gossip_test(unittest.TestCase):
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

    def test_gossip_1(self):
        response = self.put_request("13804", "x", "0", "{}")
        self.assertEqual(response.status_code, 200)
        time.sleep(3)

        response = self.get_request("13803",  "x", "{}")
        contents=response.json()
        self.assertEqual(contents["message"], get_resp)
        self.assertEqual(contents["doesExist"], True)
        self.assertEqual(response.status_code, 200)

    def test_gossip_2(self):
        response = self.put_request("13801", "x1", "0", "{}")
        self.assertEqual(response.status_code, 200)
        ct1= response.json()["causal-context"]

        response = self.put_request("13801", "y1", "1", "{}")
        self.assertEqual(response.status_code, 200)
        ct2= response.json()["causal-context"]

        response = self.put_request("13804", "test_key1", "2", "")
        self.assertEqual(response.status_code, 200)
        ct3= response.json()["causal-context"]
  
        response = self.get_request("13803",  "x1", ct1)
        contents=response.json()
        self.assertEqual(contents["message"], get_resp)
        self.assertEqual(contents["doesExist"], True)
        self.assertEqual(response.status_code, 200)

        response = self.get_request("13804",  "y1", ct2)
        contents=response.json()
        self.assertEqual(contents["message"], get_error_msg)
        self.assertEqual(response.status_code, 400)

        response = self.get_request("13801",  "test_key1", ct3)
        contents=response.json()
        self.assertEqual(contents["message"], get_resp)
        self.assertEqual(contents["doesExist"], True)
        self.assertEqual(response.status_code, 200)


    def test_gossip_3(self):
        response = self.put_request("13801", "x1", "1", "{}")
        self.assertEqual(response.status_code, 200)
        ct1= response.json()["causal-context"]
        response = self.put_request("13801", "y1", "2", "{}")
        self.assertEqual(response.status_code, 200)
        ct2= response.json()["causal-context"]
        response = self.put_request("13804", "test_key1", "3", "")
        self.assertEqual(response.status_code, 200)
        ct3= response.json()["causal-context"]
        time.sleep(3)
        
        response = self.get_request("13803",  "x1", ct1)
        contents=response.json()
    
        self.assertEqual(contents["message"], get_resp)
        self.assertEqual(contents["doesExist"], True)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(contents["value"],"1")
        response = self.get_request("13804",  "y1", ct2)
        contents=response.json()
        
        self.assertEqual(contents["message"], get_resp)
        self.assertEqual(contents["doesExist"], True)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(contents["value"],"2")

        response = self.get_request("13801",  "test_key1", ct3)
        contents=response.json()
        self.assertEqual(contents["message"], get_resp)
        self.assertEqual(contents["doesExist"], True)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(contents["value"],"3")

if __name__ == "__main__":
    unittest.main()