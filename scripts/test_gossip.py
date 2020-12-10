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
    def del_request(self, port, key, context):
        return requests.delete(
            "http://localhost:%s/kvs/keys/%s" % (port, key),
            json={"causal-context": context},
            headers={"Content-Type": "application/json"},
        )

    def test_gossip_1(self):
        response = self.put_request("13801", "x", "0", "{}")
        contents = response.json()
        c1 = contents["causal-context"]
        self.assertTrue(200 <= response.status_code <= 201)

        response = self.put_request("13803", "y", "0", c1)
        contents = response.json()
        c1 = contents["causal-context"]
        self.assertTrue(200 <= response.status_code <= 201)

        response = self.get_request("13804", "y", c1)
        contents = response.json()
        self.assertEqual(response.status_code, 400)

        time.sleep(3)
        response = self.get_request("13804",  "y", c1)
        contents=response.json()
        c1 = contents["causal-context"]
        self.assertEqual(contents["message"], get_resp)
        self.assertEqual(contents["doesExist"], True)
        self.assertTrue(200 <= response.status_code <= 201)

    def test_gossip_2(self):
        response = self.put_request("13801", "x1", "0", "{}")
        self.assertTrue(200 <= response.status_code <= 201)
        c1= response.json()["causal-context"]

        response = self.put_request("13801", "y1", "1", "{}")
        self.assertTrue(200 <= response.status_code <= 201)
        c2= response.json()["causal-context"]

        response = self.put_request("13804", "test_key1", "2", "")
        self.assertTrue(200 <= response.status_code <= 201)
        c3= response.json()["causal-context"]
  
        response = self.get_request("13803",  "x1", c1)
        contents=response.json()
        self.assertEqual(contents["message"], get_resp)
        self.assertEqual(contents["doesExist"], True)
        self.assertTrue(200 <= response.status_code <= 201)

        #Might have an error here. Need to check logic
        response = self.get_request("13804",  "y1", c2)
        contents=response.json()
        self.assertEqual(contents["message"], get_error_msg)
        self.assertEqual(response.status_code, 404)

        response = self.get_request("13801",  "test_key1", c3)
        contents=response.json()
        self.assertEqual(contents["message"], get_resp)
        self.assertEqual(contents["doesExist"], True)
        self.assertTrue(200 <= response.status_code <= 201)


    def ttest_gossip_3(self):
        response = self.put_request("13801", "x1", "1", "{}")
        self.assertTrue(200 <= response.status_code <= 201)
        c1= response.json()["causal-context"]
        response = self.put_request("13801", "y1", "2", "{}")
        self.assertTrue(200 <= response.status_code <= 201)
        c2= response.json()["causal-context"]
        response = self.put_request("13804", "test_key1", "3", "")
        self.assertTrue(200 <= response.status_code <= 201)
        c3= response.json()["causal-context"]
        time.sleep(3)
        
        response = self.get_request("13803",  "x1", c1)
        contents=response.json()
    
        self.assertEqual(contents["message"], get_resp)
        self.assertEqual(contents["doesExist"], True)
        self.assertTrue(200 <= response.status_code <= 201)
        self.assertEqual(contents["value"],"1")
        response = self.get_request("13804",  "y1", c2)
        contents=response.json()
        
        self.assertEqual(contents["message"], get_resp)
        self.assertEqual(contents["doesExist"], True)
        self.assertTrue(200 <= response.status_code <= 201)
        self.assertEqual(contents["value"],"2")

        response = self.get_request("13801",  "test_key1", c3)
        contents=response.json()
        self.assertEqual(contents["message"], get_resp)
        self.assertEqual(contents["doesExist"], True)
        self.assertTrue(200 <= response.status_code <= 201)
        self.assertEqual(contents["value"],"3")

if __name__ == "__main__":
    unittest.main()