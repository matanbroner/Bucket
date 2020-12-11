#! /usr/bin/python3
#Run multi_node_partition.sh before running this test

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

    # 13801,13803 in 1 View 13802,13804 in another view
    def test_causal_1(self):

        response = self.put_request("13801", "x", "0", "{}")
        contents = response.json()
        c1 = contents["causal-context"]
        self.assertTrue(200 <= response.status_code <= 201)
        
        response = self.put_request("13802", "y", "0", c1)
        contents = response.json()
        c1 = contents["causal-context"]
        self.assertEqual(response.status_code, 200)

        response = self.get_request("13802", "x", c1)
        contents = response.json()
        self.assertEqual(response.status_code, 400)
        
        response = self.get_request("13804", "y", c1)
        contents = response.json()
        self.assertEqual(response.status_code, 400)

        response = self.get_request("13804", "x", "")
        contents = response.json()
        self.assertEqual(response.status_code, 404)

        response = self.get_request("13803", "y", "")
        contents = response.json()
        self.assertEqual(response.status_code, 404)
        
       
        



if __name__ == "__main__":
    unittest.main()