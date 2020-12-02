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
        response = self.put_request("13801", "x", "0", "")
        # Write X to R1
        response = self.put_request("13801", "x", "1", "")
        contents = response.json()
        self.assertEqual(contents["message"], update_resp)
        self.assertEqual(response.status_code, 200)
        # Read X from R2 with causal-context from prev write. Should Result in error
        response = self.get_request("13802", "x", contents["causal-context"])
        contents2 = response.json()
        self.assertEqual(contents2["message"], get_error_msg)
        self.assertEqual(contents2["error"], unable_error_resp)
        self.assertEqual(response.status_code, 400)

    # only works on first run since Y will exist
    def test_causal_2(self):
        response = self.put_request("13801", "y", "0", "")
        # Write X to R1
        response = self.put_request("13801", "y", "1", "")
        contents = response.json()
        self.assertEqual(contents["message"], update_resp)
        self.assertEqual(response.status_code, 200)
        # Read X from R2 without causal-context. Should Result in key DNE
        response = self.get_request("13802", "y", "")
        contents2 = response.json()
        self.assertEqual(contents2["message"], get_error_msg)
        self.assertEqual(contents2["error"], get_error_resp)
        self.assertEqual(contents2["doesExist"], False)
        self.assertEqual(response.status_code, 404)


    def test_causal_3(self):
        #Wait so that Y exists on nodes, so msg will be updated not added
        time.sleep(3)
        #C1 writes y=test1 to R1
        response = self.put_request("13801", "y", "test1", "")
        contents = response.json()
        self.assertEqual(contents["message"], update_resp)
        self.assertEqual(response.status_code, 200)
        c1= contents["causal-context"]
        #C2 writes y=test2 to R2
        response = self.put_request("13802", "y", "test2", "")
        contents = response.json()
        self.assertEqual(contents["message"], update_resp)
        self.assertEqual(response.status_code, 200)
        c2= contents["causal-context"]
        #C1 reads from R2
        response = self.get_request("13802", "y", c1)
        contents = response.json()
        self.assertEqual(contents["message"], get_resp)
        self.assertEqual(contents["value"], "test2")
        self.assertEqual(response.status_code, 200)
        #C2 reads from R1
        response = self.get_request("13801", "y", c2)
        contents = response.json()
        self.assertEqual(contents["message"], get_error_msg)
        self.assertEqual(contents["error"], unable_error_resp)
        self.assertEqual(response.status_code, 400)
        
if __name__ == "__main__":
    unittest.main()