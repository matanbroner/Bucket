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
        print ("\nTEST 1")
        # C1 Writes X to R1
        response = self.put_request("13801", "x", "1", "")
        contents = response.json()
        c1 = contents["causal-context"]
        print("C1 context after write x=1 to R1:", c1)
        self.assertTrue(200 <= response.status_code <= 201)

        # C2 Writes X to R2
        response = self.put_request("13802", "x", "2", "")
        contents = response.json()
        c2 = contents["causal-context"]
        print("C2 context after write x=2 to R2:", c2)
        self.assertTrue(200 <= response.status_code <= 201)

        # C1 Writes Y to R1
        response = self.put_request("13801", "y", "1", c1)
        contents = response.json()
        c1 = contents["causal-context"]
        print("C1 context after write y=1 to R1:", c1)
        self.assertTrue(200 <= response.status_code <= 201)

        # C2 Writes Y to R2
        response = self.put_request("13802", "y", "2", c2)
        contents = response.json()
        c2 = contents["causal-context"]
        print("C2 context after write y=2 to R2:", c2)
        self.assertTrue(200 <= response.status_code <= 201)

        # C1 Reads Y from R2
        response = self.get_request("13802", "y", c1)
        contents = response.json()
        c1 = contents["causal-context"]
        print("C1 context after read y from R2:", c1)
        print("Y R2 value:", contents["value"])

        # C2 Reads Y from R1, should get causal error due to writing x second
        response = self.get_request("13801", "y", c2)
        self.assertEqual(response.status_code, 400)



    '''
    C1      R1     C2
    |  a =1 |      |
    |-----> |  R(a)|
    |       |<-----|
    |  b=1  |      |
    |-----> |      |
    |  c=1  |      |
    |-----> |  R(c)|
    |       |<-----|

    '''
    def test_causal_2(self):
        # C1 Writes a to R1
        print ("\n\nTEST 2")
        response = self.put_request("13801", "a", "1", "")
        contents = response.json()
        c1 = contents["causal-context"]
        print("C1 context after write a=1 to R1:", c1)
        self.assertTrue(200 <= response.status_code <= 201)

        #C2 Reads a from R1
        response = self.get_request("13801", "a", "")
        contents = response.json()
        c2 = contents["causal-context"]
        print("C2 context after reads a from R1:", c2)
        print("Y R2 value:", contents["value"])
        self.assertEqual(response.status_code, 200)

        # C1 Writes b to R1
        response = self.put_request("13801", "b", "1", c1)
        contents = response.json()
        c1 = contents["causal-context"]
        print("C1 context after write b=1 to R1:", c1)
        self.assertTrue(200 <= response.status_code <= 201)
        
        # C1 Writes c to R1
        response = self.put_request("13801", "c", "1", c1)
        contents = response.json()
        c1 = contents["causal-context"]
        print("C1 context after write c=1 to R1:", c1)
        self.assertTrue(200 <= response.status_code <= 201)

        #C2 Reads c from R1
        response = self.get_request("13801", "c", c2)
        contents = response.json()
        c2 = contents["causal-context"]
        print("C2 context after read c from R1:", c2)
        self.assertEqual(response.status_code, 503)

if __name__ == "__main__":
    unittest.main()