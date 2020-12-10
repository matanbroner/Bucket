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
    def view_change(self, port):
        return requests.put(
            "http://localhost:%s/kvs/view-change" % (port),
            json={"view":"10.10.0.2:13800,10.10.0.3:13800,10.10.0.4:13800,10.10.0.5:13800","repl-factor":2},
            headers={"Content-Type": "application/json"},
        )

    def test_gossip_1(self):
        response = self.view_change("13801")
        contents = response.json()
        self.assertEqual(response.status_code, 200)

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

        time.sleep(5)
        response = self.get_request("13804",  "y", c1)
        contents=response.json()
        c1 = contents["causal-context"]
        self.assertEqual(contents["message"], get_resp)
        self.assertEqual(contents["doesExist"], True)
        self.assertTrue(200 <= response.status_code <= 201)

    def test_gossip_2(self):
        response = self.put_request("13801", "y1", "0", "")
        self.assertTrue(200 <= response.status_code <= 201)
        c1= response.json()["causal-context"]

        response = self.put_request("13804", "x1", "0", "")
        self.assertTrue(200 <= response.status_code <= 201)
        c2= response.json()["causal-context"]

        time.sleep(5)
        response = self.put_request("13803", "y1", "1",c1)
        self.assertTrue(200 <= response.status_code <= 201)
        c1= response.json()["causal-context"]

        response = self.put_request("13802", "x1", "1", c2)
        self.assertTrue(200 <= response.status_code <= 201)
        c2= response.json()["causal-context"]
 
        response = self.get_request("13804", "y1", c2)
        contents = response.json()
        self.assertEqual(response.status_code, 400)#400

        response = self.get_request("13801", "x1", c1)
        contents = response.json()
        self.assertEqual(response.status_code, 400)#400

        response = self.get_request("13804", "y1", "")
        contents = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(contents["value"], "0")

        response = self.get_request("13801", "x1", "")
        contents = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(contents["value"], "0")

        time.sleep(5)
        response = self.get_request("13804", "y1", c1)
        contents = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(contents["value"], "1")

        response = self.get_request("13801", "x1", c2)
        contents = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(contents["value"], "1")

        response = self.del_request("13804", "y1", c1)
        contents = response.json()
        self.assertEqual(response.status_code, 200)

        response = self.del_request("13801", "x1", c2)
        contents = response.json()
        self.assertEqual(response.status_code, 200)


if __name__ == "__main__":
    unittest.main()