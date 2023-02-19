from .wrapper import MyWrapper
import unittest
from unittest.mock import Mock

# Define the test case class
class TestMyWrapper(unittest.TestCase):
    # Define the test methods
    def test_init(self):
        wrapper = MyWrapper()
        self.assertEqual(wrapper.base_url, "http://localhost:25510")
        self.assertIsNone(wrapper.call_type)
        self.assertIsNone(wrapper.sec_type)
        self.assertIsNone(wrapper.req_type)
        
        self.assertIsNone(wrapper.url)
        self.assertIsNone(wrapper.params)
        self.assertIsNone(wrapper.request)
        self.assertIsNone(wrapper.header)
        self.assertIsNone(wrapper.response)
        
        self.assertIsNone(wrapper.format)
        

    def test_parse_header(self):
        wrapper = MyWrapper()
        
        # Test with valid response
        request = Mock()
        request.ok = True
        request.json.return_value = {"response": [1, 2, 3], "header":{"error_type":'null',"error_msg":'null'}}

        wrapper.request = request
        data = wrapper._parse_header()
        self.assertTrue(data)

        # Test with invalid status code
        request.ok = False
        wrapper.request = request
        with self.assertRaises(Exception):
            wrapper._parse_header()

        # Test with empty response
        request.ok = True
        request.json.return_value = None
        wrapper.request = request
        with self.assertRaises(Exception):
            wrapper._parse_header()

            
    def test_parse_response(self):
        wrapper = MyWrapper()
        # Test with valid response
        request = Mock()
        request.ok = True
        
        header = {"error_type":'null',"error_msg":'null' ,"format":["a","b","c"]}
        request.json.return_value = {"response": [[1, 2, 3]],"header":header}
                                      
        wrapper.request = request
        wrapper.header = header
        data = wrapper._parse_response()
        self.assertListEqual(data, [{"a":1,"b":2,"c":3}])
        
        # Test with response content is not a list
        header = {"error_type":'null',"error_msg":'null',"format":None} 
        request.json.return_value = {"response": 0 ,"header":header}
                       
        wrapper.request = request
        wrapper.header = header
        with self.assertRaises(Exception):
            wrapper._parse_response()
            
        # Test with list response content is empty
        header = {"error_type":'null',"error_msg":'null',"format":None}
        request.json.return_value = {"response": [] ,"header":header}
                                     
        wrapper.request = request
        wrapper.header = header
        with self.assertRaises(Exception):
            wrapper._parse_response()

        
    def test_get_list_roots(self):
        # Set up a test wrapper
        wrapper = MyWrapper()

        # Test with valid sec_type='opt'
        roots = wrapper.get_list_roots("opt")
        self.assertGreater(len(roots), 0)
        for root in roots:
            self.assertIsInstance(root, dict)

        # Test with valid sec_type='stk'
        roots = wrapper.get_list_roots("stk")
        self.assertGreater(len(roots), 0)
        for root in roots:
            self.assertIsInstance(root, dict)

        # Test with invalid sec_type='foo'
        with self.assertRaises(ValueError):
            wrapper.get_list_roots("foo")
           
    def test_get_list_strikes(self):
        # Set up a test wrapper
        wrapper = MyWrapper()

        # Test with valid inputs
        strikes = wrapper.get_list_strikes("20230317", "AAPL")
        self.assertGreater(len(strikes), 0)
        for strike in strikes:
            self.assertIsInstance(strike, dict)

        # Test with invalid expiry date
        with self.assertRaises(Exception):
            wrapper.get_strikes("abdc-03-17", "AAPL")

        # Test with invalid root
        with self.assertRaises(Exception):
            wrapper.get_strikes("20230317", "123")

    def test_get_list_expirations(self):
        # Set up a test wrapper
        wrapper = MyWrapper()

        # Test with valid input
        expirations = wrapper.get_list_expirations("AAPL")
        self.assertGreater(len(expirations), 0)
        for exp in expirations:
            self.assertIsInstance(exp, dict)

        # Test with invalid input
        with self.assertRaises(Exception):
            wrapper.get_expirations("123")

    def test_get_hist_option_eod(self):
        start_date = "20230101"
        end_date = "20230217"
        exp = "20230317"
        right = "C"
        root = "SPY"

        strike = 400

        wrapper = MyWrapper()
        options = wrapper.get_hist_option_eod(end_date=end_date, exp=exp, right=right
                                          , root=root, start_date=start_date, strike=strike)
        

        # check that the response is a non-empty list
        self.assertIsInstance(options, list)
        self.assertTrue(options)

        # check that each item in the response is a dictionnary
        for item in options:
            self.assertIsInstance(item, dict)
            self.assertTrue(item)

            # check that the last item in each inner list is a valid timestamp
            self.assertTrue(isinstance(item["date"], int) and item["date"] > 0)

            # check that the first item in each inner list is a valid float
            self.assertTrue(isinstance(item["open"], float) and item["open"] >= 0.0)

# Create a test suite and run the tests
suite = unittest.TestLoader().loadTestsFromTestCase(TestMyWrapper)
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)
