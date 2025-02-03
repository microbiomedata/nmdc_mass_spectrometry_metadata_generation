import os
class Example:
    def __init__(self):
        self.client_id = os.getenv('CLIENT_ID')
        self.client_secret = os.getenv('CLIENT_SECRET')
    def calculate(self):
        """
        This function calculates 1 + 1 and returns the result. 
        """
        return 1 + 1