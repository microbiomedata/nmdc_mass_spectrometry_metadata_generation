import os
class Example:
    def __init__(self):
        self.api_key = os.getenv('API_KEY')
        self.api_secret = os.getenv('API_SECRET')
    def calculate(self):
        """
        This function calculates 1 + 1 and returns the result. 
        """
        return 1 + 1