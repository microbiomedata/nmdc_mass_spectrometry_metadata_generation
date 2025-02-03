from src.example import Example
class TestExample():
    
    def test_example(self):
        ex = Example()
        result = ex.calculate()
        assert result == 2