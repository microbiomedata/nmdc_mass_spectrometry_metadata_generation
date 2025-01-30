from nmdc_mass_spectrometry_metadata_generation.src.example import Example
class TestExample(Example):
    
    def test_example(self):
        result = self.calculate()
        assert result == 2