from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("nmdc_mass_spectrometry_metadata_generation")
except PackageNotFoundError:
    __version__ = "0.0.0-dev"
