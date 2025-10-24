"""
Tests for json_submit and validate_nmdc_database methods.
"""
import json
import os
import tempfile
from unittest.mock import MagicMock, Mock, patch

import pytest

from nmdc_ms_metadata_gen.metadata_generator import NMDCMetadataGenerator


@pytest.fixture
def sample_json_data():
    """Provide sample JSON data for testing."""
    return {
        "biosample_set": [],
        "data_object_set": [],
        "study_set": [],
    }


@pytest.fixture
def json_file(sample_json_data):
    """Create a temporary JSON file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(sample_json_data, f)
        temp_path = f.name
    yield temp_path
    # Cleanup
    if os.path.exists(temp_path):
        os.unlink(temp_path)


class TestValidateNmdcDatabase:
    """Tests for validate_nmdc_database method."""

    def test_validate_with_dict(self, sample_json_data):
        """Test validate_nmdc_database with a dictionary input."""
        generator = NMDCMetadataGenerator()
        # Use local validation to avoid API calls
        result = generator.validate_nmdc_database(
            json=sample_json_data, use_api=False
        )
        assert result["result"] == "All Okay!"

    def test_validate_with_file_path(self, json_file):
        """Test validate_nmdc_database with a file path input."""
        generator = NMDCMetadataGenerator()
        # Use local validation to avoid API calls
        result = generator.validate_nmdc_database(json=json_file, use_api=False)
        assert result["result"] == "All Okay!"


class TestJsonSubmit:
    """Tests for json_submit method."""

    @patch("nmdc_ms_metadata_gen.metadata_generator.NMDCAuth")
    @patch("nmdc_ms_metadata_gen.metadata_generator.Metadata")
    def test_json_submit_with_dict(self, mock_metadata, mock_auth, sample_json_data):
        """Test json_submit with a dictionary input."""
        # Setup mocks
        mock_metadata_instance = Mock()
        mock_metadata_instance.submit_json.return_value = 200
        mock_metadata.return_value = mock_metadata_instance

        generator = NMDCMetadataGenerator()
        # Should not raise an error
        generator.json_submit(
            json=sample_json_data, CLIENT_ID="test_id", CLIENT_SECRET="test_secret"
        )

        # Verify the submit_json was called with the correct data
        mock_metadata_instance.submit_json.assert_called_once_with(sample_json_data)

    @patch("nmdc_ms_metadata_gen.metadata_generator.NMDCAuth")
    @patch("nmdc_ms_metadata_gen.metadata_generator.Metadata")
    def test_json_submit_with_file_path(
        self, mock_metadata, mock_auth, json_file, sample_json_data
    ):
        """Test json_submit with a file path input."""
        # Setup mocks
        mock_metadata_instance = Mock()
        mock_metadata_instance.submit_json.return_value = 200
        mock_metadata.return_value = mock_metadata_instance

        generator = NMDCMetadataGenerator()
        # Should not raise an error
        generator.json_submit(
            json=json_file, CLIENT_ID="test_id", CLIENT_SECRET="test_secret"
        )

        # Verify the submit_json was called with the loaded JSON data
        mock_metadata_instance.submit_json.assert_called_once()
        # Get the actual argument passed to submit_json
        actual_arg = mock_metadata_instance.submit_json.call_args[0][0]
        assert actual_arg == sample_json_data

    @patch("nmdc_ms_metadata_gen.metadata_generator.NMDCAuth")
    @patch("nmdc_ms_metadata_gen.metadata_generator.Metadata")
    def test_json_submit_failure(self, mock_metadata, mock_auth, sample_json_data):
        """Test json_submit raises ValueError on submission failure."""
        # Setup mocks to return error code
        mock_metadata_instance = Mock()
        mock_metadata_instance.submit_json.return_value = 500
        mock_metadata.return_value = mock_metadata_instance

        generator = NMDCMetadataGenerator()
        with pytest.raises(ValueError, match="Failed to submit JSON metadata"):
            generator.json_submit(
                json=sample_json_data,
                CLIENT_ID="test_id",
                CLIENT_SECRET="test_secret",
            )

    def test_json_submit_is_instance_method(self):
        """Test that json_submit is an instance method (has self parameter)."""
        generator = NMDCMetadataGenerator()
        # This should work without error - json_submit should be an instance method
        assert hasattr(generator, "json_submit")
        assert callable(generator.json_submit)
