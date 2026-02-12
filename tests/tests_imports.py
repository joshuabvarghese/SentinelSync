"""
Test that all required modules can be imported.
"""

import pytest


def test_imports():
    """Test that core dependencies can be imported"""
    try:
        import yaml
        import psycopg2
        import confluent_kafka
        import cassandra
    except ImportError as e:
        pytest.fail(f"Failed to import dependency: {e}")
    
    # If we get here, all imports succeeded
    assert True


def test_version():
    """Test that version is defined"""
    import src
    assert hasattr(src, "__version__")
    assert src.__version__ == "0.1.0"