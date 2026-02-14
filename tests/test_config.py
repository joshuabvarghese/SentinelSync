"""
Tests for SentinelSync configuration management
"""

import os
import pytest
import tempfile
from pathlib import Path
import yaml

from src.config import (
    PostgresConfig,
    KafkaConfig,
    CassandraConfig,
    AppConfig,
    ConfigLoader,
    get_config,
    reload_config,
)


class TestPostgresConfig:
    """Tests for PostgreSQL configuration"""
    
    def test_connection_string(self):
        """Test PostgreSQL connection string generation"""
        config = PostgresConfig(
            host='localhost',
            port=5432,
            database='testdb',
            user='testuser',
            password='testpass',
            replication_slot='test_slot',
        )
        
        expected = "postgresql://testuser:testpass@localhost:5432/testdb"
        assert config.connection_string == expected
    
    def test_dsn(self):
        """Test DSN dictionary generation"""
        config = PostgresConfig(
            host='localhost',
            port=5432,
            database='testdb',
            user='testuser',
            password='testpass',
            replication_slot='test_slot',
        )
        
        dsn = config.dsn
        assert dsn['host'] == 'localhost'
        assert dsn['port'] == 5432
        assert dsn['database'] == 'testdb'
        assert dsn['user'] == 'testuser'
        assert dsn['password'] == 'testpass'
    
    def test_validation_success(self):
        """Test validation with valid config"""
        config = PostgresConfig(
            host='localhost',
            port=5432,
            database='testdb',
            user='testuser',
            password='testpass',
            replication_slot='test_slot',
        )
        
        errors = config.validate()
        assert len(errors) == 0
    
    def test_validation_failures(self):
        """Test validation with invalid config"""
        config = PostgresConfig(
            host='',  # Empty host
            port=99999,  # Invalid port
            database='',  # Empty database
            user='',  # Empty user
            password='testpass',
            replication_slot='',  # Empty slot
        )
        
        errors = config.validate()
        assert len(errors) >= 4  # At least 4 errors


class TestKafkaConfig:
    """Tests for Kafka configuration"""
    
    def test_producer_config(self):
        """Test Kafka producer configuration"""
        config = KafkaConfig(
            bootstrap_servers='localhost:9092',
            topic='test.topic',
            group_id='test-group',
        )
        
        producer_config = config.producer_config
        assert producer_config['bootstrap.servers'] == 'localhost:9092'
        assert producer_config['acks'] == 'all'
        assert producer_config['compression.type'] == 'snappy'
        assert producer_config['enable.idempotence'] is True
    
    def test_consumer_config(self):
        """Test Kafka consumer configuration"""
        config = KafkaConfig(
            bootstrap_servers='localhost:9092',
            topic='test.topic',
            group_id='test-group',
        )
        
        consumer_config = config.consumer_config
        assert consumer_config['bootstrap.servers'] == 'localhost:9092'
        assert consumer_config['group.id'] == 'test-group'
        assert consumer_config['enable.auto.commit'] is False
    
    def test_validation_success(self):
        """Test validation with valid config"""
        config = KafkaConfig(
            bootstrap_servers='localhost:9092',
            topic='test.topic',
            group_id='test-group',
        )
        
        errors = config.validate()
        assert len(errors) == 0
    
    def test_validation_failures(self):
        """Test validation with invalid config"""
        config = KafkaConfig(
            bootstrap_servers='',
            topic='',
            group_id='',
            auto_offset_reset='invalid',
        )
        
        errors = config.validate()
        assert len(errors) >= 4


class TestCassandraConfig:
    """Tests for Cassandra configuration"""
    
    def test_contact_points(self):
        """Test contact points property"""
        config = CassandraConfig(
            hosts=['node1', 'node2'],
            port=9042,
            keyspace='testks',
        )
        
        assert config.contact_points == ['node1', 'node2']
    
    def test_auth_provider(self):
        """Test auth provider with and without credentials"""
        # Without credentials
        config = CassandraConfig(
            hosts=['localhost'],
            port=9042,
            keyspace='testks',
        )
        assert config.auth_provider is None
        
        # With credentials
        config = CassandraConfig(
            hosts=['localhost'],
            port=9042,
            keyspace='testks',
            username='testuser',
            password='testpass',
        )
        assert config.auth_provider == ('testuser', 'testpass')
    
    def test_validation_success(self):
        """Test validation with valid config"""
        config = CassandraConfig(
            hosts=['localhost'],
            port=9042,
            keyspace='testks',
            replication_factor=3,
        )
        
        errors = config.validate()
        assert len(errors) == 0
    
    def test_validation_failures(self):
        """Test validation with invalid config"""
        config = CassandraConfig(
            hosts=[],
            port=99999,
            keyspace='',
            replication_factor=0,
        )
        
        errors = config.validate()
        assert len(errors) >= 4


class TestAppConfig:
    """Tests for application configuration"""
    
    @pytest.fixture
    def valid_config(self):
        """Create a valid app config for testing"""
        return AppConfig(
            postgres=PostgresConfig(
                host='localhost',
                port=5432,
                database='testdb',
                user='testuser',
                password='testpass',
                replication_slot='test_slot',
            ),
            kafka=KafkaConfig(
                bootstrap_servers='localhost:9092',
                topic='test.topic',
                group_id='test-group',
            ),
            cassandra=CassandraConfig(
                hosts=['localhost'],
                port=9042,
                keyspace='testks',
            ),
            log_level='INFO',
            metrics_port=9090,
            health_check_port=8080,
        )
    
    def test_validation_success(self, valid_config):
        """Test validation with valid config"""
        errors = valid_config.validate()
        assert len(errors) == 0
    
    def test_validation_failures(self, valid_config):
        """Test validation with invalid config"""
        valid_config.log_level = 'INVALID'
        valid_config.metrics_port = 99999
        valid_config.max_retries = -1
        
        errors = valid_config.validate()
        assert len(errors) >= 3
    
    def test_to_dict(self, valid_config):
        """Test conversion to dictionary"""
        config_dict = valid_config.to_dict()
        
        assert 'postgres' in config_dict
        assert 'kafka' in config_dict
        assert 'cassandra' in config_dict
        assert config_dict['log_level'] == 'INFO'
        assert config_dict['metrics_port'] == 9090


class TestConfigLoader:
    """Tests for configuration loader"""
    
    @pytest.fixture
    def temp_config_file(self):
        """Create a temporary config file for testing"""
        config_data = {
            'postgres': {
                'host': 'testhost',
                'port': 5432,
                'database': 'testdb',
                'user': 'testuser',
                'password': 'testpass',
                'replication_slot': 'test_slot',
            },
            'kafka': {
                'bootstrap_servers': 'testkafka:9092',
                'topic': 'test.topic',
                'group_id': 'test-group',
            },
            'cassandra': {
                'hosts': ['testcassandra'],
                'port': 9042,
                'keyspace': 'testks',
                'replication_factor': 3,
            },
            'app': {
                'log_level': 'DEBUG',
                'metrics_port': 9091,
                'health_check_port': 8081,
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_path = f.name
        
        yield temp_path
        
        # Cleanup
        os.unlink(temp_path)
    
    def test_load_yaml(self, temp_config_file):
        """Test loading YAML configuration"""
        loader = ConfigLoader(temp_config_file)
        config = loader.load(env_overrides=False)
        
        assert config.postgres.host == 'testhost'
        assert config.kafka.bootstrap_servers == 'testkafka:9092'
        assert config.cassandra.hosts == ['testcassandra']
        assert config.log_level == 'DEBUG'
    
    def test_env_overrides(self, temp_config_file):
        """Test environment variable overrides"""
        # Set environment variables
        os.environ['POSTGRES_HOST'] = 'envhost'
        os.environ['KAFKA_TOPIC'] = 'env.topic'
        os.environ['CASSANDRA_HOSTS'] = 'env1,env2'
        os.environ['LOG_LEVEL'] = 'ERROR'
        
        loader = ConfigLoader(temp_config_file)
        config = loader.load(env_overrides=True)
        
        assert config.postgres.host == 'envhost'
        assert config.kafka.topic == 'env.topic'
        assert config.cassandra.hosts == ['env1', 'env2']
        assert config.log_level == 'ERROR'
        
        # Cleanup
        del os.environ['POSTGRES_HOST']
        del os.environ['KAFKA_TOPIC']
        del os.environ['CASSANDRA_HOSTS']
        del os.environ['LOG_LEVEL']
    
    def test_missing_file(self):
        """Test loading non-existent file"""
        loader = ConfigLoader('/nonexistent/path/config.yaml')
        
        with pytest.raises(FileNotFoundError):
            loader.load()


class TestSingleton:
    """Tests for singleton config access"""
    
    def test_get_config(self):
        """Test getting singleton config"""
        # Clear any existing config
        import src.config
        src.config._config = None
        
        config1 = get_config()
        config2 = get_config()
        
        # Should be the same instance
        assert config1 is config2
    
    def test_reload_config(self):
        """Test reloading config"""
        # Clear any existing config
        import src.config
        src.config._config = None
        
        config1 = get_config()
        config2 = reload_config()
        
        # Should be different instances (reloaded)
        assert config1 is not config2
    
    def test_config_with_path(self):
        """Test loading config with custom path"""
        # This should fail because file doesn't exist
        with pytest.raises(FileNotFoundError):
            get_config('/nonexistent/path/config.yaml', reload=True)