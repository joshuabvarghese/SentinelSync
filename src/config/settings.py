"""
SentinelSync Configuration Management

Loads and validates application configuration from YAML files and environment variables.
Supports:
- YAML configuration files
- Environment variable overrides
- Type-safe dataclasses
- Singleton pattern for global config access
"""

import os
import yaml
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Any
import logging


logger = logging.getLogger(__name__)


@dataclass
class PostgresConfig:
    """PostgreSQL source database configuration"""
    host: str
    port: int
    database: str
    user: str
    password: str
    replication_slot: str
    publication: str = "sentinelsync_pub"
    
    @property
    def connection_string(self) -> str:
        """Get PostgreSQL connection string"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    @property
    def dsn(self) -> Dict[str, Any]:
        """Get DSN parameters for psycopg2"""
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'password': self.password,
        }
    
    def validate(self) -> List[str]:
        """Validate configuration and return list of errors"""
        errors = []
        
        if self.port < 1 or self.port > 65535:
            errors.append(f"Invalid PostgreSQL port: {self.port}")
        
        if not self.host:
            errors.append("PostgreSQL host cannot be empty")
        
        if not self.database:
            errors.append("PostgreSQL database cannot be empty")
        
        if not self.user:
            errors.append("PostgreSQL user cannot be empty")
        
        if not self.replication_slot:
            errors.append("PostgreSQL replication slot cannot be empty")
        
        return errors


@dataclass
class KafkaConfig:
    """Apache Kafka configuration"""
    bootstrap_servers: str
    topic: str
    group_id: str
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    session_timeout_ms: int = 30000
    max_poll_interval_ms: int = 300000
    compression_type: str = "snappy"
    
    @property
    def producer_config(self) -> dict:
        """Get Kafka producer configuration"""
        return {
            'bootstrap.servers': self.bootstrap_servers,
            'compression.type': self.compression_type,
            'acks': 'all',  # Wait for all replicas
            'retries': 10,
            'retry.backoff.ms': 100,
            'enable.idempotence': True,  # Exactly-once semantics
            'max.in.flight.requests.per.connection': 5,
        }
    
    @property
    def consumer_config(self) -> dict:
        """Get Kafka consumer configuration"""
        return {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset_reset,
            'enable.auto.commit': self.enable_auto_commit,
            'session.timeout.ms': self.session_timeout_ms,
            'max.poll.interval.ms': self.max_poll_interval_ms,
            'max.poll.records': 500,  # Max records per poll
        }
    
    def validate(self) -> List[str]:
        """Validate configuration and return list of errors"""
        errors = []
        
        if not self.bootstrap_servers:
            errors.append("Kafka bootstrap servers cannot be empty")
        
        if not self.topic:
            errors.append("Kafka topic cannot be empty")
        
        if not self.group_id:
            errors.append("Kafka group ID cannot be empty")
        
        valid_reset_values = ['earliest', 'latest', 'none']
        if self.auto_offset_reset not in valid_reset_values:
            errors.append(f"Invalid auto_offset_reset: {self.auto_offset_reset}. Must be one of {valid_reset_values}")
        
        return errors


@dataclass
class CassandraConfig:
    """Apache Cassandra sink database configuration"""
    hosts: List[str]
    port: int
    keyspace: str
    replication_factor: int = 3
    username: Optional[str] = None
    password: Optional[str] = None
    protocol_version: int = 4
    consistency_level: str = "QUORUM"
    
    @property
    def contact_points(self) -> List[str]:
        """Get Cassandra contact points"""
        return self.hosts
    
    @property
    def auth_provider(self) -> Optional[tuple]:
        """Get authentication provider tuple (username, password) if configured"""
        if self.username and self.password:
            return (self.username, self.password)
        return None
    
    def validate(self) -> List[str]:
        """Validate configuration and return list of errors"""
        errors = []
        
        if not self.hosts:
            errors.append("Cassandra hosts cannot be empty")
        
        if self.port < 1 or self.port > 65535:
            errors.append(f"Invalid Cassandra port: {self.port}")
        
        if not self.keyspace:
            errors.append("Cassandra keyspace cannot be empty")
        
        if self.replication_factor < 1:
            errors.append(f"Invalid replication factor: {self.replication_factor}. Must be >= 1")
        
        return errors


@dataclass
class AppConfig:
    """Application-wide configuration"""
    postgres: PostgresConfig
    kafka: KafkaConfig
    cassandra: CassandraConfig
    log_level: str = "INFO"
    metrics_port: int = 9090
    health_check_port: int = 8080
    max_retries: int = 5
    retry_backoff_seconds: int = 2
    
    def validate(self) -> List[str]:
        """Validate entire configuration and return list of errors"""
        errors = []
        
        # Validate each component
        errors.extend(self.postgres.validate())
        errors.extend(self.kafka.validate())
        errors.extend(self.cassandra.validate())
        
        # Validate app settings
        valid_log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if self.log_level not in valid_log_levels:
            errors.append(f"Invalid log_level: {self.log_level}. Must be one of {valid_log_levels}")
        
        if self.metrics_port < 1 or self.metrics_port > 65535:
            errors.append(f"Invalid metrics_port: {self.metrics_port}")
        
        if self.health_check_port < 1 or self.health_check_port > 65535:
            errors.append(f"Invalid health_check_port: {self.health_check_port}")
        
        if self.max_retries < 0:
            errors.append(f"Invalid max_retries: {self.max_retries}. Must be >= 0")
        
        if self.retry_backoff_seconds < 0:
            errors.append(f"Invalid retry_backoff_seconds: {self.retry_backoff_seconds}. Must be >= 0")
        
        return errors
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary (for serialization)"""
        return {
            'postgres': asdict(self.postgres),
            'kafka': asdict(self.kafka),
            'cassandra': asdict(self.cassandra),
            'log_level': self.log_level,
            'metrics_port': self.metrics_port,
            'health_check_port': self.health_check_port,
            'max_retries': self.max_retries,
            'retry_backoff_seconds': self.retry_backoff_seconds,
        }


class ConfigLoader:
    """Loads configuration from YAML file with environment variable overrides"""
    
    def __init__(self, config_path: str = "config/app.yaml"):
        """
        Initialize config loader.
        
        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = config_path
        self.env_prefix = ""  # Can be set for different environments
    
    def load(self, env_overrides: bool = True) -> AppConfig:
        """
        Load configuration from YAML file and environment variables.
        
        Environment variables override YAML values:
        - POSTGRES_HOST, POSTGRES_PORT, etc.
        - KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, etc.
        - CASSANDRA_HOSTS, CASSANDRA_KEYSPACE, etc.
        - LOG_LEVEL, METRICS_PORT, etc.
        
        Args:
            env_overrides: Whether to apply environment variable overrides
            
        Returns:
            AppConfig object
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If configuration validation fails
        """
        # Load YAML config
        config_data = self._load_yaml()
        
        # Apply environment variable overrides if requested
        if env_overrides:
            config_data = self._apply_env_overrides(config_data)
        
        # Create config objects
        config = self._create_config(config_data)
        
        # Validate configuration
        errors = config.validate()
        if errors:
            error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
            raise ValueError(error_msg)
        
        return config
    
    def _load_yaml(self) -> Dict[str, Any]:
        """Load YAML configuration file"""
        config_file = Path(self.config_path)
        
        # Check if file exists
        if not config_file.exists():
            # Try looking in parent directory
            config_file = Path("config") / "app.yaml"
            if not config_file.exists():
                raise FileNotFoundError(
                    f"Configuration file not found: {self.config_path}\n"
                    "Please ensure config/app.yaml exists or set CONFIG_PATH environment variable."
                )
        
        with open(config_file, 'r') as f:
            try:
                config_data = yaml.safe_load(f)
                logger.info(f"Loaded configuration from {config_file}")
                return config_data
            except yaml.YAMLError as e:
                raise ValueError(f"Failed to parse YAML configuration: {e}")
    
    def _apply_env_overrides(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply environment variable overrides to config data"""
        
        # PostgreSQL overrides
        if 'postgres' in config_data:
            pg = config_data['postgres']
            pg['host'] = os.getenv('POSTGRES_HOST', pg.get('host', 'localhost'))
            pg['port'] = int(os.getenv('POSTGRES_PORT', pg.get('port', 5432)))
            pg['database'] = os.getenv('POSTGRES_DB', pg.get('database', 'sourcedb'))
            pg['user'] = os.getenv('POSTGRES_USER', pg.get('user', 'cdcuser'))
            pg['password'] = os.getenv('POSTGRES_PASSWORD', pg.get('password', 'cdcpass'))
            pg['replication_slot'] = os.getenv('POSTGRES_REPLICATION_SLOT', 
                                              pg.get('replication_slot', 'sentinelsync_slot'))
            pg['publication'] = os.getenv('POSTGRES_PUBLICATION', 
                                         pg.get('publication', 'sentinelsync_pub'))
        
        # Kafka overrides
        if 'kafka' in config_data:
            kf = config_data['kafka']
            kf['bootstrap_servers'] = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 
                                               kf.get('bootstrap_servers', 'localhost:9092'))
            kf['topic'] = os.getenv('KAFKA_TOPIC', kf.get('topic', 'cdc.events'))
            kf['group_id'] = os.getenv('KAFKA_GROUP_ID', kf.get('group_id', 'sentinelsync-consumer'))
            kf['auto_offset_reset'] = os.getenv('KAFKA_AUTO_OFFSET_RESET', 
                                               kf.get('auto_offset_reset', 'earliest'))
            
            # Optional numeric overrides
            if os.getenv('KAFKA_SESSION_TIMEOUT_MS'):
                kf['session_timeout_ms'] = int(os.getenv('KAFKA_SESSION_TIMEOUT_MS'))
            if os.getenv('KAFKA_MAX_POLL_INTERVAL_MS'):
                kf['max_poll_interval_ms'] = int(os.getenv('KAFKA_MAX_POLL_INTERVAL_MS'))
        
        # Cassandra overrides
        if 'cassandra' in config_data:
            cs = config_data['cassandra']
            
            # Handle hosts (comma-separated list)
            hosts_env = os.getenv('CASSANDRA_HOSTS')
            if hosts_env:
                cs['hosts'] = [h.strip() for h in hosts_env.split(',')]
            
            cs['port'] = int(os.getenv('CASSANDRA_PORT', cs.get('port', 9042)))
            cs['keyspace'] = os.getenv('CASSANDRA_KEYSPACE', cs.get('keyspace', 'sinkdb'))
            cs['replication_factor'] = int(os.getenv('CASSANDRA_REPLICATION_FACTOR', 
                                                     cs.get('replication_factor', 3)))
            cs['username'] = os.getenv('CASSANDRA_USERNAME', cs.get('username'))
            cs['password'] = os.getenv('CASSANDRA_PASSWORD', cs.get('password'))
        
        # App settings overrides
        if 'app' in config_data:
            app = config_data['app']
            app['log_level'] = os.getenv('LOG_LEVEL', app.get('log_level', 'INFO'))
            app['metrics_port'] = int(os.getenv('METRICS_PORT', app.get('metrics_port', 9090)))
            app['health_check_port'] = int(os.getenv('HEALTH_CHECK_PORT', 
                                                     app.get('health_check_port', 8080)))
            app['max_retries'] = int(os.getenv('MAX_RETRIES', app.get('max_retries', 5)))
            app['retry_backoff_seconds'] = int(os.getenv('RETRY_BACKOFF_SECONDS', 
                                                         app.get('retry_backoff_seconds', 2)))
        else:
            # Create app section if it doesn't exist
            config_data['app'] = {
                'log_level': os.getenv('LOG_LEVEL', 'INFO'),
                'metrics_port': int(os.getenv('METRICS_PORT', 9090)),
                'health_check_port': int(os.getenv('HEALTH_CHECK_PORT', 8080)),
                'max_retries': int(os.getenv('MAX_RETRIES', 5)),
                'retry_backoff_seconds': int(os.getenv('RETRY_BACKOFF_SECONDS', 2)),
            }
        
        return config_data
    
    def _create_config(self, config_data: Dict[str, Any]) -> AppConfig:
        """Create AppConfig from dictionary"""
        
        # PostgreSQL config
        pg_data = config_data.get('postgres', {})
        postgres_config = PostgresConfig(
            host=pg_data.get('host', 'localhost'),
            port=pg_data.get('port', 5432),
            database=pg_data.get('database', 'sourcedb'),
            user=pg_data.get('user', 'cdcuser'),
            password=pg_data.get('password', 'cdcpass'),
            replication_slot=pg_data.get('replication_slot', 'sentinelsync_slot'),
            publication=pg_data.get('publication', 'sentinelsync_pub'),
        )
        
        # Kafka config
        kafka_data = config_data.get('kafka', {})
        kafka_config = KafkaConfig(
            bootstrap_servers=kafka_data.get('bootstrap_servers', 'localhost:9092'),
            topic=kafka_data.get('topic', 'cdc.events'),
            group_id=kafka_data.get('group_id', 'sentinelsync-consumer'),
            auto_offset_reset=kafka_data.get('auto_offset_reset', 'earliest'),
            enable_auto_commit=kafka_data.get('enable_auto_commit', False),
            session_timeout_ms=kafka_data.get('session_timeout_ms', 30000),
            max_poll_interval_ms=kafka_data.get('max_poll_interval_ms', 300000),
            compression_type=kafka_data.get('compression_type', 'snappy'),
        )
        
        # Cassandra config
        cassandra_data = config_data.get('cassandra', {})
        cassandra_config = CassandraConfig(
            hosts=cassandra_data.get('hosts', ['localhost']),
            port=cassandra_data.get('port', 9042),
            keyspace=cassandra_data.get('keyspace', 'sinkdb'),
            replication_factor=cassandra_data.get('replication_factor', 3),
            username=cassandra_data.get('username'),
            password=cassandra_data.get('password'),
            protocol_version=cassandra_data.get('protocol_version', 4),
            consistency_level=cassandra_data.get('consistency_level', 'QUORUM'),
        )
        
        # App config
        app_data = config_data.get('app', {})
        return AppConfig(
            postgres=postgres_config,
            kafka=kafka_config,
            cassandra=cassandra_config,
            log_level=app_data.get('log_level', 'INFO'),
            metrics_port=app_data.get('metrics_port', 9090),
            health_check_port=app_data.get('health_check_port', 8080),
            max_retries=app_data.get('max_retries', 5),
            retry_backoff_seconds=app_data.get('retry_backoff_seconds', 2),
        )


# Singleton configuration instance
_config: Optional[AppConfig] = None
_loader: Optional[ConfigLoader] = None


def get_config(config_path: Optional[str] = None, reload: bool = False) -> AppConfig:
    """
    Get the global configuration instance (lazy-loaded singleton).
    
    Args:
        config_path: Optional path to configuration file
        reload: Force reload configuration
        
    Returns:
        AppConfig instance
    """
    global _config, _loader
    
    if reload or _config is None:
        if _loader is None or config_path:
            _loader = ConfigLoader(config_path or "config/app.yaml")
        _config = _loader.load()
    
    return _config


def reload_config(config_path: Optional[str] = None) -> AppConfig:
    """
    Reload configuration from file.
    
    Args:
        config_path: Optional path to configuration file
        
    Returns:
        Reloaded AppConfig instance
    """
    return get_config(config_path, reload=True)


def print_config_summary(config: AppConfig):
    """Print a summary of the configuration (for debugging)"""
    print("\n" + "="*60)
    print("📋 SentinelSync Configuration Summary")
    print("="*60)
    
    print("\n🔵 PostgreSQL:")
    print(f"  Host: {config.postgres.host}:{config.postgres.port}")
    print(f"  Database: {config.postgres.database}")
    print(f"  User: {config.postgres.user}")
    print(f"  Replication Slot: {config.postgres.replication_slot}")
    
    print("\n🟡 Kafka:")
    print(f"  Bootstrap Servers: {config.kafka.bootstrap_servers}")
    print(f"  Topic: {config.kafka.topic}")
    print(f"  Consumer Group: {config.kafka.group_id}")
    
    print("\n🟢 Cassandra:")
    print(f"  Hosts: {', '.join(config.cassandra.hosts)}")
    print(f"  Keyspace: {config.cassandra.keyspace}")
    print(f"  Replication Factor: {config.cassandra.replication_factor}")
    
    print("\n⚙️ Application:")
    print(f"  Log Level: {config.log_level}")
    print(f"  Metrics Port: {config.metrics_port}")
    print(f"  Health Check Port: {config.health_check_port}")
    print(f"  Max Retries: {config.max_retries}")
    print("="*60 + "\n")