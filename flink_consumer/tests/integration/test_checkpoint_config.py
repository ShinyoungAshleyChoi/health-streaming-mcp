"""Integration tests for checkpoint and recovery configuration"""

import pytest
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode


class TestCheckpointConfiguration:
    """Integration tests for checkpoint and recovery setup"""

    @pytest.fixture
    def env(self):
        """Create Flink execution environment"""
        env = StreamExecutionEnvironment.get_execution_environment()
        return env

    def test_checkpoint_configuration(self, env):
        """Test that checkpoint can be configured"""
        # Enable checkpointing
        env.enable_checkpointing(60000)  # 60 seconds
        
        # Get checkpoint config
        checkpoint_config = env.get_checkpoint_config()
        
        # Configure checkpoint settings
        checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
        checkpoint_config.set_min_pause_between_checkpoints(30000)
        checkpoint_config.set_checkpoint_timeout(600000)
        checkpoint_config.set_max_concurrent_checkpoints(1)
        
        # Verify configuration was applied
        assert checkpoint_config.get_checkpointing_mode() == CheckpointingMode.EXACTLY_ONCE
        assert checkpoint_config.get_min_pause_between_checkpoints() == 30000
        assert checkpoint_config.get_checkpoint_timeout() == 600000
        assert checkpoint_config.get_max_concurrent_checkpoints() == 1

    def test_state_backend_configuration(self, env):
        """Test that state backend can be configured"""
        # Set state backend
        env.set_state_backend('rocksdb')
        
        # Configuration should not raise errors
        assert True

    def test_checkpoint_with_data_stream(self, env):
        """Test checkpoint configuration with actual data stream"""
        # Enable checkpointing
        env.enable_checkpointing(60000)
        checkpoint_config = env.get_checkpoint_config()
        checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
        
        # Create simple data stream
        test_data = [1, 2, 3, 4, 5]
        source = env.from_collection(test_data)
        
        # Apply simple transformation
        result = source.map(lambda x: x * 2)
        
        # Collect results
        results = list(result.execute_and_collect())
        
        assert results == [2, 4, 6, 8, 10]

    def test_externalized_checkpoints(self, env):
        """Test externalized checkpoint configuration"""
        env.enable_checkpointing(60000)
        checkpoint_config = env.get_checkpoint_config()
        
        # Enable externalized checkpoints
        checkpoint_config.enable_externalized_checkpoints(
            checkpoint_config.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        )
        
        # Configuration should be applied without errors
        assert True

    def test_checkpoint_interval_validation(self, env):
        """Test that checkpoint interval is properly set"""
        # Set different checkpoint intervals
        intervals = [10000, 30000, 60000, 120000]
        
        for interval in intervals:
            env.enable_checkpointing(interval)
            checkpoint_config = env.get_checkpoint_config()
            
            # Verify interval is set (Flink may adjust it)
            assert checkpoint_config.get_checkpoint_interval() >= interval


class TestRecoveryConfiguration:
    """Integration tests for recovery strategy configuration"""

    @pytest.fixture
    def env(self):
        """Create Flink execution environment"""
        env = StreamExecutionEnvironment.get_execution_environment()
        return env

    def test_restart_strategy_fixed_delay(self, env):
        """Test fixed delay restart strategy configuration"""
        # Configure fixed delay restart strategy
        env.set_restart_strategy(
            env.get_config().set_restart_strategy_fixed_delay_restart(
                restart_attempts=3,
                delay_between_attempts=10000
            )
        )
        
        # Configuration should be applied without errors
        assert True

    def test_restart_strategy_failure_rate(self, env):
        """Test failure rate restart strategy configuration"""
        # Configure failure rate restart strategy
        env.set_restart_strategy(
            env.get_config().set_restart_strategy_failure_rate_restart(
                max_failures_per_interval=3,
                failure_rate_interval=300000,
                delay_between_attempts=10000
            )
        )
        
        # Configuration should be applied without errors
        assert True

    def test_combined_checkpoint_and_recovery(self, env):
        """Test combined checkpoint and recovery configuration"""
        # Enable checkpointing
        env.enable_checkpointing(60000)
        checkpoint_config = env.get_checkpoint_config()
        checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
        
        # Configure restart strategy
        env.set_restart_strategy(
            env.get_config().set_restart_strategy_fixed_delay_restart(
                restart_attempts=3,
                delay_between_attempts=10000
            )
        )
        
        # Create and execute simple pipeline
        test_data = [1, 2, 3]
        source = env.from_collection(test_data)
        result = source.map(lambda x: x + 1)
        
        results = list(result.execute_and_collect())
        assert results == [2, 3, 4]
