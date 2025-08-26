"""
File logging utilities for Domo-to-Snowflake migration.
Provides structured logging to files with different levels and purposes.
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional


class FileLogger:
    """Enhanced file logging for migration operations."""
    
    def __init__(self, base_dir: str = "results/logs", session_timestamp: Optional[str] = None):
        """
        Initialize file logger.
        
        Args:
            base_dir: Base directory for log files (default: results/logs)
            session_timestamp: Fixed timestamp for session (if None, generates new one)
        """
        self.base_dir = Path(base_dir)
        
        # Use provided timestamp or generate new one
        self.session_timestamp = session_timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # If base_dir already contains the timestamp (e.g., results/comparison/YYYYMMDD_HHMMSS/logs)
        # don't create another timestamp subdirectory
        if self.session_timestamp in str(self.base_dir):
            self.session_dir = self.base_dir
        else:
            # Create session-specific directory for legacy structure
            self.session_dir = self.base_dir / self.session_timestamp
        
        self.session_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize loggers
        self.general_logger = None
        self.error_logger = None
        self.comparison_logger = None
        
    def setup_general_logger(self, name: str = "general_execution") -> logging.Logger:
        """
        Setup general execution logger.
        
        Args:
            name: Logger name
            
        Returns:
            Configured logger instance
        """
        if self.general_logger:
            return self.general_logger
            
        # Create general log file in session directory
        log_file = self.session_dir / f"{name}.log"
        
        # Configure logger
        logger = logging.getLogger(f"file_{name}")
        logger.setLevel(logging.INFO)
        
        # Remove existing handlers to avoid duplicates
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # File handler
        file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.propagate = False  # Don't propagate to root logger
        
        self.general_logger = logger
        
        # Log session start
        logger.info("="*80)
        logger.info(f"🚀 Migration session started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📁 Log file: {log_file}")
        logger.info("="*80)
        
        return logger
    
    def setup_error_logger(self, name: str = "errors_and_warnings") -> logging.Logger:
        """
        Setup error-specific logger.
        
        Args:
            name: Logger name
            
        Returns:
            Configured error logger instance
        """
        if self.error_logger:
            return self.error_logger
            
        # Create error log file in session directory
        log_file = self.session_dir / f"{name}.log"
        
        # Configure logger
        logger = logging.getLogger(f"file_{name}")
        logger.setLevel(logging.WARNING)
        
        # Remove existing handlers to avoid duplicates
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # File handler
        file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
        file_handler.setLevel(logging.WARNING)
        
        # Formatter with more detail for errors
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.propagate = False  # Don't propagate to root logger
        
        self.error_logger = logger
        
        # Log session start
        logger.warning("="*80)
        logger.warning(f"❌ Error tracking started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.warning(f"📁 Error log file: {log_file}")
        logger.warning("="*80)
        
        return logger
    
    def setup_comparison_logger(self, name: str = "comparison_details") -> logging.Logger:
        """
        Setup comparison-specific logger.
        
        Args:
            name: Logger name
            
        Returns:
            Configured comparison logger instance
        """
        if self.comparison_logger:
            return self.comparison_logger
            
        # Create comparison log file in session directory
        log_file = self.session_dir / f"{name}.log"
        
        # Configure logger
        logger = logging.getLogger(f"file_{name}")
        logger.setLevel(logging.DEBUG)
        
        # Remove existing handlers to avoid duplicates
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # File handler
        file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
        # Detailed formatter for comparison operations
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.propagate = False  # Don't propagate to root logger
        
        self.comparison_logger = logger
        
        # Log session start
        logger.info("="*80)
        logger.info(f"📊 Comparison details tracking started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📁 Comparison log file: {log_file}")
        logger.info("="*80)
        
        return logger
    
    def log_comparison_start(self, domo_dataset_id: str, snowflake_table: str, 
                           key_columns: list, transform_columns: bool = False):
        """Log comparison operation start."""
        if self.general_logger:
            self.general_logger.info("-"*60)
            self.general_logger.info("📊 COMPARISON STARTED")
            self.general_logger.info(f"   Domo Dataset ID: {domo_dataset_id}")
            self.general_logger.info(f"   Snowflake Table: {snowflake_table}")
            self.general_logger.info(f"   Key Columns: {', '.join(key_columns)}")
            self.general_logger.info(f"   Transform Columns: {transform_columns}")
            self.general_logger.info("-"*60)
        
        # Also log to comparison details
        if not self.comparison_logger:
            self.setup_comparison_logger()
        
        if self.comparison_logger:
            self.comparison_logger.info("="*80)
            self.comparison_logger.info(f"📊 NEW COMPARISON: {domo_dataset_id} vs {snowflake_table}")
            self.comparison_logger.info(f"   Key Columns: {', '.join(key_columns)}")
            self.comparison_logger.info(f"   Transform Columns: {transform_columns}")
            self.comparison_logger.info(f"   Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.comparison_logger.info("="*80)
    
    def log_comparison_result(self, result: dict):
        """Log comparison operation result."""
        if self.general_logger:
            self.general_logger.info("-"*60)
            self.general_logger.info("📈 COMPARISON COMPLETED")
            self.general_logger.info(f"   Overall Match: {result.get('overall_match', 'Unknown')}")
            self.general_logger.info(f"   Schema Match: {result.get('schema_match', 'Unknown')}")
            self.general_logger.info(f"   Data Match: {result.get('data_match', 'Unknown')}")
            if result.get('errors'):
                self.general_logger.warning(f"   Errors Found: {len(result['errors'])}")
            self.general_logger.info("-"*60)
        
        # Detailed logging to comparison logger
        if self.comparison_logger:
            self.comparison_logger.info("-"*80)
            self.comparison_logger.info("📈 COMPARISON RESULT DETAILS")
            self.comparison_logger.info(f"   Dataset: {result.get('domo_dataset_id')} vs {result.get('snowflake_table')}")
            self.comparison_logger.info(f"   Overall Match: {result.get('overall_match')}")
            
            # Schema details
            schema_comp = result.get('schema_comparison', {})
            if schema_comp:
                self.comparison_logger.info("   SCHEMA COMPARISON:")
                self.comparison_logger.info(f"      Match: {schema_comp.get('schema_match')}")
                self.comparison_logger.info(f"      Domo Columns: {schema_comp.get('domo_columns')}")
                self.comparison_logger.info(f"      Snowflake Columns: {schema_comp.get('snowflake_columns')}")
                if schema_comp.get('missing_in_snowflake'):
                    self.comparison_logger.warning(f"      Missing in Snowflake: {schema_comp.get('missing_in_snowflake')}")
                if schema_comp.get('extra_in_snowflake'):
                    self.comparison_logger.warning(f"      Extra in Snowflake: {schema_comp.get('extra_in_snowflake')}")
            
            # Row count details
            row_comp = result.get('row_count_comparison', {})
            if row_comp:
                self.comparison_logger.info("   ROW COUNT COMPARISON:")
                self.comparison_logger.info(f"      Domo Rows: {row_comp.get('domo_rows')}")
                self.comparison_logger.info(f"      Snowflake Rows: {row_comp.get('snowflake_rows')}")
                self.comparison_logger.info(f"      Difference: {row_comp.get('difference')}")
                
            # Data comparison details
            data_comp = result.get('data_comparison', {})
            if data_comp:
                self.comparison_logger.info("   DATA COMPARISON:")
                self.comparison_logger.info(f"      Sample Size: {data_comp.get('sample_size')}")
                self.comparison_logger.info(f"      Data Match: {data_comp.get('data_match')}")
                self.comparison_logger.info(f"      Missing in Snowflake: {data_comp.get('missing_in_snowflake')}")
                self.comparison_logger.info(f"      Extra in Snowflake: {data_comp.get('extra_in_snowflake')}")
                self.comparison_logger.info(f"      Rows with Differences: {data_comp.get('rows_with_differences')}")
            
            self.comparison_logger.info("-"*80)
    
    def log_error(self, error_type: str, context: str, error_message: str):
        """Log error to both general and error logs."""
        error_msg = f"[{error_type}] {context}: {error_message}"
        
        if self.general_logger:
            self.general_logger.error(error_msg)
        
        if self.error_logger:
            self.error_logger.error(error_msg)
    
    def log_batch_failure(self, batch_num: int, domo_rows: int, snowflake_rows: int, 
                         error: str):
        """Log batch processing failure."""
        if self.error_logger:
            self.error_logger.error(f"BATCH_FAILURE - Batch {batch_num}: "
                                  f"Domo={domo_rows} rows, Snowflake={snowflake_rows} rows, "
                                  f"Error: {error}")
    
    def close_loggers(self):
        """Close all file handlers."""
        if self.general_logger:
            self.general_logger.info("="*80)
            self.general_logger.info(f"📝 Migration session ended: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.general_logger.info(f"📁 Session logs saved in: {self.session_dir}")
            self.general_logger.info("="*80)
            
            for handler in self.general_logger.handlers:
                handler.close()
        
        if self.error_logger:
            self.error_logger.warning("="*80)
            self.error_logger.warning(f"❌ Error tracking ended: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.error_logger.warning("="*80)
            
            for handler in self.error_logger.handlers:
                handler.close()
        
        if self.comparison_logger:
            self.comparison_logger.info("="*80)
            self.comparison_logger.info(f"📊 Comparison details tracking ended: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.comparison_logger.info("="*80)
            
            for handler in self.comparison_logger.handlers:
                handler.close()


# Global file logger instance and session management
_file_logger_instance: Optional[FileLogger] = None
_current_session_timestamp: Optional[str] = None


def start_logging_session(session_name: str = None) -> str:
    """
    Start a new logging session with a fixed timestamp.
    
    Args:
        session_name: Optional session name (defaults to auto-generated)
    
    Returns:
        Session timestamp string
    """
    global _current_session_timestamp, _file_logger_instance
    
    # Generate session timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    _current_session_timestamp = timestamp
    
    # Create comparison directory structure
    comparison_dir = f"results/comparison/{timestamp}"
    logs_dir = f"{comparison_dir}/logs"
    
    # Reset the file logger instance to use the new session timestamp and directory
    if _file_logger_instance:
        _file_logger_instance.close_loggers()
    _file_logger_instance = FileLogger(base_dir=logs_dir, session_timestamp=timestamp)
    
    # Initialize all loggers
    _file_logger_instance.setup_general_logger()
    _file_logger_instance.setup_error_logger()
    _file_logger_instance.setup_comparison_logger()
    
    # Log session start
    logger = _file_logger_instance.general_logger
    logger.info(f"🎬 LOGGING SESSION STARTED: {session_name or 'Default'}")
    logger.info(f"📅 Session Timestamp: {timestamp}")
    logger.info("="*80)
    
    return timestamp


def end_logging_session():
    """End the current logging session."""
    global _file_logger_instance, _current_session_timestamp
    
    if _file_logger_instance:
        # Log session end
        if _file_logger_instance.general_logger:
            _file_logger_instance.general_logger.info("="*80)
            _file_logger_instance.general_logger.info("🎬 LOGGING SESSION ENDED")
            _file_logger_instance.general_logger.info("="*80)
        
        _file_logger_instance.close_loggers()
        _file_logger_instance = None
    
    _current_session_timestamp = None


def get_file_logger() -> FileLogger:
    """Get or create global file logger instance with current session timestamp."""
    global _file_logger_instance, _current_session_timestamp
    
    if _file_logger_instance is None:
        _file_logger_instance = FileLogger(session_timestamp=_current_session_timestamp)
    
    return _file_logger_instance


def get_current_session_timestamp() -> Optional[str]:
    """Get the current session timestamp."""
    return _current_session_timestamp


def configure_comparison_logging(comparison_dir: str, session_timestamp: str) -> FileLogger:
    """
    Configure logging to use a specific comparison directory.
    
    Args:
        comparison_dir: Base directory for the comparison (e.g., results/comparison/YYYYMMDD_HHMMSS)
        session_timestamp: Session timestamp to use
        
    Returns:
        Configured FileLogger instance
    """
    logs_dir = f"{comparison_dir}/logs"
    return FileLogger(base_dir=logs_dir, session_timestamp=session_timestamp)


def setup_file_logging() -> tuple[logging.Logger, logging.Logger]:
    """
    Setup file logging for the application.
    
    Returns:
        Tuple of (general_logger, error_logger)
    """
    file_logger = get_file_logger()
    general_logger = file_logger.setup_general_logger()
    error_logger = file_logger.setup_error_logger()
    comparison_logger = file_logger.setup_comparison_logger()
    
    # Log session info
    if general_logger:
        general_logger.info(f"📁 All logs for this session are saved in: {file_logger.session_dir}")
        general_logger.info(f"   - general_execution.log: Overall execution flow")
        general_logger.info(f"   - errors_and_warnings.log: All errors and warnings")
        general_logger.info(f"   - comparison_details.log: Detailed comparison results")
    
    return general_logger, error_logger


def close_file_logging():
    """Close all file logging handlers."""
    end_logging_session()
