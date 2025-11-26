"""Structured logging configuration for Health Data MCP Server."""

import logging
import logging.config
import json
import os
import sys
from typing import Any
from datetime import datetime


class SensitiveDataFilter(logging.Filter):
    """로그에서 민감한 정보를 마스킹하는 필터
    
    비밀번호, 토큰, API 키 등의 민감한 정보를 자동으로 마스킹합니다.
    """
    
    # 마스킹할 필드 이름 패턴
    SENSITIVE_FIELDS = {
        "password", "passwd", "pwd",
        "secret", "secret_key", "secret-key",
        "token", "access_token", "refresh_token",
        "api_key", "api-key", "apikey",
        "access_key", "access-key",
        "s3_secret_key", "s3-secret-key",
    }
    
    def filter(self, record: logging.LogRecord) -> bool:
        """로그 레코드에서 민감한 정보 마스킹
        
        Args:
            record: 로그 레코드
            
        Returns:
            True (항상 로그를 통과시킴)
        """
        # 메시지 마스킹
        if hasattr(record, 'msg') and isinstance(record.msg, str):
            record.msg = self._mask_sensitive_data(record.msg)
        
        # args 마스킹
        if hasattr(record, 'args') and record.args:
            if isinstance(record.args, dict):
                record.args = self._mask_dict(record.args)
            elif isinstance(record.args, (list, tuple)):
                record.args = tuple(
                    self._mask_dict(arg) if isinstance(arg, dict) else arg
                    for arg in record.args
                )
        
        return True
    
    def _mask_sensitive_data(self, text: str) -> str:
        """텍스트에서 민감한 데이터 마스킹
        
        Args:
            text: 원본 텍스트
            
        Returns:
            마스킹된 텍스트
        """
        # 간단한 패턴 매칭으로 민감한 정보 마스킹
        for field in self.SENSITIVE_FIELDS:
            # key=value 형식 마스킹
            if f"{field}=" in text.lower():
                parts = text.split(f"{field}=")
                if len(parts) > 1:
                    # 값 부분을 마스킹 (공백이나 쉼표까지)
                    value_part = parts[1].split()[0].split(',')[0]
                    text = text.replace(f"{field}={value_part}", f"{field}=***MASKED***")
            
            # "key": "value" 형식 마스킹 (JSON)
            if f'"{field}"' in text.lower() or f"'{field}'" in text.lower():
                # 간단한 JSON 값 마스킹
                import re
                pattern = rf'(["\']){field}\1\s*:\s*["\']([^"\']+)["\']'
                text = re.sub(pattern, rf'\1{field}\1: "***MASKED***"', text, flags=re.IGNORECASE)
        
        return text
    
    def _mask_dict(self, data: dict) -> dict:
        """딕셔너리에서 민감한 필드 마스킹
        
        Args:
            data: 원본 딕셔너리
            
        Returns:
            마스킹된 딕셔너리 (복사본)
        """
        masked = data.copy()
        for key in masked:
            if key.lower().replace('_', '').replace('-', '') in {
                f.replace('_', '').replace('-', '') for f in self.SENSITIVE_FIELDS
            }:
                masked[key] = "***MASKED***"
            elif isinstance(masked[key], dict):
                masked[key] = self._mask_dict(masked[key])
        return masked


class JSONFormatter(logging.Formatter):
    """JSON 형식으로 로그를 포맷팅하는 포매터
    
    구조화된 로그를 JSON 형식으로 출력하여 로그 분석 도구와의 통합을 용이하게 합니다.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """로그 레코드를 JSON 형식으로 포맷팅
        
        Args:
            record: 로그 레코드
            
        Returns:
            JSON 형식의 로그 문자열
        """
        log_data = {
            "timestamp": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # 추가 컨텍스트 정보
        if hasattr(record, 'user_id'):
            log_data['user_id'] = record.user_id
        if hasattr(record, 'data_type'):
            log_data['data_type'] = record.data_type
        if hasattr(record, 'tool_name'):
            log_data['tool_name'] = record.tool_name
        
        # 예외 정보 추가
        if record.exc_info:
            log_data['exception'] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": self.formatException(record.exc_info) if record.exc_info else None
            }
        
        return json.dumps(log_data, ensure_ascii=False)


class HumanReadableFormatter(logging.Formatter):
    """사람이 읽기 쉬운 형식으로 로그를 포맷팅하는 포매터
    
    개발 환경에서 사용하기 적합한 가독성 높은 로그 형식을 제공합니다.
    """
    
    # 로그 레벨별 색상 코드 (ANSI)
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
    }
    RESET = '\033[0m'
    
    def __init__(self, use_colors: bool = True):
        """HumanReadableFormatter 초기화
        
        Args:
            use_colors: 색상 사용 여부 (기본값: True)
        """
        super().__init__(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.use_colors = use_colors and sys.stdout.isatty()
    
    def format(self, record: logging.LogRecord) -> str:
        """로그 레코드를 사람이 읽기 쉬운 형식으로 포맷팅
        
        Args:
            record: 로그 레코드
            
        Returns:
            포맷팅된 로그 문자열
        """
        # 기본 포맷팅
        formatted = super().format(record)
        
        # 색상 적용
        if self.use_colors and record.levelname in self.COLORS:
            color = self.COLORS[record.levelname]
            formatted = f"{color}{formatted}{self.RESET}"
        
        # 예외 정보 추가
        if record.exc_info:
            formatted += "\n" + self.formatException(record.exc_info)
        
        return formatted


def setup_logging(
    log_level: str | None = None,
    log_format: str | None = None,
    log_file: str | None = None
) -> None:
    """로깅 설정 초기화
    
    환경 변수를 기반으로 로깅을 설정합니다.
    개발 모드에서는 사람이 읽기 쉬운 형식을, 프로덕션에서는 JSON 형식을 사용합니다.
    
    Args:
        log_level: 로그 레벨 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
                   None이면 환경 변수 LOG_LEVEL 사용 (기본값: INFO)
        log_format: 로그 형식 (json, human)
                    None이면 환경 변수 LOG_FORMAT 사용 (기본값: human)
        log_file: 로그 파일 경로 (선택적)
                  None이면 환경 변수 LOG_FILE 사용
    
    Environment Variables:
        LOG_LEVEL: 로그 레벨 (기본값: INFO)
        LOG_FORMAT: 로그 형식 (기본값: human)
        LOG_FILE: 로그 파일 경로 (선택적)
        ENVIRONMENT: 환경 (development, production 등)
    """
    # 환경 변수에서 설정 읽기
    log_level = log_level or os.getenv("LOG_LEVEL", "INFO").upper()
    log_format = log_format or os.getenv("LOG_FORMAT", "human").lower()
    log_file = log_file or os.getenv("LOG_FILE")
    environment = os.getenv("ENVIRONMENT", "development").lower()
    
    # 개발 환경에서는 DEBUG 레벨 사용
    if environment == "development" and log_level == "INFO":
        log_level = "DEBUG"
    
    # 로그 레벨 검증
    numeric_level = getattr(logging, log_level, logging.INFO)
    
    # 포매터 선택
    if log_format == "json":
        formatter = JSONFormatter()
    else:
        formatter = HumanReadableFormatter(use_colors=True)
    
    # 핸들러 설정
    handlers = []
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(SensitiveDataFilter())
    handlers.append(console_handler)
    
    # 파일 핸들러 (선택적)
    if log_file:
        try:
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(numeric_level)
            # 파일에는 항상 JSON 형식 사용
            file_handler.setFormatter(JSONFormatter())
            file_handler.addFilter(SensitiveDataFilter())
            handlers.append(file_handler)
        except Exception as e:
            print(f"Warning: Failed to create log file handler: {e}", file=sys.stderr)
    
    # 루트 로거 설정
    logging.basicConfig(
        level=numeric_level,
        handlers=handlers,
        force=True  # 기존 설정 덮어쓰기
    )
    
    # 외부 라이브러리 로그 레벨 조정
    logging.getLogger("pyiceberg").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    
    # 설정 완료 로그
    logger = logging.getLogger(__name__)
    logger.info(
        f"Logging configured: level={log_level}, format={log_format}, "
        f"environment={environment}"
    )
    if log_file:
        logger.info(f"Logging to file: {log_file}")


def get_logger(name: str) -> logging.Logger:
    """로거 인스턴스 가져오기
    
    Args:
        name: 로거 이름 (일반적으로 __name__ 사용)
        
    Returns:
        로거 인스턴스
    """
    return logging.getLogger(name)


def log_tool_call(
    logger: logging.Logger,
    tool_name: str,
    params: dict[str, Any],
    level: int = logging.INFO
) -> None:
    """도구 호출 로깅
    
    Args:
        logger: 로거 인스턴스
        tool_name: 도구 이름
        params: 도구 파라미터
        level: 로그 레벨 (기본값: INFO)
    """
    # 민감한 정보 마스킹
    safe_params = SensitiveDataFilter()._mask_dict(params)
    
    logger.log(
        level,
        f"Tool called: {tool_name}",
        extra={
            "tool_name": tool_name,
            "params": safe_params
        }
    )


def log_tool_result(
    logger: logging.Logger,
    tool_name: str,
    result_count: int,
    duration_ms: float | None = None,
    level: int = logging.INFO
) -> None:
    """도구 실행 결과 로깅
    
    Args:
        logger: 로거 인스턴스
        tool_name: 도구 이름
        result_count: 결과 레코드 수
        duration_ms: 실행 시간 (밀리초, 선택적)
        level: 로그 레벨 (기본값: INFO)
    """
    message = f"Tool completed: {tool_name} - {result_count} records"
    if duration_ms is not None:
        message += f" ({duration_ms:.2f}ms)"
    
    logger.log(
        level,
        message,
        extra={
            "tool_name": tool_name,
            "result_count": result_count,
            "duration_ms": duration_ms
        }
    )


def log_error(
    logger: logging.Logger,
    error: Exception,
    context: dict[str, Any] | None = None,
    level: int = logging.ERROR
) -> None:
    """에러 로깅 (스택 트레이스 포함)
    
    Args:
        logger: 로거 인스턴스
        error: 예외 객체
        context: 추가 컨텍스트 정보 (선택적)
        level: 로그 레벨 (기본값: ERROR)
    """
    message = f"{error.__class__.__name__}: {str(error)}"
    
    extra = {
        "error_type": error.__class__.__name__,
        "error_message": str(error)
    }
    
    if context:
        # 민감한 정보 마스킹
        safe_context = SensitiveDataFilter()._mask_dict(context)
        extra["context"] = safe_context
    
    logger.log(level, message, exc_info=True, extra=extra)
