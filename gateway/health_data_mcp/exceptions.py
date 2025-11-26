"""Custom exceptions for health data MCP server."""

from typing import Any


class ValidationError(Exception):
    """파라미터 검증 오류
    
    잘못된 파라미터 형식, 범위, 또는 필수 파라미터 누락 시 발생합니다.
    
    Attributes:
        message: 오류 메시지
        details: 오류 상세 정보 (선택적)
    """
    
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        """ValidationError 초기화
        
        Args:
            message: 오류 메시지
            details: 오류 상세 정보 (field, provided, expected 등)
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}
    
    def to_dict(self) -> dict[str, Any]:
        """에러를 딕셔너리로 변환
        
        Returns:
            에러 정보 딕셔너리
        """
        error_dict: dict[str, Any] = {
            "type": "ValidationError",
            "message": self.message
        }
        if self.details:
            error_dict["details"] = self.details
        return {"error": error_dict}
