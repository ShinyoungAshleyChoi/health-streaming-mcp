# iOS 앱에서 셀프사인드 인증서 HTTPS 연결 설정

## 문제
iOS 앱에서 셀프사인드 인증서를 사용하는 HTTPS 서버에 연결 시 다음 오류 발생:
```
Error Domain=NSURLErrorDomain Code=-1200 "A TLS error caused the secure connection to fail."
```

## 해결 방법: Info.plist에 ATS 예외 추가

### 1. Info.plist 파일 수정

iOS 프로젝트의 `Info.plist` 파일에 다음 내용을 추가:

```xml
<key>NSAppTransportSecurity</key>
<dict>
    <key>NSExceptionDomains</key>
    <dict>
        <key>192.168.45.185</key>
        <dict>
            <key>NSExceptionAllowsInsecureHTTPLoads</key>
            <true/>
            <key>NSExceptionRequiresForwardSecrecy</key>
            <false/>
            <key>NSIncludesSubdomains</key>
            <true/>
        </dict>
    </dict>
</dict>
```

### 2. Xcode에서 직접 추가하는 방법

1. Xcode에서 프로젝트 열기
2. 프로젝트 네비게이터에서 `Info.plist` 파일 선택
3. 빈 공간에서 우클릭 → "Add Row" 선택
4. 다음 계층 구조로 추가:

```
App Transport Security Settings (Dictionary)
  └─ Exception Domains (Dictionary)
      └─ 192.168.45.185 (Dictionary)
          ├─ NSExceptionAllowsInsecureHTTPLoads (Boolean) = YES
          ├─ NSExceptionRequiresForwardSecrecy (Boolean) = NO
          └─ NSIncludesSubdomains (Boolean) = YES
```

### 3. 서버 IP가 변경되는 경우

개발 중 서버 IP가 자주 변경된다면, 로컬 네트워크 전체를 허용할 수도 있습니다:

```xml
<key>NSAppTransportSecurity</key>
<dict>
    <key>NSAllowsLocalNetworking</key>
    <true/>
    <key>NSExceptionDomains</key>
    <dict>
        <key>192.168.45.185</key>
        <dict>
            <key>NSExceptionAllowsInsecureHTTPLoads</key>
            <true/>
            <key>NSExceptionRequiresForwardSecrecy</key>
            <false/>
        </dict>
    </dict>
</dict>
```

## 서버 설정

### HTTPS 활성화

1. 셀프사인드 인증서 생성:
```bash
cd gateway
./generate_cert.sh
```

2. `.env` 파일에서 SSL 활성화:
```bash
SSL_ENABLED=true
SSL_CERTFILE=./certs/cert.pem
SSL_KEYFILE=./certs/key.pem
```

3. Docker Compose 재시작:
```bash
docker-compose restart gateway
```

### 서버 URL 확인

iOS 앱에서 사용할 URL:
```
https://192.168.45.185:3000/api/v1/health-data
```

## 주의사항

⚠️ **보안 경고**
- 이 설정은 **개발 환경 전용**입니다
- 프로덕션 앱에서는 절대 사용하지 마세요
- App Store 제출 시 이 설정을 제거하거나 프로덕션 도메인만 허용하도록 변경하세요

### 프로덕션 환경 권장 사항

프로덕션에서는:
1. 공인 인증 기관(CA)에서 발급한 SSL 인증서 사용
2. Let's Encrypt 등의 무료 인증서 서비스 활용
3. ATS 예외 설정 제거 또는 최소화

## 테스트

설정 후 테스트:

```bash
# 서버 상태 확인
curl -k https://192.168.45.185:3000/health

# iOS 앱에서 데이터 전송 테스트
# 앱을 실행하고 데이터 동기화 시도
```

## 문제 해결

### 여전히 연결 실패하는 경우

1. **서버가 HTTPS로 실행 중인지 확인**:
```bash
docker-compose logs gateway | grep SSL
```

2. **방화벽 확인**:
```bash
# macOS
sudo pfctl -s all | grep 3000
```

3. **iOS 앱 재빌드**:
   - Xcode에서 Clean Build Folder (Cmd+Shift+K)
   - 앱 재빌드 및 재설치

4. **서버 로그 확인**:
```bash
docker-compose logs -f gateway
```

### 다른 IP 주소로 테스트

서버 IP 확인:
```bash
ifconfig | grep "inet " | grep -v 127.0.0.1
```

iOS 앱의 서버 URL을 해당 IP로 업데이트하고 `Info.plist`의 도메인도 변경하세요.
