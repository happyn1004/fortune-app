# GitHub 업로드용 안내

이 폴더는 GitHub 업로드에 맞게 정리된 버전입니다.

## 업로드 전 정리된 항목
- `.venv` 제거
- `__pycache__` 제거
- 로컬 DB(`app/fortune.db`) 제거
- `.gitignore` 추가
- `.env.example` 추가
- `SESSION_SECRET`, `DATA_DIR` 환경변수 지원 추가

## GitHub에 올리는 방법
1. 이 폴더를 압축 해제
2. GitHub 새 저장소 생성
3. 폴더 전체를 업로드
4. Render / Railway / Netlify가 아니라 **Python 서버 배포 가능한 곳**에 연결

## 로컬 실행
```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
```

## 환경변수 예시
- `SESSION_SECRET`: 세션 암호키
- `DATA_DIR`: DB/백업 저장 위치
- `PORT`: 배포 환경 포트

---

# Mystic Day v8.7 운영안정화 + 결제/백업/배포 준비팩

## 이번 버전 핵심
- 관리자 필수정보 4개를 큰 스크롤 박스로 편집
- 관리자에서 수정한 약관/개인정보/환불/고객센터 문구가 고객용 페이지에 그대로 반영
- APPDATA 영구 DB 저장으로 업데이트 후에도 회원/결제/문의/설정 유지
- 시작 시 자동 백업 생성
- 관리자에서 수동 백업 / DB 다운로드 / DB 복원 지원
- 회원 CSV / 결제 CSV 다운로드 지원
- TESTPG 결제 성공 시 등급 자동 반영
- 무통장입금 신청 후 관리자 입금 확인 시 등급 자동 반영
- 계좌이체 신청 오류 수정 (payments 테이블 컬럼 자동 보정)

## 로컬 실행
1. 압축 해제
2. `run_server.bat` 실행
3. 브라우저에서 `http://127.0.0.1:8000` 접속

## 운영용 실행
- Windows: `run_production.bat`
- Linux/Docker: `docker-compose.yml` 또는 `deploy_ubuntu.md` 참고

## 데이터 위치
- 기본 DB 위치: `%APPDATA%\MysticDay\fortune.db`
- 자동/수동 백업 위치: `%APPDATA%\MysticDay\backups\`

## 관리자 데이터 관리
- `/admin` → 데이터 백업 · 복원 · 배포 준비 섹션
- 즉시 백업 생성
- 현재 DB 다운로드
- DB 복원 업로드
- 회원 CSV 다운로드
- 결제 CSV 다운로드

## 결제 테스트
1. 회원 로그인
2. `/plans` 에서 Basic/Premium/VIP 선택
3. TESTPG 또는 BANK 선택
4. TESTPG는 즉시 성공 처리 가능
5. BANK는 입금자명 입력 후 신청 → 관리자에서 입금 확인

## 관리자 계정
- 이메일: `admin@fortune.local`
- 비밀번호: `admin1234`

## 실제 배포 준비 포인트
- 도메인 연결
- Nginx 리버스 프록시
- SSL 적용
- Docker 볼륨 유지
- 정기 백업 파일 별도 보관


## 누구나 접속 가능한 공개 배포
- 가장 쉬운 방법: `Render` 또는 `Railway`
- 배포용 설정 파일: `render.yaml`, `railway.json`, `Procfile`, `start_public.sh`
- 자세한 순서: `DEPLOY_PUBLIC.md`
- 공개 후 웹 주소만 공유하면 누구나 접속 가능
- 현재 구조는 PWA 포함이라 앱처럼 설치도 가능
