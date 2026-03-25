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


## Render Free 운영 시 수정/배포 안전 절차
1. 관리자 `/admin` 접속
2. **현재 DB 다운로드** 또는 **즉시 백업 생성** 실행
3. GitHub `fortune-app` 저장소 안의 파일 수정 후 커밋
4. Render 자동 배포 또는 Manual Deploy 확인
5. 데이터가 초기화되었으면 관리자에서 `.db` 백업 파일 업로드 후 복원

### 꼭 기억할 점
- **GitHub는 코드 보관용**입니다.
- **회원/결제/문의/설정 데이터는 DB 백업 파일로 따로 보호**해야 합니다.
- Free 플랜은 Persistent Disk가 없으므로, 중요한 수정 전에는 관리자에서 반드시 백업을 내려받는 것을 권장합니다.

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


## NICEPAY 카드결제 설정
- 운영 화면에서는 테스트결제 버튼을 제거하고 카드결제/계좌이체만 노출되도록 수정했습니다.
- Render Environment에 `NICEPAY_MID`, `NICEPAY_CLIENT_KEY`, `NICEPAY_SECRET_KEY`, `NICEPAY_MERCHANT_KEY`, `NICEPAY_RETURN_BASE_URL`를 추가합니다.
- `NICEPAY_RETURN_BASE_URL`에는 실제 서비스 주소(예: onrender 도메인)를 넣습니다.
- 운영 전에는 외부에 노출된 키를 재발급한 뒤 새 키를 넣어 주세요.

## Render에서 회원정보가 안 날아가게 하는 필수 설정
1. Render 서비스에서 **Disks** 또는 **Add Disk**로 영구 디스크를 1개 추가합니다.
2. 마운트 경로는 `/var/data` 또는 `/data` 중 하나로 설정합니다.
3. Environment에 아래 중 하나를 넣습니다.
   - `DATA_DIR=/var/data/mysticday`
   - 또는 `RENDER_DISK_PATH=/var/data`
4. 저장 시 **Save, rebuild, and deploy**를 누릅니다.
5. 배포 후 관리자 `/admin` 상단의 **회원/결제 데이터 보관 상태**가 초록색이면 정상입니다.

### 권장 체크
- 업데이트 전: 관리자 → 전체 백업 다운로드(.zip)
- 업데이트 후: 같은 회원으로 로그인되는지 확인
- 신규 회원 가입 후: 재배포 후에도 회원/출석/결제내역이 그대로 남는지 확인


## 이번 수정 반영
- 모바일/PC 로그인 불일치 방지를 위해 이메일을 소문자+공백제거 기준으로 통일
- 기존 회원 이메일도 앱 시작 시 자동 정규화
- Render 영구 저장 경로를 DATA_DIR=/data/mysticday 기준으로 통일
- Render 대시보드에서 Disk + Environment 설정이 반드시 필요
