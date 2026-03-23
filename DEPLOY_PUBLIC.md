# 누구나 접속 가능한 웹 공개 배포 가이드

이 프로젝트는 **FastAPI + SQLite + PWA** 구조입니다.
가장 빠른 공개 방법은 아래 2가지입니다.

## 1) Render로 공개하기
1. GitHub에 이 폴더 전체 업로드
2. Render에서 `New +` → `Blueprint` 선택
3. 저장소 연결
4. `render.yaml` 자동 인식
5. 배포 완료 후 발급된 URL 접속
6. 커스텀 도메인 연결 가능

### Render 운영 포인트
- 영구 데이터는 `/data` 디스크에 저장됩니다.
- 관리자 계정은 첫 실행 후 `/admin`에서 로그인해서 비밀번호를 바로 변경하세요.
- 광고/이미지 업로드, 회원, 결제, 설정이 디스크에 유지됩니다.

## 2) Railway로 공개하기
1. GitHub에 업로드
2. Railway에서 `New Project` → `Deploy from GitHub repo`
3. 서비스 생성 후 `Variables`에서 `APPDATA=/data` 추가
4. `Volumes` 또는 영구 스토리지 연결
5. Start Command는 아래로 지정

```bash
sh start_public.sh
```

## 3) Ubuntu VPS에 직접 공개하기
### Docker로 실행
```bash
sudo apt update
sudo apt install -y docker.io docker-compose-plugin
sudo mkdir -p /opt/mysticday
cd /opt/mysticday
# 여기에 프로젝트 업로드
sudo docker compose up -d --build
```

### Nginx 연결
```nginx
server {
    server_name your-domain.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### SSL 적용
```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d your-domain.com
```

## 4) 앱처럼 보이게 하는 방법
이 프로젝트는 PWA가 포함되어 있습니다.
- 안드로이드/크롬: 브라우저 메뉴 → 홈 화면에 추가
- 아이폰/사파리: 공유 → 홈 화면에 추가

즉, 별도 앱스토어 등록 전에도 **누구나 웹 링크로 접속**하고 **앱처럼 설치**할 수 있습니다.

## 5) 앱스토어/플레이스토어로 확장하려면
- 1단계: 웹 공개 배포
- 2단계: PWA 테스트
- 3단계: 웹뷰 래핑 또는 네이티브 패키징
- 4단계: 인앱결제 정책에 맞춰 결제 분리

## 6) 오픈 직후 꼭 바꿔야 할 것
- 관리자 비밀번호
- 사이트명/고객센터 정보
- 도메인
- 결제 수단 실제 키
- 푸시 문구
- 광고 이미지
