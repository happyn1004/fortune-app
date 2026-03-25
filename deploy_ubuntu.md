# Mystic Day 배포 가이드 (Ubuntu/Nginx)

## 1. 서버 준비
- Ubuntu 22.04 이상
- 도메인 연결
- 80/443 포트 개방

## 2. Docker 방식 권장
```bash
sudo apt update
sudo apt install -y docker.io docker-compose-plugin
cd mysticday
sudo docker compose up -d --build
```

## 3. Nginx 리버스 프록시 예시
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

## 4. SSL 적용
```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d your-domain.com
```

## 5. 핵심 데이터 위치
- 앱 DB: `/data/fortune.db`
- 자동 백업: `/data/backups/`

즉, 컨테이너를 새로 올려도 볼륨을 유지하면 회원/결제/설정 데이터가 유지됩니다.
