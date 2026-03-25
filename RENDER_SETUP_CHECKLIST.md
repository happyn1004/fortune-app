# Render 설정 체크리스트

1. Render 서비스 > Settings > Disks 에서 영구 디스크가 연결되어 있는지 확인
2. Disk mount path 는 /data 로 설정
3. Environment 에 DATA_DIR=/data/mysticday 추가
4. SESSION_SECRET 는 길고 랜덤한 값으로 설정
5. Save, rebuild, and deploy 실행
6. 배포 후 /admin 에서 데이터 저장 경로와 회원 보관 상태 확인

## 로그인/회원 관련 핵심 수정
- 이메일을 공백 제거 + 소문자로 통일 저장
- 로그인 조회도 소문자/공백 무시 방식으로 통일
- 기존 회원 이메일도 시작 시 자동 정규화
