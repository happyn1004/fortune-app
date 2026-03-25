# Netlify 빠른 첫화면 쉘

이 폴더는 Render 무료 플랜의 waking 화면 대신 먼저 보여줄 수 있는 정적 첫화면입니다.

## 사용 순서
1. `shell-config.js`의 `APP_BASE_URL`을 실제 Render 주소로 수정
2. 이 폴더를 Netlify에 배포
3. 고객에게는 Netlify 주소를 안내
4. 실제 데이터 호출은 Render `/api/ping`과 본 서비스 URL로 연결

## 목적
- 첫 화면 체감 속도 개선
- Render waking 화면을 고객이 덜 보게 함
- 광고/블로그 유입 시 이탈 감소
