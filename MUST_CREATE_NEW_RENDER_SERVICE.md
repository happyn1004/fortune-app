중요: 기존 무료 Render 서비스에 ZIP만 다시 올리면 local SQLite 초기화 문제를 코드만으로 해결할 수 없습니다.

필수 조건
1. Web Service plan: Starter 이상
2. Persistent Disk 추가
3. mountPath: /data
4. Environment Variable: DATA_DIR=/data/mysticday
5. REQUIRE_PERSISTENT_STORAGE=true 유지

꼭 확인할 것
- Storage Debug 의 data_dir 가 /data/mysticday 여야 함
- /var/data/mysticday 가 보이면 기존 서비스 설정이 남아 있는 것일 가능성이 큼
- 가장 안전한 방법은 새 Render 서비스를 render.yaml 기준으로 다시 생성하는 것
