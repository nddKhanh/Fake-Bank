# Ops service — quan sát, logging và Scenario Lab

Phần hỗ trợ nằm ngoài backend CRUD:

- `ops/OpsQueryService`, `OpsContracts`: dữ liệu cho các màn quan sát.
- `lab/ScenarioRunner`: khung điều phối 36 kịch bản chương 7.
- `controller/OpsController`, `LabController`: API phục vụ giao diện/test.
- `logging/`: điểm mở rộng tra cứu và liên kết log, chưa có implementation.

Mọi nghiệp vụ/query/runner vẫn để TODO, API trả 501. Chưa có hệ thống thu gom log.
Log runtime thông thường của mỗi ứng dụng vẫn ghi tại process ứng dụng đó.
Không có dependency vào source của wallet-service và không khởi tạo writer DB.
Sau này dùng HTTP tới backend/bank và datasource SELECT-only cho các query Ops.

```powershell
cd F:\HIT\Fake-Bank\ops-service
.\mvnw.cmd spring-boot:run
```

Cổng 8082, chỉ localhost. Lab chỉ có route khi bật profile lab:

```powershell
.\mvnw.cmd spring-boot:run "-Dspring-boot.run.profiles=lab"
```

Backend đích cấu hình qua WALLET_URL (8080), BANK_URL (8081). Chưa triển khai
lời gọi HTTP, query DB, CSRF hay điều phối process crash; không bật capability
cho đến khi implementation hoàn tất. Tài liệu gốc chương 7–8 vẫn là đặc tả runner.

Chạy test: `.\mvnw.cmd test`. UI chạy riêng trong wallet-ui ở cổng 3000.
