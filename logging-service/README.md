# Logging Service — Ops read model

Logging Service phục vụ giao diện quan sát ở cổng 8082 và đọc trực tiếp database
Wallet theo chế độ read-only.

## Chạy local

PostgreSQL Wallet và `wallet-service` nên được bật trước. Từ thư mục gốc project:

```powershell
cd logging-service
.\mvnw.cmd spring-boot:run
```

Service tự đọc `.env` ở thư mục gốc. Nếu chưa cấu hình `OPS_DB_USERNAME` và
`OPS_DB_PASSWORD`, local development dùng tài khoản Wallet nhưng connection vẫn
được đặt read-only.

## API đã hoạt động

- `GET /api/ops/capabilities`
- `GET /api/ops/overview`
- `GET /api/ops/transfers`
- `GET /api/ops/transfers/{id}`
- `GET /api/ops/accounts`
- `GET /api/ops/accounts/{id}`
- `GET /api/ops/queues`
- `GET /api/ops/reconciliation`
- `GET /api/ops/runs`
- `GET /api/ops/schema` — metadata thật từ `information_schema`
- `POST /api/ops/reset` với body `{"confirmed":true}`
- `POST /api/ops/experiments/{id}` — chạy thí nghiệm CRUD V0 trên Wallet thật

Queue, reconciliation và runs trả collection rỗng cho đến khi Wallet có schema
tương ứng. Chi tiết giới hạn: `../docs/ops-backend-gaps.md`.

Endpoint reset chỉ nhận request từ localhost và chuyển tiếp đến
`wallet-service /api/dev/reset`; nó không ghi qua JDBC read-only của Logging
Service. Wallet phải chạy profile `db-seed`.

Các experiment local hiện có: `V0-VALIDATION`, `V0-PARTIAL-WRITE`, `V0-OVERDRAFT`,
`V0-DUPLICATE`, `V0-CONCURRENCY`. Mỗi lần chạy tự reset seed, gọi API
`/transfers`, rồi đọc lại số dư và số transfer để trả bằng chứng trước/sau. Đây là
công cụ chứng minh lỗi của V0, không sửa logic chuyển tiền. Mỗi kết quả có
`runId`, `executedAt`, `executionMode=LIVE_BACKEND` và danh sách HTTP action thật.
Test `CrudExperimentServiceTests` xác nhận runner gọi đúng endpoint Wallet thay vì
tự dựng kết quả.

## Scenario Lab

Các API ghi `/api/lab/*` chỉ tồn tại khi bật profile `lab` và hiện vẫn trả `501`.
Phần này cần Wallet/Fake Bank hỗ trợ fault injection nên chưa được giả lập trong
Logging Service.

## Test

```powershell
.\mvnw.cmd clean test
```
