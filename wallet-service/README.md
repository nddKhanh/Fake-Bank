# Wallet service — backend CRUD

Chỉ giữ domain, repository, service và controller cho bậc 1.1 chương 2.
Service còn AccountService và CrudTransferService; mọi logic vẫn TODO/501.
Các khung nâng cao ledger, outbox, callback, recovery, reconciliation và
TransferService đã được xóa theo yêu cầu; bổ sung lại khi học đến đúng giai đoạn.

FE nằm trong ../wallet-ui. Ops, Scenario Lab và khung logging nằm trong ../ops-service.
Wallet backend không còn phục vụ index.html hay /api/ops, /api/lab.

```powershell
cd F:\HIT\Fake-Bank\wallet-service
.\mvnw.cmd spring-boot:run
```

Backend chạy cổng 8080, khung mặc định không cần DB. Với PostgreSQL local và Flyway:

```powershell
$env:WALLET_DB_PORT = "5432"
$env:WALLET_DB_USERNAME = "postgres"
$env:WALLET_DB_PASSWORD = "mat-khau-cua-ban"
.\mvnw.cmd spring-boot:run "-Dspring-boot.run.profiles=db"
```

Tạo database wallet trống trước. V0__naive.sql tạo accounts/transfers, không seed.
Đổi cấu hình bằng WALLET_DB_HOST/PORT/NAME/USERNAME/PASSWORD.
Chưa có JPA/repository implementation hay nghiệp vụ chuyển tiền.

Hướng dẫn khởi đầu: docs/crud-start.md. Test: .\mvnw.cmd test.
