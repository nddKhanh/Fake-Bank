# Implementation hiện tại — chương 2, bậc 1.1

AccountServiceImpl đã thực hiện CRUD tài khoản; TransferServiceImpl chuyển tiền bằng
ba lần ghi độc lập: trừ số dư người gửi, cộng số dư người nhận, lưu transfer.
Controller đã nối service và trả ApiResponse/ListResponse. Chạy profile db để bật
PostgreSQL local, Flyway và JPA; profile mặc định ui giữ API ở 501 khi chưa có DB.

```powershell
.\mvnw.cmd spring-boot:run "-Dspring-boot.run.profiles=db"
```

POST /accounts: {"ownerRef":"user-A","currency":"VND","balance":"100000"}
POST /transfers: {"fromAccountId":"UUID-A","toAccountId":"UUID-B","amount":"30000"}
Dùng UUID trả về khi tạo tài khoản. V0 không tự seed A/B/C.

Bản 1.1 cố ý không có transaction bao quanh ba bước, khóa hay kiểm tra thiếu số dư.
Lỗi sau bước trừ tiền có thể để lại số dư dở dang; gửi đồng thời có thể mất cập nhật.
Đây là các lỗi để bạn chứng minh trước khi nâng cấp bậc 1.2–1.4.
Validation cơ bản chỉ kiểm tra định dạng tiền, amount dương, hai tài khoản khác nhau,
cùng loại tiền và giới hạn BIGINT. Không có ledger, idempotency hay Kafka.
# Wallet service — backend CRUD

Chỉ giữ domain, repository, service và controller cho bậc 1.1 chương 2.
Service gồm AccountService và TransferService; implementation CRUD bậc 1.1 đã có.
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
Profile db đã bật JPA/repository và nghiệp vụ chuyển tiền CRUD bậc 1.1.

Hướng dẫn khởi đầu: docs/crud-start.md. Test: .\mvnw.cmd test.
