# Implementation hiện tại — chương 2, bậc 1.1

AccountServiceImpl đã thực hiện CRUD tài khoản; TransferServiceImpl chuyển tiền bằng
ba lần ghi độc lập: trừ số dư người gửi, cộng số dư người nhận, lưu transfer.
Controller đã nối service và trả ApiResponse/ListResponse. Cấu hình mặc định bật
PostgreSQL local, Flyway, JPA và seed; Spring tự đọc `.env` ở thư mục gốc.

```powershell
.\mvnw.cmd spring-boot:run
```

POST /accounts: {"ownerRef":"user-A","currency":"VND","balance":"100000"}
POST /transfers: {"fromAccountId":"UUID-A","toAccountId":"UUID-B","amount":"30000"}
Dùng UUID trả về khi tạo tài khoản. Khi chạy local mặc định, Flyway seed sẵn A/B/C.

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

FE nằm trong `../wallet-ui`. Ops, Scenario Lab và khung logging nằm trong
`../logging-service`.
Wallet backend không còn phục vụ index.html hay /api/ops, /api/lab.

```powershell
cd wallet-service
.\mvnw.cmd spring-boot:run
```

Backend chạy cổng 8080 và cần PostgreSQL local. Database `wallet` phải tồn tại;
thông tin kết nối được đọc tự động từ `.env`:

```powershell
.\mvnw.cmd spring-boot:run
```

Tạo database wallet trống trước. Flyway tạo accounts/transfers và seed A/B/C.
Đổi cấu hình bằng WALLET_DB_HOST/PORT/NAME/USERNAME/PASSWORD.
Profile mặc định đã bật JPA/repository và nghiệp vụ chuyển tiền CRUD bậc 1.1.

Dữ liệu mẫu local được quản lý riêng trong `src/main/resources/db/seed` và chạy
mặc định cùng migration.

Quy tắc seed số dư và template cho migration mới nằm trong `db/seed/README.md`.

`POST /api/dev/reset` với body `{"confirmed":true}` xóa toàn bộ transfer/tài khoản
và tạo lại seed A/B/C. Endpoint chỉ có trong profile `db-seed` và chỉ nhận request
từ localhost; giao diện gọi gián tiếp qua Logging Service.

Hướng dẫn khởi đầu: docs/crud-start.md. Test: .\mvnw.cmd test.
