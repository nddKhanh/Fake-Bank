# Bắt đầu chương 2 — bậc 1.1

Khung bắt đầu chỉ có Account và Transfer tối thiểu, chưa có logic thực thi.
Không thêm transaction, khóa, sổ cái, trạng thái, hạn mức hay idempotency ở bậc này.
Các interface nâng cao đã dựng trước đó dành cho các bậc sau.

## Các file bạn bắt đầu viết

1. `src/main/resources/db/migration/V0__naive.sql`: schema V0 đã có; bạn tự thêm migration version mới khi tiến tới các bậc sau.
2. `domain/Account.java`, `domain/Transfer.java`: cấu trúc dữ liệu ban đầu.
3. `infrastructure/AccountRepository.java`, `TransferRepository.java`: tự thêm lớp
   implementation để lưu/đọc database. Chưa có bean hay truy vấn được viết sẵn.
4. `application/AccountService.java`, `CrudTransferService.java`: tự viết lớp
   implementation của use case CRUD/chuyển tiền ban đầu.
5. `api/AccountController.java`, `TransferController.java`: thay các lệnh ném
   NotImplemented bằng lời gọi service của bạn.

Giữ riêng `/api/ops/*`: đây là API quan sát, không phải API nghiệp vụ CRUD.

## Hợp đồng endpoint ban đầu

| Endpoint | Khung |
|---|---|
| POST /accounts | Tạo tài khoản |
| GET /accounts | Danh sách tài khoản |
| GET /accounts/{id} | Chi tiết tài khoản |
| PUT /accounts/{id} | Cập nhật tài khoản ở bản CRUD học tập |
| DELETE /accounts/{id} | Xóa tài khoản ở bản CRUD học tập |
| POST /transfers | Tạo yêu cầu chuyển nội bộ |
| GET /transfers | Danh sách giao dịch |
| GET /transfers/{id} | Chi tiết giao dịch |

Chương 2, bậc 1.1 tạo giao dịch bằng POST và lưu dấu vết. Khung không cung cấp sửa/
xóa lịch sử giao dịch; ở các bậc sau, việc hoàn tiền đi qua giao dịch/bút toán mới.

Ví dụ request để bạn biết DTO cần nhận gì; hiện mọi endpoint vẫn trả 501:

```json
{ "ownerRef": "user-A", "currency": "VND", "balance": "100000" }
```

```json
{
  "fromAccountId": "00000000-0000-0000-0000-00000000000a",
  "toAccountId": "00000000-0000-0000-0000-00000000000b",
  "amount": "30000"
}
```

Tiền ở DTO là chuỗi số nguyên; domain dùng long tương ứng BIGINT. Bạn tự viết
chuyển đổi và validation. POST /transfers ở bậc 1.1 **không yêu cầu Idempotency-Key**;
đến giai đoạn 2 mới nối interface TransferService nâng cao.

## Chạy giao diện

```powershell
cd F:\HIT\Fake-Bank\wallet-service
.\mvnw.cmd spring-boot:run
```

Mở http://localhost:8080 và bấm **Xem dữ liệu minh họa**. Không cần Docker để xem UI.
Giao diện hiện là Ops Console để quan sát/test, chưa có form nhập CRUD; bạn có thể
gọi API CRUD bằng Postman hoặc curl sau khi tự triển khai.

## Khi tự nối PostgreSQL

Từ thư mục repo, tạo `.env` theo `.env.example`, rồi:

```powershell
docker compose --profile wallet up -d wallet-postgres
```

V0__naive.sql đã có. Chạy với profile db: .\mvnw.cmd spring-boot:run "-Dspring-boot.run.profiles=db". Database mặc định localhost:5433/wallet. Profile db bật JDBC/Flyway; JPA và nghiệp vụ vẫn chưa triển khai. Spring Boot không tự nạp .env; export biến môi trường nếu thông số khác default. Giữ datasource Ops chỉ đọc riêng khi nối UI với dữ liệu thật.

Đọc tiếp bậc 1.2–1.4 sau khi bạn đã làm chạy được bản 1.1 và ghi lại lỗi của CRUD
ở `docs/why-crud-fails.md`. Không nối ngay các interface ledger/recovery/runner vào
luồng khởi đầu.
