# Fake Bank / Wallet learning workspace

## Cần chạy bao nhiêu tiến trình?

| Mục tiêu | Số tiến trình | Thành phần cần chạy |
|---|---:|---|
| Xem đầy đủ giao diện bằng dữ liệu minh họa | 1 | `wallet-ui` |
| Gọi API tài khoản/chuyển tiền thật | 1 | `wallet-service` và PostgreSQL local đã bật |
| Chạy toàn bộ stack hiện có | 3 | `wallet-service`, `logging-service`, `wallet-ui` |

> Chế độ API thật hiện đọc được tổng quan, tài khoản và giao dịch từ PostgreSQL.
> Các màn phụ thuộc ledger/outbox/reconciliation/Scenario Lab hiển thị trạng thái
> trống cho đến khi Wallet backend có schema tương ứng.

## Cách 1 — chỉ xem đầy đủ giao diện

Chạy một tiến trình:

```powershell
cd wallet-ui
npm.cmd start
```

Mở http://localhost:3000 và bấm **Xem dữ liệu minh họa**. Không cần Java,
PostgreSQL, Docker, Wallet backend hay Logging Service.

## Cách 2 — chạy API chuyển tiền thật

PostgreSQL local phải đang chạy và database `wallet` phải tồn tại. Cấu hình kết
nối nằm trong `.env`; Spring tự đọc file này.

```powershell
cd wallet-service
.\mvnw.cmd spring-boot:run
```

Backend chạy tại http://localhost:8080. Flyway tự chạy migration và seed A/B/C.
Frontend đọc dữ liệu này thông qua Logging Service; nếu chỉ chạy Wallet API thì
có thể thử trực tiếp bằng Postman hoặc file HTTP.

## Cách 3 — chạy toàn bộ stack hiện có

Cần ba terminal và ba tiến trình.

Terminal 1 — Wallet API, cổng 8080:

```powershell
cd wallet-service
.\mvnw.cmd spring-boot:run
```

Terminal 2 — Logging Service/Ops, cổng 8082:

```powershell
cd logging-service
.\mvnw.cmd spring-boot:run
```

Terminal 3 — Frontend, cổng 3000:

```powershell
cd wallet-ui
npm.cmd start
```

Fake Bank ở cổng 8081 chưa cần cho luồng chuyển tiền nội bộ hiện tại. Docker
Compose là tùy chọn, không bắt buộc.

### Khôi phục dữ liệu local về seed ban đầu

Khi chạy đủ ba tiến trình, bấm **Khôi phục seed** ở góc trên giao diện. Sau bước
xác nhận, hệ thống xóa toàn bộ giao dịch và tài khoản hiện tại rồi tạo lại:

- `user-A`: 100.000 VND
- `user-B`: 50.000 VND
- `user-C`: 0 VND

Reset không thể hoàn tác, chỉ nhận request từ localhost và chỉ tồn tại khi
`wallet-service` bật profile `db-seed` (profile mặc định khi chạy local).

### Thí nghiệm lỗi CRUD V0

Mở **Scenario Lab → Thí nghiệm thật trên CRUD V0**. Năm nút ở đầu trang tự reset
seed, gọi Wallet thật và hiển thị HTTP status, số dư cùng số transfer trước/sau:

- amount bằng 0 — validation phải từ chối và không đổi dữ liệu;
- lỗi one-shot sau debit — A bị trừ nhưng B chưa nhận và chưa có transfer;
- chuyển 120.000 từ tài khoản chỉ có 100.000 — tái hiện số dư âm;
- gửi cùng payload hai lần — tái hiện thiếu idempotency;
- 20 request đồng thời — tái hiện lỗi lost update/tổng tiền do không khóa.

Các thẻ A–E bên dưới vẫn khóa vì kiểm thử retry, callback, crash và outbox cần
backend tương ứng. Sau thí nghiệm, bấm **Khôi phục seed** để làm sạch dữ liệu.

## Các thành phần

| Thư mục | Vai trò | Cổng |
|---|---|---:|
| `wallet-service` | API tài khoản và chuyển tiền nội bộ | 8080 |
| `fakebank` | Ngân hàng bên ngoài giả lập | 8081 |
| `logging-service` | Ops read model và điểm mở rộng Scenario Lab | 8082 |
| `wallet-ui` | Frontend và cầu nối tới Ops | 3000 |

Mỗi thành phần có README riêng. Các giới hạn Ops hiện tại được ghi tại
`docs/ops-backend-gaps.md`.
