# Vì sao CRUD thất bại — ghi chép của bạn (bậc 1.1)

## Cơ chế thí nghiệm trên giao diện

Các thẻ `V0-*` trong **Scenario Lab** không dùng fixture để quyết định kết quả.
Mỗi lần chạy đi qua chuỗi thật sau:

1. `POST /api/dev/reset` trên Wallet để nạp lại seed A/B/C.
2. Gọi `POST /transfers` bằng chính `TransferController` và
   `TransferServiceImpl` đang học.
3. Đọc lại bảng `accounts` và `transfers` từ PostgreSQL qua Logging Service.
4. So snapshot trước/sau và trả nhật ký HTTP, run ID, thời điểm chạy cùng nhãn
   `LIVE_BACKEND` cho giao diện.

Riêng `V0-PARTIAL-WRITE` gọi thêm
`POST /api/dev/faults/next-transfer` để arm failpoint one-shot `AFTER_DEBIT`.
Failpoint chỉ có ở profile local `db-seed`, tự tắt ngay khi được kích hoạt và
không bọc transaction hay sửa logic chuyển tiền.

| Thẻ | Request thật | Điều được đo |
|---|---|---|
| `V0-VALIDATION` | amount `0` | HTTP 400 và DB không đổi |
| `V0-PARTIAL-WRITE` | amount `30000`, lỗi sau debit | A/B và số transfer sau HTTP 500 |
| `V0-OVERDRAFT` | amount `120000` | A có âm hay không |
| `V0-DUPLICATE` | cùng payload hai lần | số transfer và số lần trừ tiền |
| `V0-CONCURRENCY` | 20 request đồng thời, mỗi request `10000` | số request thành công, số dư và tổng tiền |

Test contract `CrudExperimentServiceTests` sẽ thất bại nếu runner không còn gọi
đúng các HTTP endpoint Wallet. Những phần TODO dưới đây vẫn để bạn tự ghi nhận và
giải thích sau khi chạy thí nghiệm.

## Thử nghiệm 1: lỗi giữa hai lần cập nhật số dư

TODO: cách gây lỗi, dữ liệu trước/sau, điều quan sát được, nguyên nhân bạn nhận ra.

## Thử nghiệm 2: hai request rút tiền đồng thời

TODO: cách chạy, số dư kỳ vọng/thực tế, dấu vết và nguyên nhân.

## Thử nghiệm 3: giải thích nguồn gốc số dư

TODO: dữ liệu nào đang thiếu để trả lời số dư đến từ đâu.
