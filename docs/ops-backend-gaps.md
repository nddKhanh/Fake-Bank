# Các phần Ops chờ Wallet backend

Tài liệu này ghi lại những màn hình/trường dữ liệu Logging Service chưa thể cung
cấp chính xác vì schema Wallet hiện tại mới ở V0/V1. Logging Service không tạo dữ
liệu giả để lấp các trường nghiệp vụ chưa tồn tại.

## Đã hoạt động với dữ liệu thật

- Tổng quan: số tài khoản, số giao dịch và các kiểm tra có thể suy ra từ V0.
- Danh sách/lọc/phân trang giao dịch nội bộ đã lưu.
- Chi tiết giao dịch và hai chân debit/credit dạng projection.
- Danh sách/tìm kiếm/phân trang tài khoản và số dư hiện tại.
- Chi tiết tài khoản.
- Kết nối PostgreSQL ở chế độ read-only.
- Bản đồ database lấy trực tiếp bảng, cột và constraint/FK từ
  `information_schema`; không trộn schema dự kiến vào schema đang chạy.
- Reset local về đúng seed V0 thông qua Wallet (`db-seed`, localhost và xác nhận
  bắt buộc). Reset xóa transfer/tài khoản; đây không phải Scenario Lab đầy đủ.

Trong V0, một dòng `transfers` chỉ được lưu sau khi hai số dư đã cập nhật nên Ops
hiển thị nó là `INTERNAL / COMPLETED`. Đây là diễn giải đúng cho dữ liệu đang tồn
tại, không phải state machine đầy đủ.

## Chờ ledger

Backend chưa có `ledger_transactions`, `ledger_entries`, `account_seq` và
`balance_after`, vì vậy chưa thể cung cấp:

- số dư tính lại từ sổ cái và chênh lệch cache/ledger;
- sổ phụ, biểu đồ số dư lịch sử và thứ tự bút toán;
- tám bất biến ledger đầy đủ;
- reconciliation dựa trên ledger;
- số dư tại đúng thời điểm từng giao dịch.

Khi backend thêm ledger, bổ sung query trong `JdbcOpsQueryService.account`,
`accounts`, `overview` và `reconciliation`.

## Chờ transfer state machine và idempotency

Schema chưa có `status`, `type`, `updated_at`, `idempotency_key`, `source`,
`source_id`, `failure_code` hoặc `transfer_status_history`. Vì vậy chưa thể hiển
thị giao dịch đang dở, UNKNOWN/FAILED/REFUNDED, lịch sử chuyển trạng thái, nguồn
thanh toán hoặc kiểm tra idempotency.

## Chờ outbox, bank request và callback

Schema chưa có `outbox_events`, `bank_requests`, `bank_callbacks` và chưa có luồng
chuyển tiền ra Fake Bank. Màn Queue hiện trả danh sách rỗng cùng transport
`NOT_AVAILABLE_IN_WALLET_V0`.

Khi các bảng trên được thêm, triển khai lại `JdbcOpsQueryService.queues` và phần
`attempts` trong `transfer`.

## Chờ reconciliation persistence

Schema chưa có `reconciliation_issues` hoặc bảng lưu lịch sử chạy đối soát. Màn
Reconciliation hiện trả danh sách rỗng. Không được hiểu danh sách rỗng là bằng
chứng hệ thống đã đối soát thành công.

## Chờ Scenario Lab

Schema và backend chưa có `scenario_runs`, snapshot, assertion, fault injection
hay global run lock. Reset seed V0 cơ bản đã có, nhưng chưa lưu lịch sử run, chưa
phục hồi trạng thái nâng cao và chưa điều phối kịch bản. Các API ghi `/api/lab/*`
tiếp tục trả `501` và capability `labEnabled=false`.

Riêng năm thí nghiệm `V0-VALIDATION`, `V0-PARTIAL-WRITE`, `V0-OVERDRAFT`, `V0-DUPLICATE` và
`V0-CONCURRENCY` đã chạy thật qua `/api/ops/experiments/*`. Chúng chỉ chứng minh
đặc tính/lỗi của CRUD hiện tại; không thay thế ScenarioRunner nâng cao và không
được báo PASS cho các invariant chưa tồn tại.

Chỉ triển khai Lab sau khi Wallet/Fake Bank có các điểm điều khiển lỗi tương ứng.
Logging Service không tự sửa số dư hoặc điều phối crash trong luồng đọc Ops.

## Quyền database cần siết trước khi deploy

Local development có thể dùng fallback `WALLET_DB_USERNAME/PASSWORD`; Hikari vẫn
đặt connection ở chế độ read-only. Trước khi deploy, tạo role `ops_reader` chỉ có
`CONNECT`, `USAGE` và `SELECT`, sau đó đặt `OPS_DB_USERNAME` và `OPS_DB_PASSWORD`.
