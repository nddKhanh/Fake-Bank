# Wallet UI — frontend độc lập

HTML/CSS/JavaScript trong public/. Không còn nằm trong wallet-service.
Server Node dùng thư viện chuẩn, không cần npm install để chạy và không cần Docker.

Từ thư mục gốc project:

```powershell
cd wallet-ui
npm.cmd start
```

Mở http://localhost:3000. Bấm **Xem dữ liệu minh họa** để xem đủ màn mà không
cần bật bất kỳ backend hay database nào. Chỉ làm mới bằng nút bấm.

Chế độ API thật gọi /api/ops và /api/lab qua cầu nối tới Logging Service (mặc định
localhost:8082). Chưa bật Ops thì hiện lỗi 502, không tự chuyển sang minh họa.
Override bằng OPS_URL; đổi cổng frontend bằng UI_PORT. Không có logic xử lý tiền,
query SQL hay chạy test nghiệp vụ trong frontend.

Logging Service hiện đọc dữ liệu thật cho tổng quan, tài khoản và giao dịch. Vì vậy:

- muốn xem đầy đủ các màn hình: chỉ chạy `wallet-ui` và chọn **Xem dữ liệu minh họa**;
- muốn dùng chế độ API thật: chạy thêm `wallet-service` và `logging-service`;
- `wallet-ui` chưa gọi trực tiếp CRUD `/accounts` và `/transfers` của Wallet API.

Trang **Bản đồ database** luôn gọi API thật, kể cả khi các trang khác đang ở chế
độ minh họa. Nó hiển thị metadata hiện có trong PostgreSQL (bảng/view, cột, kiểu,
default, nullable và constraint/FK), không hiển thị các bảng mới chỉ nằm trong kế
hoạch.

Trang **Scenario Lab** có hướng dẫn quy trình reset → chọn case → cấu hình lỗi →
chạy → đọc bằng chứng. Khi Lab được triển khai, ScenarioRunner phải tự tạo giao
dịch theo seed để kết quả tái lập; người dùng không cần tạo thủ công. Với Wallet
V0 hiện tại, chỉ có thể tạo giao dịch happy-path qua `POST /transfers`; thay đổi
amount/tài khoản không tạo được timeout, retry hay crash.

Bốn thẻ **CRUD Lab V0** chạy được ngay trên database local: kiểm tra amount bằng
0, chuyển quá số dư, gửi trùng payload và 20 request đồng thời. Mỗi thẻ tự reset
seed và để lại trạng thái sau thí nghiệm để có thể mở trang Tài khoản/Giao dịch
đối chiếu. Các thẻ nâng cao A–E vẫn khóa cho đến khi backend có cơ chế tương ứng.

Ở chế độ API thật, nút **Khôi phục seed** xóa toàn bộ transfer/tài khoản local và
tạo lại ba tài khoản A/B/C với số dư 100.000/50.000/0 VND. Nút bị khóa ở chế độ
minh họa hoặc khi backend không báo `resetEnabled=true`; hộp thoại luôn yêu cầu
xác nhận trước khi gửi request.

Ledger, queue, reconciliation và Scenario Lab chờ schema backend tương ứng; xem
`../docs/ops-backend-gaps.md`.

Kiểm tra server: `npm.cmd test`.
Kiểm tra 10 luồng DOM (Node 24.15+ hoặc 26+):

```powershell
npm.cmd install --prefix ui-tests
npm.cmd test --prefix ui-tests
```
