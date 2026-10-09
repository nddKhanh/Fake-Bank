# Wallet UI — frontend độc lập

HTML/CSS/JavaScript trong public/. Không còn nằm trong wallet-service.
Server Node dùng thư viện chuẩn, không cần npm install để chạy và không cần Docker.

```powershell
cd F:\HIT\Fake-Bank\wallet-ui
npm.cmd start
```

Mở http://localhost:3000. Bấm **Xem dữ liệu minh họa** để xem đủ màn mà không
cần bật bất kỳ backend hay database nào. Chỉ làm mới bằng nút bấm.

Chế độ API thật gọi /api/ops và /api/lab qua cầu nối tới ops-service (mặc định
localhost:8082). Chưa bật Ops thì hiện lỗi 502, không tự chuyển sang minh họa.
Override bằng OPS_URL; đổi cổng frontend bằng UI_PORT. Không có logic xử lý tiền,
query SQL hay chạy test nghiệp vụ trong frontend.

Kiểm tra server: `npm.cmd test`.
Kiểm tra 10 luồng DOM (Node 24.15+ hoặc 26+):

```powershell
npm.cmd install --prefix ui-tests
npm.cmd test --prefix ui-tests
```
