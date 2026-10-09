# Fake Bank / Wallet learning workspace

| Thư mục | Vai trò | Cổng |
|---|---|---|
| wallet-service | Backend CRUD tài khoản/chuyển tiền (TODO) | 8080 |
| fakebank | Ngân hàng bên ngoài giả lập, backend khung | 8081 |
| ops-service | API quan sát, logging extension, Scenario Lab (TODO) | 8082 |
| wallet-ui | Frontend HTML/CSS/JS và cầu nối API | 3000 |

Để xem giao diện, chỉ cần Node.js:

```powershell
cd wallet-ui
npm.cmd start
```

Mở http://localhost:3000, chọn Xem dữ liệu minh họa. Không cần Docker hoặc Java.
Dữ liệu minh họa không phải database thật, không chạy các kịch bản tài chính.

Backend: chạy .\mvnw.cmd spring-boot:run từ thư mục service tương ứng.
Wallet profile db hỗ trợ PostgreSQL local/Flyway; profile mặc định không cần DB.
Ops chạy độc lập, không cần bật wallet để xem các API khung; Lab chỉ ở profile lab.

Tất cả nghiệp vụ vẫn để trống theo yêu cầu. Mỗi phần có README riêng để chạy/test.
Kế hoạch học: money-transfer-plan.md. Docker Compose chỉ là tùy chọn, không bắt buộc.
