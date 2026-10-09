# Fake Bank

Khung Spring Boot cho hệ thống ngân hàng ngoài giả lập trong
[`money-transfer-plan.md`](money-transfer-plan.md). Project hiện dừng ở lớp hạ tầng và
domain; có thêm khung controller/interface, chưa có repository hay nghiệp vụ CRUD.

## Ops Console theo chương 7–8

Repo có thêm **`wallet-service/`**: giao diện quan sát database và Scenario Lab,
gồm đủ S1–S7 và bản đồ cấu trúc dữ liệu. **Logic backend để trống**: interface/DTO,
controller trả `501 NOT_IMPLEMENTED`; bạn tự điền nghiệp vụ và truy vấn sau.
Fake Bank có thêm khung API transfer/inquiry, cũng chưa thực thi nghiệp vụ.

Chạy giao diện ngay, không cần Docker hay PostgreSQL:

```powershell
cd wallet-service
.\mvnw.cmd spring-boot:run
```

Mở `http://localhost:8080`, bấm **Xem dữ liệu minh họa** để duyệt giao diện.
Fixtures minh họa được ghi nhãn rõ; nút chạy test/reset/chaos khóa khi chưa có backend.

Xem [wallet-service/README.md](wallet-service/README.md) để biết API contract, các
điểm TODO và cách nối database thật. Danh mục 36 kịch bản lấy từ mục 7.5; đây là
thẻ đặc tả để bạn triển khai runner, chưa phải test tài chính đã chạy thành công.

## Stack

- Java 17
- Spring Boot 3.5
- PostgreSQL 16
- Flyway
- JPA/Hibernate
- Testcontainers

## Cấu trúc chính

```text
.
├── docker-compose.yml
├── .env.example
└── fakebank
    └── src
        ├── main
        │   ├── java/com/example/fakebank
        │   │   ├── api           # để bạn tự viết controller/DTO
        │   │   ├── application   # để bạn tự viết use case/service
        │   │   └── domain        # entity và enum theo mục 4.1
        │   └── resources
        │       ├── application.yml
        │       └── db/migration
        └── test
```

## Chạy local

Yêu cầu: JDK 17 trở lên và Docker.

```powershell
Copy-Item .env.example .env
# Đổi mật khẩu trong .env nếu cần.
docker compose up -d
cd fakebank
.\mvnw.cmd spring-boot:run
```

Ứng dụng mặc định chạy ở `http://localhost:8081`; health check ở
`http://localhost:8081/actuator/health`.

## Chạy test

```powershell
cd fakebank
.\mvnw.cmd test
```

Test khởi động PostgreSQL thật bằng Testcontainers và tự bỏ qua nếu Docker không
sẵn sàng.

## Phần dành cho bạn tự triển khai

Theo thứ tự học gợi ý:

1. Repository cho `BankAccount` và CRUD tài khoản.
2. API `POST /bank/transfers` có idempotency theo `clientRequestId`.
3. Inquiry và báo cáo ngày.
4. Callback outbox và retry.
5. Các công tắc lỗi từ `ChaosRule`.

Kafka chưa được thêm vào vì bản kế hoạch chỉ đưa Kafka vào ở giai đoạn outbox.
