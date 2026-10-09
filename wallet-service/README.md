# Wallet service & Ops Console — khung chương 7–8

**Bắt đầu học CRUD theo chương 2:** xem [docs/crud-start.md](docs/crud-start.md).
AccountService và CrudTransferService là điểm bắt đầu bậc 1.1; TransferService
nâng cao và các interface ledger/recovery/runner dành cho các bậc sau.

Giao diện đã triển khai. **Backend nghiệp vụ và truy vấn database chủ ý để trống**
theo yêu cầu: interface, DTO và controller có TODO; endpoint chưa triển khai trả
`501 NOT_IMPLEMENTED`. Không có giao dịch thật, runner thật, seed DB, migration ví,
retry, ghi sổ, callback hay SQL thực thi.

## Chạy giao diện ngay (không cần Docker/database)

```powershell
cd F:\HIT\Fake-Bank\wallet-service
.\mvnw.cmd spring-boot:run
```

Mở <http://localhost:8080>. Mặc định giao diện gọi API thật và hiển thị trạng thái
chưa triển khai. Bấm **Xem dữ liệu minh họa** để duyệt đủ màn bằng fixtures tĩnh.
Không có tự động chuyển sang fixtures khi API lỗi. Trong chế độ minh họa, mọi nút
chạy test, reset, bật/tắt chaos đều khóa; lịch sử mẫu được ghi rõ là minh họa.

## Các màn đã dựng

| Màn | Chức năng giao diện |
|---|---|
| S1 Tổng quan | 8 đèn, số vi phạm, metric có drilldown, trạng thái 1 giờ, p50/p95 |
| S2 Giao dịch | Lọc trạng thái/loại/thời gian; tìm ID/key/mã đơn; phân trang |
| S3 Chi tiết | Dòng thời gian 5 nguồn, bút toán DEBIT/CREDIT, khóa retry |
| S4 Tài khoản | Cache cạnh ledger, lọc tài khoản lệch, sổ phụ, biểu đồ bậc thang |
| S5 Bank & hàng đợi | Giao dịch dở, outbox, callback sai chữ ký, công tắc chaos |
| S6 Đối soát | Expected/actual, trạng thái, ghi chú và liên kết điều chỉnh |
| S7 Scenario Lab | 36 thẻ A–E, seed, 8 bước, kết quả từng ý, trước/sau, lịch sử |
| Bản đồ DB | Cấu trúc dự kiến từ tài liệu, liên kết sang màn liên quan |

Các màn có trạng thái loading/empty/error, làm mới thủ công bằng nút “Làm mới”,
và bố cục responsive. Tiền trong API là **chuỗi số nguyên** để không mất độ chính
xác BIGINT khi JavaScript xử lý; không dùng số thập phân cho VND.

## Điểm bạn tự điền backend

| File / interface | TODO |
|---|---|
| `application/TransferService.java` | Transaction, row locks có thứ tự, idempotency, hạn mức, state machine |
| `application/LedgerService.java` | Điểm ghi sổ duy nhất, bút toán kép, account_seq, hoàn tiền/điều chỉnh |
| `application/OutboxRelay.java` | Kafka, stable event ID, relay, consumer idempotent, DLT |
| `application/RecoveryWorker.java` | Claim, retry có giới hạn, inquiry UNKNOWN |
| `application/CallbackService.java` | Chữ ký, inbox, chống trùng, xử lý mâu thuẫn |
| `application/ReconciliationService.java` | Đối soát nội bộ và bank report |
| `ops/OpsQueryService.java` | SELECT-only datasource và query các view mục 8.3–8.4 |
| `ops/OpsContracts.java` | DTO frontend đang tiêu thụ |
| `lab/ScenarioRunner.java` | Gây lỗi, chờ hội tụ, snapshot, so điều kiện từng thẻ |
| `api/OpsController.java` | Thay stub bằng lời gọi OpsQueryService |
| `api/LabController.java` | Thay stub bằng ScenarioRunner; giới hạn local, CSRF, khóa chạy |
| `../fakebank/.../BankTransferUseCase.java` | Nghiệp vụ bank, idempotency và chaos |

Chỉ thêm `@Service` khi có implementation. Interface hiện không có bean để tránh
gây hiểu nhầm là backend đã hoạt động. Mục 7.5 trong `money-transfer-plan.md` là
nguồn của `static/scenarios.json`; danh mục thẻ không phải implementation test.

## API contract

Tất cả endpoint Ops là `GET /api/ops`:

| Path | Response |
|---|---|
| `/capabilities` | `{backendImplemented, labEnabled, dataSource}` |
| `/overview` | `Overview` |
| `/transfers?status=&type=&search=&from=&to=&page=0&size=50` | `Page<Transfer>` |
| `/transfers/{uuid}` | `TransferDetail` |
| `/accounts?mismatchedOnly=false&search=&page=0&size=50` | `Page<Account>` |
| `/accounts/{uuid}` | `AccountDetail` |
| `/queues` | `Queues` |
| `/reconciliation?status=` | `Reconciliation` |
| `/runs?page=0&size=50` | `Page<ScenarioRun>` |
| `/runs/{uuid}` | `ScenarioRun` |

`from/to` dùng ISO-8601; timezone hiển thị là Asia/Bangkok. Clamp page/size và validate
filter tại backend khi triển khai. `Check.drilldown` / `Metric.drilldown` là hash
nội bộ, ví dụ `#accounts?mismatchedOnly=true` hoặc `#transfers?status=UNKNOWN`.
Danh sách ledger của một giao dịch phải bao gồm mọi bước ghi sổ, không chỉ PRINCIPAL.

Lab chỉ có route khi profile `lab` được bật:

| Method / path | Request | Response dự kiến |
|---|---|---|
| `POST /api/lab/scenarios/{id}/runs` | `{ "seed": 42 }` | UUID JSON của lần chạy nền |
| `POST /api/lab/reset` | `{ "confirmed": true }` | 204 khi hoàn tất |
| `PUT /api/lab/chaos/{name}` | `{ "enabled": true }` | 204 khi hoàn tất |
| `POST /api/lab/runs/{uuid}/cancel` | Không body | 204 khi hủy |

Frontend đọc lại `/api/ops/runs/{uuid}` để theo dõi tiến độ. `ScenarioRun.status`:
`RUNNING`, `PASS`, `FAIL`, `CANCELLED`, `UNSUPPORTED`; `phase` mô tả bước hiện tại.
Hiện `/capabilities` luôn trả hai cờ `false`; sau khi triển khai, trả capability
theo bean/profile thật để frontend mở đúng chức năng. Không bật cờ chỉ để mở nút
khi chưa có runner. Lab `RUNNING` phải khóa các lệnh reset/chaos/chạy mới ở server;
không dựa riêng vào trạng thái nút trên trình duyệt.

## Kết nối database sau này

`application.yml` hiện exclude datasource/JPA/Flyway auto-configuration để UI chạy
độc lập. Khi bắt đầu triển khai backend:

1. Thêm migration ví theo từng giai đoạn kế hoạch; tạo view mục 8.3.
2. Bỏ ba exclusions, bật Flyway và cấu hình writer datasource.
3. Tạo datasource riêng dùng `ops_reader`, chỉ SELECT các bảng/view cần dùng.
4. Implement OpsQueryService; không gửi mật khẩu/hash khóa API ra frontend.
5. Kết nối Fake Bank qua HTTP, giữ hai database tách biệt.

Postgres ví tùy chọn đã có trong Compose, cổng 5433, volume riêng:

```powershell
# Từ thư mục repo, sau khi tạo .env với thông số local:
docker compose --profile wallet up -d wallet-postgres
```

Spring Boot chạy local không tự import `.env`; đặt biến môi trường trong shell khi
muốn override. Khung UI hiện không kết nối hay thay đổi DB này.

## Quy tắc runner từ chương 7

- Reset fixture có xác nhận; giữ riêng lịch sử chạy.
- Chụp trước → gây lỗi → tác động → **gỡ lỗi trong finally** → chờ hội tụ → chụp sau
  và 8 đèn → so toàn bộ điều kiện của thẻ.
- Hội tụ: 3 lần liên tiếp cách 1 giây có backlog transfer/outbox/callback bằng 0;
  loại `PENDING_REVIEW`; hết timeout thì FAIL.
- PASS khi và chỉ khi đã hội tụ, đủ 8 check bằng 0 và mọi assertion đúng.
- C1–C8 cần process supervisor/runner ngoài process bị kill; không `halt` web UI.
- E1 lưu seed và chuỗi thao tác thực tế để chạy lại đúng.
- Các kịch bản chưa đủ dependency phải UNSUPPORTED, không PASS.

Profile lab đã có bind localhost:

```powershell
.\mvnw.cmd spring-boot:run "-Dspring-boot.run.profiles=lab"
```

Ngay cả profile lab hiện cũng chỉ trả 501; chưa có ghi/truncate/chaos/crash.

## Kiểm tra

```powershell
.\mvnw.cmd test
```

Các test xác nhận UI không cần database, Ops trả 501, không có route Lab ngoài
profile, và profile Lab không thực thi nghiệp vụ. Đây là kiểm tra **khung**, không
phải các test tài chính A–E.

Kiểm tra các luồng frontend bằng DOM (Node.js 24.15+ hoặc 26+, không cần mở browser):

```powershell
npm.cmd install --prefix ui-tests
npm.cmd test --prefix ui-tests
```

Bao gồm chuyển API/minh họa, 8 đèn, bộ lọc/drilldown, dòng thời gian, sổ phụ,
36 thẻ kịch bản, khóa các thao tác ghi và escaping nội dung tìm kiếm. Kiểm tra DOM
không thay thế việc kiểm tra bố cục trực tiếp trên trình duyệt.
