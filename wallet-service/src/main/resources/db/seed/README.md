# Seed data cho Wallet Service

Thư mục này chứa dữ liệu mẫu phục vụ phát triển local. Seed được tách khỏi
`db/migration` để production chỉ chạy migration schema, không tự chèn dữ liệu mẫu.

## Cấu trúc

```text
db/seed/
├── README.md
├── scripts/       # SQL được Flyway chạy khi bật profile db-seed
├── samples/       # dữ liệu tham khảo, không được Flyway tự chạy
└── templates/     # mẫu tạo seed cho migration schema mới
```

## Liên kết seed với schema migration

Tên seed phải theo mẫu:

```text
R__after_<schema-version>__<description>.sql
```

Ví dụ:

```text
db/migration/V0__naive.sql
db/seed/scripts/R__after_V0__initial_accounts.sql
```

Phần `after_V0` cho biết seed này bổ sung dữ liệu cho schema được tạo bởi
`V0__naive.sql`. Đây là quy ước tài liệu của project; file vẫn là repeatable
migration của Flyway vì bắt đầu bằng `R__`.

Mỗi script phải có comment `Target schema migration` ở đầu file và dùng
`ON CONFLICT` để có thể chạy lại an toàn. Không sửa seed cũ để phục vụ schema mới;
hãy tạo một seed mới từ template và ghi đúng migration đích.

## Quy tắc bắt buộc về số dư

> **Chỉ seed `accounts.balance` trong seed đầu tiên gắn với `V0__naive.sql`.**

Từ mọi migration sau V0:

- không thêm cột `balance` vào câu `INSERT` seed tài khoản;
- không dùng `UPDATE accounts SET balance = ...`;
- không reset hoặc ghi đè số dư của tài khoản đã tồn tại;
- khi ledger được thêm, tiền mẫu phải được nạp bằng bút toán `TOPUP` từ
  `SYSTEM_BANK` sang tài khoản người dùng, không sửa số dư trực tiếp.

Template trong `templates` cố ý không chứa `balance`. Nếu một seed tương lai cần
tiền để chạy kịch bản, seed bút toán hợp lệ theo schema ledger của bậc đó.

## Cách chạy

Seed chỉ chạy khi bật đồng thời profile `db` và `db-seed`:

```powershell
.\mvnw.cmd spring-boot:run "-Dspring-boot.run.profiles=db,db-seed"
```

Chỉ chạy schema, không seed:

```powershell
.\mvnw.cmd spring-boot:run "-Dspring-boot.run.profiles=db"
```

Không bật profile `db-seed` trong production.
