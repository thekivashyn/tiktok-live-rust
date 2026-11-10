# GitHub setup — giá trị để tạo repo

Chuẩn bị cho Tri Nguyen, ngày 2026-11-10 (Asia/Saigon).

## Khi tạo repository

| Trường | Giá trị |
| --- | --- |
| Owner | `thekivashyn` |
| Repository name | `tiktok-live-rust` |
| Description | `Unofficial TikTok LIVE event engine in Rust. Direct guest connections, typed events, bounded queues, automatic reconnects, and an NDJSON CLI.` |
| Visibility | **Public** |
| Template | Không chọn |
| Add README | **Off** — đã có README trong source |
| Add .gitignore | **None** — dùng file đã chuẩn bị |
| Choose a license | **None ở form** — source đã có MIT LICENSE, tránh tạo commit thừa |
| Copilot prompt / Apps | Để trống nếu form có mục này |
| Default branch | `main` khi khởi tạo Git cục bộ |

[Mở form đã điền tên, mô tả và Public](https://github.com/new?owner=%40me&name=tiktok-live-rust&description=Unofficial%20TikTok%20LIVE%20event%20engine%20in%20Rust.%20Direct%20guest%20connections%2C%20typed%20events%2C%20bounded%20queues%2C%20automatic%20reconnects%2C%20and%20an%20NDJSON%20CLI.&visibility=public).

Kiểm tra lại Owner trong form trước khi tạo. Link chỉ mở form, không tự tạo repo.
GitHub hướng dẫn tránh khởi tạo README/.gitignore/license khi nhập source đã có;
xem [hướng dẫn chính thức](https://docs.github.com/en/repositories/creating-and-managing-repositories/creating-a-new-repository).

## Sau khi tạo: About và Settings

- **Topics:** `rust`, `tiktok`, `tiktok-live`, `websocket`, `protobuf`, `tokio`, `cli`, `real-time`, `event-driven`, `livestream`.
- **Website:** để trống đến khi có demo hoặc trang cá nhân phù hợp.
- **Issues:** bật. **Wiki:** tắt nếu chưa dùng. Discussions có thể bật khi có nhu cầu.
- **Security:** đã bật private vulnerability reporting cho repo này.
- **Actions:** [lần CI đầu tiên](https://github.com/thekivashyn/tiktok-live-rust/actions/runs/42765689325)
  đã pass trên Windows/Linux/macOS; README đã có badge CI.
- **Profile:** pin repo lên trang cá nhân, thêm mô tả ngắn theo đúng điểm mạnh kỹ thuật.

## Đưa đúng source lên repo riêng

Không tải toàn bộ project cha lên GitHub. Từ thư mục engine, tạo bản source độc lập:

```powershell
python scripts/prepare_public_repo.py --destination .public-export/tiktok-live-rust-public
```

Bản chuẩn bị chỉ gồm source/tài liệu/cấu hình đã chọn, kèm manifest SHA-256.
Trong workspace hiện tại đã chuẩn bị sẵn thư mục `.public-export/tiktok-live-rust-public`;
anh có thể dùng ngay, không cần chạy lại lệnh xuất.
Không gồm `.env`, `.build-tools-root`, `target`, `reports`, local tools hay
`node_modules`. Script từ chối ghi đè thư mục đã tồn tại; dùng tên mới nếu chạy lại.
Đọc [audit](PUBLICATION_AUDIT.md) trước khi public.

Sau khi tạo repo trống, vào thư mục source vừa xuất và chạy:

```powershell
git init -b main
git add .
git diff --cached --stat
git diff --cached
git commit -m "Initial public release: Rust TikTok LIVE event engine"
```

Repository đã được tạo tại [thekivashyn/tiktok-live-rust](https://github.com/thekivashyn/tiktok-live-rust).
Remote thật để dùng cho bản source riêng:

```powershell
git remote add origin https://github.com/thekivashyn/tiktok-live-rust.git
git push -u origin main
```

Máy cần có tên/email Git và quyền đăng nhập GitHub. Không thay cấu hình danh tính
Git toàn máy chỉ để xuất source.

`0.8.0` hiện là phiên bản phát triển cục bộ. Chỉ tạo tag/release `v0.8.0` sau khi
CI trên repo public pass và đã kiểm tra commit được phát hành. Không upload các
binary cũ trong reports làm bản release mới.

## Nội dung bài giới thiệu gợi ý

> Mình vừa public tiktok-live-rust: một engine nhận sự kiện TikTok LIVE bằng Rust,
> dùng được dưới dạng thư viện hoặc CLI xuất JSON. Mình tập trung vào reconnect,
> giới hạn bộ nhớ/hàng đợi và biểu diễn rõ những khoảng mất dữ liệu. Repo có hướng
> dẫn chạy, ghi chú kiến trúc và các bài kiểm tra offline. Mình muốn nhận góp ý về
> tích hợp thực tế và khả năng chạy trên Linux/macOS.

Thêm link repo thật và một clip demo ngắn sau khi repo đã public. Tránh quảng bá
đảm bảo 24/7, nhận đủ mọi sự kiện, hoặc tốc độ chung dựa trên benchmark cũ.
