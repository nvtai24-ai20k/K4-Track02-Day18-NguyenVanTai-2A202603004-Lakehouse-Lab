# Reflection — Anti-pattern: ingest nhỏ giọt mà không có maintenance

Hệ thống tôi quan tâm là log LLM observability: mỗi request sinh một sự kiện, được ingest
từ Kafka theo micro-batch vài giây. Đây đúng là kịch bản NB6: 200 commit tạo 200 file
~51 KB; ở NB5, metadata Iceberg còn lớn gần gấp 3 lần dữ liệu. Từng commit đều đúng,
nhưng tích lũy lại thì truy vấn chậm, chi phí GET tăng theo số file và reader lạnh phải
replay hàng trăm JSON log.

Nguy hiểm hơn là cảm giác "đã có VACUUM/expiry nên ổn": NB6 cho thấy VACUUM của delta-rs
không thấy 3 orphan chưa từng commit, còn `expire_snapshots` của PyIceberg giảm 20 → 3
snapshot nhưng không xóa file nào cho tới khi chạy thêm bước sweep.

Cách phòng tránh: (1) sửa từ writer — tăng trigger interval/batch size; (2) lên lịch đủ
5 job: compaction, Z-order theo cột lọc chính (`model`, `user_id`), expiry với retention
≥ 7 ngày, orphan sweep có age guard, checkpoint; (3) theo dõi kích thước file trung bình
và tỷ lệ metadata:data như metric, cảnh báo khi file trung bình < 64 MB.

**Sử dụng AI:** xem [AI_USAGE.md](AI_USAGE.md).
